package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Peer struct {
	Name       string
	PrivateKey string
	PublicKey  string
	Endpoint   string
	Address    []string
	AllowedIPs string
	Keepalive  string
}

type State struct {
	LastGood int `json:"lastGood"`
}

func main() {
	iface := flag.String("iface", "", "wireguard interface name")
	poolDir := flag.String("pool", "", "directory of *.conf pool files")
	intervalStr := flag.String("interval", "3h", "rotation interval")
	statePath := flag.String("state", "", "state file path")
	handshakeTimeout := flag.Duration("handshake-timeout", 15*time.Second, "max wait for handshake confirmation")
	flag.Parse()

	interval, err := time.ParseDuration(*intervalStr)
	if err != nil {
		log.Fatalf("invalid interval: %v\n", err)
	}

	configs, err := loadPool(*poolDir)
	if err != nil {
		log.Fatalf("loading pool: %v\n", err)
	}

	if len(configs) == 0 {
		log.Fatalf("no *.conf files found in %s", *poolDir)
	}
	log.Printf("loaded %d configs from %s", len(configs), *poolDir)

	state := loadState(*statePath)
	if state.LastGood < 0 || state.LastGood >= len(configs) {
		state.LastGood = 0
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	idx := state.LastGood
	log.Printf("applying startup config: %s", configs[idx].Name)
	if err := rotateTo(*iface, configs, idx, *handshakeTimeout); err != nil {
		log.Printf("startup config %s failed to come up: %v", configs[idx].Name, err)
	} else {
		state.LastGood = idx
		saveState(*statePath, state)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				log.Println("SIGHUP recieved, rotating now")
				idx = (idx + 1) % len(configs)
				doRotate(*iface, configs, &idx, &state, *statePath, *handshakeTimeout)
				continue
			}
			log.Println("shuting down")
			return
		case <-ticker.C:
			idx = (idx + 1) % len(configs)
			doRotate(*iface, configs, &idx, &state, *statePath, *handshakeTimeout)
		}
	}
}

func doRotate(iface string, configs []Peer, idx *int, state *State, statePath string, timeout time.Duration) {
	next := *idx

	for {
		log.Printf("rotating to %s", configs[next].Name)
		if err := rotateTo(iface, configs, next, timeout); err != nil {
			log.Printf("rotation to %s failed: %v - attempting next...", configs[next].Name, err)
			next = (next + 1) % len(configs)
			time.Sleep(1 * time.Second)
			continue
		}

		break
	}

	state.LastGood = next
	*idx = state.LastGood
	saveState(statePath, *state)
}

func loadPool(dir string) ([]Peer, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".conf") {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)

	var configs []Peer
	for _, n := range names {
		c, err := parseConfig(filepath.Join(dir, n))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", n, err)
		}
		c.Name = n
		configs = append(configs, c)
	}

	return configs, nil
}

func parseConfig(path string) (Peer, error) {
	f, err := os.Open(path)
	if err != nil {
		return Peer{}, err
	}
	defer f.Close()

	c := Peer{}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return c, fmt.Errorf("bad syntax: %s", line)
		}

		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		switch key {
		case "privatekey":
			c.PrivateKey = val
		case "address":
			c.Address = strings.Split(val, ",")
			for i := range c.Address {
				c.Address[i] = strings.TrimSpace(c.Address[i])
			}
		case "publickey":
			c.PublicKey = val
		case "allowedips":
			c.AllowedIPs = val
		case "endpoint":
			c.Endpoint = val
		case "persistentkeepalive":
			c.Keepalive = val
		}
	}
	if err := scanner.Err(); err != nil {
		return c, err
	}

	if c.PrivateKey == "" || len(c.Address) == 0 || c.PublicKey == "" || c.AllowedIPs == "" || c.Endpoint == "" || c.Keepalive == "" {
		return c, fmt.Errorf("missing required field")
	}

	return c, nil
}

func rotateTo(iface string, configs []Peer, idx int, timeout time.Duration) error {
	c := configs[idx]

	keyFile, err := os.CreateTemp("", "wg-key-*")
	if err != nil {
		return fmt.Errorf("creating tmp key file: %w", err)
	}
	defer os.Remove(keyFile.Name())

	if err := keyFile.Chmod(0o600); err != nil {
		keyFile.Close()
		return err
	}

	if _, err := keyFile.WriteString(c.PrivateKey); err != nil {
		keyFile.Close()
		return err
	}

	cmd1 := exec.Command("wg", "set", iface, "private-key", keyFile.Name(), "peer", c.PublicKey, "endpoint", c.Endpoint, "persistent-keepalive", c.Keepalive, "allowed-ips", c.AllowedIPs)
	if err := cmd1.Run(); err != nil {
		return fmt.Errorf("wg set: %w", err)
	}

	cmd2 := exec.Command("ip", "addr", "flush", "dev", iface, "scope", "global")
	if err := cmd2.Run(); err != nil {
		return fmt.Errorf("ip addr flush: %w", err)
	}

	for i := range c.Address {
		addr := c.Address[i]

		cmd3 := exec.Command("ip", "addr", "add", addr, "dev", iface)
		if err := cmd3.Run(); err != nil {
			return fmt.Errorf("ip addr add %s: %w", addr, err)
		}
	}

	cmd4 := exec.Command("ip", "route", "replace", "default", "dev", iface)
	if err := cmd4.Run(); err != nil {
		return fmt.Errorf("ip route replace: %w", err)
	}

	return waitForHandshake(iface, c.PublicKey, timeout)
}

func waitForHandshake(iface, pubKey string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	start := time.Now().Unix()

	for time.Now().Before(deadline) {
		out, err := exec.Command("wg", "show", iface, "latest-handshakes").Output()

		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				fields := strings.Fields(line)

				if len(fields) == 2 && fields[0] == pubKey {
					ts, _ := strconv.ParseInt(fields[1], 10, 64)

					if ts >= start {
						return nil
					}
				}
			}
		}

		time.Sleep(time.Second)
	}

	return fmt.Errorf("no handshake within %s", timeout)
}

func loadState(path string) State {
	var s State
	data, err := os.ReadFile(path)
	if err != nil {
		return s
	}

	_ = json.Unmarshal(data, &s)
	return s
}

func saveState(path string, s State) {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		log.Printf("marshal state: %v", err)
		return
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		log.Printf("write state: %v", err)
	}
}
