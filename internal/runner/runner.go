package runner

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"wgrot/v2/internal/peer"
	"wgrot/v2/internal/pool"
	"wgrot/v2/internal/state"
)

const rekeyWindow = int64(3 * 60)

func Start(state *state.State, pool *pool.Pool, iface string, refreshInterval, verifyInterval, timeout time.Duration) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	start := state.Next(pool)
	fmt.Printf("applying startup config: %s\n", start.Name)
	if err := rotateTo(start, iface, timeout); err != nil {
		fmt.Printf("startup config %s failed to come up: %v\n", start.Name, err)
	}

	refresh := time.NewTicker(refreshInterval)
	defer refresh.Stop()

	verify := time.NewTicker(verifyInterval)
	defer verify.Stop()

	for {
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				fmt.Println("SIGHUP recieved, rotating now")
				doRotate(state, pool, iface, timeout)
				continue
			}
			fmt.Println("shuting down")
			return
		case <-refresh.C:
			doRotate(state, pool, iface, timeout)
		case <-verify.C:
			fmt.Println("checking network status...")
			if isConnected() {
				fmt.Println("network status ok")
				continue
			}

			fmt.Println("network down, rotating now")
			doRotate(state, pool, iface, timeout)
		}
	}
}

func isConnected() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	urls := []string{
		"http://clients3.google.com/generate_204",
		"http://captive.apple.com/hotspot-detect.html",
		"http://detectportal.firefox.com/success.txt",
		"https://1.1.1.1/cdn-cgi/trace",
	}

	for _, url := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			fmt.Printf("creating request: %v\n", err)
			continue
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("request error: %v\n", err)
			continue
		}

		status := resp.StatusCode
		resp.Body.Close()

		if status == http.StatusOK || status == http.StatusNoContent {
			return true
		}
	}

	return false
}

func doRotate(state *state.State, pool *pool.Pool, iface string, timeout time.Duration) {
	for {
		next := state.Next(pool)
		fmt.Printf("rotating to %s\n", next.Name)

		if err := rotateTo(next, iface, timeout); err != nil {
			fmt.Printf("rotation to %s failed: %v - attempting next...\n", next.Name, err)
			time.Sleep(1 * time.Second)
			continue
		}

		break
	}

	state.Save()
}

func rotateTo(peer *peer.Peer, iface string, timeout time.Duration) error {
	start := time.Now().Unix()

	keyFile, err := os.CreateTemp("", "wg-key-*")
	if err != nil {
		return fmt.Errorf("creating tmp key file: %w", err)
	}
	defer os.Remove(keyFile.Name())

	if err := keyFile.Chmod(0o600); err != nil {
		keyFile.Close()
		return err
	}

	if _, err := keyFile.WriteString(peer.PrivateKey); err != nil {
		keyFile.Close()
		return err
	}

	cmd1 := exec.Command("wg", "set", iface, "private-key", keyFile.Name(), "peer", peer.PublicKey, "endpoint", peer.Endpoint, "persistent-keepalive", peer.Keepalive, "allowed-ips", peer.AllowedIPs)
	if err := cmd1.Run(); err != nil {
		return fmt.Errorf("wg set: %w", err)
	}

	cmd2 := exec.Command("ip", "addr", "flush", "dev", iface, "scope", "global")
	if err := cmd2.Run(); err != nil {
		return fmt.Errorf("ip addr flush: %w", err)
	}

	for i := range peer.Address {
		addr := peer.Address[i]

		cmd3 := exec.Command("ip", "addr", "add", addr, "dev", iface)
		if err := cmd3.Run(); err != nil {
			return fmt.Errorf("ip addr add %s: %w", addr, err)
		}
	}

	cmd4 := exec.Command("ip", "route", "replace", "default", "dev", iface)
	if err := cmd4.Run(); err != nil {
		return fmt.Errorf("ip route replace: %w", err)
	}

	return waitForHandshake(iface, peer.PublicKey, start, timeout)
}

func waitForHandshake(iface, pubKey string, start int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		out, err := exec.Command("wg", "show", iface, "latest-handshakes").Output()

		if err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				fields := strings.Fields(line)

				if len(fields) == 2 && fields[0] == pubKey {
					ts, _ := strconv.ParseInt(fields[1], 10, 64)

					if ts >= start || (time.Now().Unix()-ts) <= rekeyWindow {
						return nil
					}
				}
			}
		}

		time.Sleep(time.Second)
	}

	return fmt.Errorf("no handshake within %s", timeout)
}
