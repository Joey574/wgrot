package runner

import (
	"fmt"
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

type Runner struct {
	s       *state.State
	p       *pool.Pool
	m       *monitor
	iface   string
	refresh time.Duration
	verify  time.Duration
	timeout time.Duration
}

func NewRunner(state *state.State, pool *pool.Pool, iface string, refresh, verify, timeout time.Duration) *Runner {
	return &Runner{
		s:       state,
		p:       pool,
		m:       newMonitor(verify),
		iface:   iface,
		refresh: refresh,
		verify:  verify,
		timeout: timeout,
	}
}

func (r *Runner) Start() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	start := r.s.Next(r.p)
	fmt.Printf("applying startup config: %s\n", start.Name)
	if err := r.rotateTo(start); err != nil {
		fmt.Printf("startup config %s failed to come up: %v\n", start.Name, err)
	}

	refresh := time.NewTicker(r.refresh)
	defer refresh.Stop()

	verify := time.NewTicker(r.verify)
	defer verify.Stop()

	for {
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				fmt.Println("SIGHUP recieved, rotating now")
				r.rotate()
				continue
			}
			fmt.Println("shuting down")
			return
		case <-refresh.C:
			r.rotate()
		case <-verify.C:
			if r.m.IsConnected() {
				continue
			}

			fmt.Println("network down, rotating now")
			r.rotate()
		}
	}
}

func (r *Runner) rotate() {
	for {
		next := r.s.Next(r.p)
		fmt.Printf("rotating to %s\n", next.Name)

		if err := r.rotateTo(next); err != nil {
			fmt.Printf("rotation to %s failed: %v\n", next.Name, err)
			time.Sleep(1 * time.Second)
			continue
		}

		break
	}

	r.s.Save()
}

func (r *Runner) rotateTo(peer *peer.Peer) error {
	start := time.Now().Unix()

	f, err := os.CreateTemp("", "wg-conf-*")
	if err != nil {
		return fmt.Errorf("creating tmp key file: %w", err)
	}
	defer os.Remove(f.Name())

	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}

	if _, err := f.WriteString(peer.Config); err != nil {
		f.Close()
		return err
	}
	f.Close()

	cmd1 := exec.Command("wg", "syncconf", r.iface, f.Name())
	if out, err := cmd1.CombinedOutput(); err != nil {
		return fmt.Errorf("wg syncconf: %w: %s", err, string(out))
	}

	cmd2 := exec.Command("ip", "addr", "flush", "dev", r.iface, "scope", "global")
	if out, err := cmd2.CombinedOutput(); err != nil {
		return fmt.Errorf("ip addr flush: %w: %s", err, string(out))
	}

	for i := range peer.Address {
		addr := peer.Address[i]

		cmd3 := exec.Command("ip", "addr", "add", addr, "dev", r.iface)
		if out, err := cmd3.CombinedOutput(); err != nil {
			return fmt.Errorf("ip addr add %s: %w: %s", addr, err, string(out))
		}
	}

	cmd4 := exec.Command("ip", "route", "replace", "default", "dev", r.iface)
	if out, err := cmd4.CombinedOutput(); err != nil {
		return fmt.Errorf("ip route replace: %w: %s", err, string(out))
	}

	return r.waitForHandshake(peer.PublicKey, start)
}

func (r *Runner) waitForHandshake(pubKey string, start int64) error {
	deadline := time.Now().Add(r.timeout)

	for time.Now().Before(deadline) {
		out, err := exec.Command("wg", "show", r.iface, "latest-handshakes").Output()

		if err == nil {
			for line := range strings.SplitSeq(string(out), "\n") {
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

	return fmt.Errorf("no handshake within %s", r.timeout)
}
