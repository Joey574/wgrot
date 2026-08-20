package runner

import (
	"context"
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
	"wgrot/v2/internal/sink"
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

func (r *Runner) Start(skipRefresh bool) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	start := r.s.Next(r.p)
	sink.Printf(sink.DEBUG, "applying startup config: %s\n", start.Name)
	if err := r.rotateTo(start); err != nil {
		sink.Printf(sink.ERROR, "%s failed to come up: %v", start.Name, err)
	} else {
		sink.Printf(sink.DEBUG, "startup config %s online", start.Name)
	}

	var refresh *time.Ticker
	if !skipRefresh {
		refresh = time.NewTicker(r.refresh)
		defer refresh.Stop()
	}

	verify := time.NewTicker(r.verify)
	defer verify.Stop()

	for {
		select {
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				sink.Println(sink.DEBUG, "SIGHUP recieved")
				r.rotate(sigCh)
				continue
			}
			sink.Println(sink.INFO, "shutting down")
			return
		case <-refresh.C:
			r.rotate(sigCh)
		case <-verify.C:
			if r.m.IsConnected() {
				continue
			}

			sink.Println(sink.ERROR, "network down")
			r.rotate(sigCh)
		}
	}
}

func (r *Runner) rotate(sigCh chan os.Signal) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pollCtx(ctx, sigCh, cancel, 100*time.Millisecond)

	failed := 0
	failedCycles := 0
	count := r.p.UsableCount()

	for {
		select {
		case <-ctx.Done():
			sink.Println(sink.TRACE, "interupt detected, early out")
			return
		default:
			if failed >= count {
				failed = 0
				failedCycles++
				t := min(time.Hour, 5*time.Duration(failedCycles)*time.Minute)
				sink.Printf(sink.ERROR, "failed through pool, sleeping for %s\n", t.String())
				err := sleepWithContext(ctx, t)
				if err != nil {
					return
				}

			}

			next := r.s.Next(r.p)
			sink.Printf(sink.INFO, "rotating to %s\n", next.Name)
			if err := r.rotateTo(next); err != nil {
				sink.Printf(sink.ERROR, "rotation to %s failed: %v\n", next.Name, err)
				err = sleepWithContext(ctx, 5*time.Second)
				if err != nil {
					return
				}

				failed++
				continue
			}

			sink.Printf(sink.INFO, "rotation to %s complete\n", next.Name)
			r.s.Save()
			return
		}
	}
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

	current, err := currentAddrs(r.iface)
	if err != nil {
		return fmt.Errorf("reading current addrs: %w", err)
	}
	want := make(map[string]bool, len(peer.Address))
	for _, a := range peer.Address {
		want[a] = true
	}

	for addr := range want {
		cmd := exec.Command("ip", "addr", "replace", addr, "dev", r.iface)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("ip addr replace %s: %w: %s", addr, err, string(out))
		}
	}

	for _, addr := range current {
		if !want[addr] {
			cmd := exec.Command("ip", "addr", "del", addr, "dev", r.iface)
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("ip addr del %s: %w: %s", addr, err, string(out))
			}
		}
	}

	cmd4 := exec.Command("ip", "route", "replace", "default", "dev", r.iface)
	if out, err := cmd4.CombinedOutput(); err != nil {
		return fmt.Errorf("ip route replace: %w: %s", err, string(out))
	}

	return r.waitForHandshake(peer.PublicKey, start)
}

func currentAddrs(iface string) ([]string, error) {
	out, err := exec.Command("ip", "-o", "addr", "show", "dev", iface, "scope", "global").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, string(out))
	}
	var addrs []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		for i, f := range fields {
			if f == "inet" || f == "inet6" {
				addrs = append(addrs, fields[i+1])
				break
			}
		}
	}
	return addrs, nil
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
