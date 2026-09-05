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
	"wgrot/v2/internal/portforward"
	"wgrot/v2/internal/sink"
	"wgrot/v2/internal/state"
)

const (
	rekeyWindow = int64(3 * 60) // 3 minutes

	minTimeBetweenSamePeer = time.Hour
	minTimeBetweenRotation = 15 * time.Minute
	baseBackoff            = 5 * time.Second
	maxBackoff             = 5 * time.Minute
)

type Runner struct {
	s *state.State
	p *pool.Pool
	m *monitor

	iface string

	refresh time.Duration
	verify  time.Duration
	timeout time.Duration

	lastConnection time.Time

	portForward   bool
	publishPath   string
	forwarder     *portforward.Forwarder
	forwardCtx    context.Context
	forwardCancel context.CancelFunc
}

func NewRunner(
	state *state.State,
	pool *pool.Pool,
	iface string,
	refresh,
	verify,
	timeout time.Duration,
	portForward bool,
	publishPath string,
) *Runner {
	var forwarder *portforward.Forwarder

	if portForward {
		forwarder = portforward.New(
			portforward.DefaultGateway,
			publishPath,
		)
	}

	return &Runner{
		s: state,
		p: pool,
		m: newMonitor(verify),

		iface: iface,

		refresh: refresh,
		verify:  verify,
		timeout: timeout,

		portForward: portForward,
		publishPath: publishPath,
		forwarder:   forwarder,
	}
}

func (r *Runner) Start(skipRefresh bool) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var refresh <-chan time.Time
	if !skipRefresh {
		refreshTicker := time.NewTicker(r.refresh)
		defer refreshTicker.Stop()
		refresh = refreshTicker.C
	}

	verify := time.NewTicker(r.verify)
	defer verify.Stop()

	var portFailure <-chan struct{}
	if r.forwarder != nil {
		portFailure = r.forwarder.Failure()
	}

	// initial startup
	r.rotate(ctx)

	for {
		select {
		case <-ctx.Done():
			sink.Println(sink.INFO, "shutting down")
			return
		case <-portFailure:
			sink.Println(sink.ERROR, "port forwarding failure rotating now")
			r.rotate(ctx)
		case <-refresh:
			r.rotate(ctx)
		case <-verify.C:
			if r.m.IsConnected() {
				continue
			}

			sink.Println(sink.ERROR, "network down")
			r.rotate(ctx)
		}
	}
}

func (r *Runner) rotate(ctx context.Context) {
	failed := 0
	failedCycles := 0
	count := max(r.p.UsableCount(), 1)

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

			next := r.s.Next(ctx, r.p)
			if next == nil {
				sink.Println(sink.WARN, "no valid peers in pool")
				return
			}

			// check connection time
			if time.Since(next.LastConnection()) <= minTimeBetweenSamePeer {
				next.Unlock()
				if err := sleepWithContext(ctx, jittered(baseBackoff)); err != nil {
					sink.Printf(sink.ERROR, "%v\n", err)
					return
				}

				failed++
				continue
			}

			if time.Since(r.lastConnection) <= minTimeBetweenRotation {
				wait := time.Until(r.lastConnection.Add(time.Duration(minTimeBetweenRotation)))
				if err := sleepWithContext(ctx, wait); err != nil {
					sink.Printf(sink.ERROR, "%v\n", err)
					next.Unlock()
					return
				}
			}

			if err := r.s.WaitForConnection(ctx); err != nil {
				sink.Printf(sink.ERROR, "%v\n", err)
				next.Unlock()
				return
			}

			sink.Printf(sink.INFO, "rotating to %s\n", next.Name)
			if err := r.rotateTo(next); err != nil {
				sink.Printf(sink.ERROR, "rotation to %s failed: %v\n", next.Name, err)

				next.Unlock()
				shift := min(failed, 5)
				backoff := jittered(min(maxBackoff, baseBackoff*time.Duration(int64(1)<<uint(shift))))

				if err := sleepWithContext(ctx, backoff); err != nil {
					return
				}

				failed++
				continue
			}

			sink.Printf(sink.INFO, "rotation to %s complete\n", next.Name)
			next.UpdateLastConnection()
			r.lastConnection = time.Now()
			return
		}
	}
}

func (r *Runner) rotateTo(peer *peer.Peer) error {
	start := time.Now().Unix()

	if r.forwardCancel != nil {
		r.forwardCancel()
		r.forwardCancel = nil
	}

	if r.forwarder != nil {
		r.forwarder.Clear()
	}

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

	if err := r.waitForHandshake(peer.PublicKey, start); err != nil {
		return err
	}

	if r.portForward {
		if err := r.startPortForward(); err != nil {
			return fmt.Errorf("forwarding port: %w", err)
		}
	}

	return nil
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

func (r *Runner) startPortForward() error {
	if r.forwarder == nil {
		return nil
	}

	if r.forwardCancel != nil {
		r.forwardCancel()
		r.forwardCancel = nil
	}

	r.forwarder.Wait()
	r.forwarder.Clear()

	ctx, cancel := context.WithCancel(context.Background())

	if _, err := r.forwarder.Acquire(ctx); err != nil {
		cancel()
		r.forwarder.Clear()
		return err
	}

	r.forwardCtx = ctx
	r.forwardCancel = cancel

	r.forwarder.StartRenew(ctx)
	return nil
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
