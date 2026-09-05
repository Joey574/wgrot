package portforward

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultGateway  = "10.2.0.1"
	MappingLifetime = 60 * time.Second
	RenewInterval   = 45 * time.Second
)

var publicPortRE = regexp.MustCompile(`(?i)public port\s+([0-9]+)`)

type Forwarder struct {
	gateway string
	path    string

	port    int
	failed  bool
	failure chan error

	mu sync.RWMutex
	wg sync.WaitGroup
}

func New(gateway, path string) *Forwarder {
	return &Forwarder{
		gateway: gateway,
		path:    path,
		failure: make(chan error, 1),
	}
}

func (f *Forwarder) Failure() <-chan error {
	return f.failure
}

func (f *Forwarder) Port() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.port
}

func (f *Forwarder) Publish(port int) error {
	if f.path == "" {
		return nil
	}

	dir := publishDir(f.path)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create publish directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".qbit-port-*")
	if err != nil {
		return fmt.Errorf("create temp publish file: %w", err)
	}

	name := tmp.Name()
	defer os.Remove(name)

	if _, err := fmt.Fprintf(tmp, "%d\n", port); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write published port: %w", err)
	}

	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod published port: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close tmp file: %w", err)
	}

	if err := os.Rename(name, f.path); err != nil {
		return fmt.Errorf("publish port: %w", err)
	}

	return nil
}

func (f *Forwarder) Clear() {
	if f.path == "" {
		return
	}

	dir := publishDir(f.path)
	_ = os.MkdirAll(dir, 0o755)

	tmp, err := os.CreateTemp(dir, ".qbit-port-*")
	if err != nil {
		return
	}

	name := tmp.Name()
	defer os.Remove(name)

	_, _ = tmp.WriteString("")
	_ = tmp.Chmod(0o644)
	_ = tmp.Close()

	_ = os.Rename(name, f.path)
}

func (f *Forwarder) Acquire(ctx context.Context) (int, error) {
	udpPort, err := f.mapPort(ctx, "udp")
	if err != nil {
		return 0, fmt.Errorf("UDP NAT-PMP: %w", err)
	}

	tcpPort, err := f.mapPort(ctx, "tcp")
	if err != nil {
		return 0, fmt.Errorf("TCP NAT-PMP: %w", err)
	}

	if udpPort != tcpPort {
		return 0, fmt.Errorf("NAT-PMP returned different ports: UDP=%d TCP=%d", udpPort, tcpPort)
	}

	f.mu.Lock()
	f.port = tcpPort
	f.failed = false
	f.mu.Unlock()

	if err := f.Publish(tcpPort); err != nil {
		return 0, err
	}

	return tcpPort, nil
}

func (f *Forwarder) mapPort(ctx context.Context, protocol string) (int, error) {
	cmd := exec.CommandContext(
		ctx,
		"natpmpc",
		"-a", "1", "0",
		protocol,
		strconv.Itoa(int(MappingLifetime/time.Second)),
		"-g", f.gateway)

	out, err := cmd.CombinedOutput()
	output := string(out)

	if err != nil {
		return 0, fmt.Errorf("%w: %s", err, strings.TrimSpace(output))
	}

	matches := publicPortRE.FindStringSubmatch(output)
	if len(matches) != 2 {
		return 0, fmt.Errorf("could not find public port in natpmpc output: %s", strings.TrimSpace(output))
	}

	port, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("invalid port %q: %w", matches[1], err)
	}

	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("invalid forwarded port %d", port)
	}

	return port, nil
}

func (f *Forwarder) StartRenew(ctx context.Context) {
	f.wg.Go(func() { f.Renew(ctx) })
}

func (f *Forwarder) Wait() {
	f.wg.Wait()
}

func (f *Forwarder) Renew(ctx context.Context) error {
	ticker := time.NewTicker(RenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			f.Clear()
			return ctx.Err()

		case <-ticker.C:
			if _, err := f.Acquire(ctx); err != nil {
				f.Clear()

				select {
				case f.failure <- err:
				default:
				}

				return err
			}
		}
	}
}

func publishDir(path string) string {
	i := strings.LastIndexByte(path, '/')
	if i <= 0 {
		return "."
	}
	return path[:i]
}
