package peer

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/gofrs/flock"
)

type Peer struct {
	Name       string
	Path       string
	PrivateKey string
	PublicKey  string
	Endpoint   string
	Address    []string
	AllowedIPs string
	Keepalive  string
	DNS        string
	Config     string

	lock     *flock.Flock
	lockPath string
}

func NewPeer() Peer {
	return Peer{}
}

func (p *Peer) IsValid() bool {
	return p.PrivateKey != "" &&
		p.PublicKey != "" &&
		p.Endpoint != "" &&
		len(p.Address) != 0 &&
		p.AllowedIPs != "" &&
		p.Keepalive != "" &&
		p.DNS != ""
}

func (p *Peer) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	p.Path = path
	p.lockPath = path + ".lock"

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("bad syntax: %s", line)
		}

		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])

		switch key {
		case "privatekey":
			p.PrivateKey = val
		case "address":
			p.Address = strings.Split(val, ",")
			for i := range p.Address {
				p.Address[i] = strings.TrimSpace(p.Address[i])
			}
		case "publickey":
			p.PublicKey = val
		case "allowedips":
			ips := strings.Split(val, ",")
			for i := range ips {
				ips[i] = strings.TrimSpace(ips[i])
			}
			p.AllowedIPs = strings.Join(ips, ",")
		case "endpoint":
			p.Endpoint = val
		case "persistentkeepalive":
			p.Keepalive = val
		case "dns":
			p.DNS = val
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if !p.IsValid() {
		return fmt.Errorf("config is invalid")
	}

	// create pre-prepared wg Config
	p.Config = fmt.Sprintf("[Interface]\nPrivateKey = %s\n\n[Peer]\nPublicKey = %s\nEndpoint = %s\nAllowedIPs = %s\nPersistentKeepalive = %s", p.PrivateKey, p.PublicKey, p.Endpoint, p.AllowedIPs, p.Keepalive)
	return nil
}

func (p *Peer) TryLock() (bool, error) {
	p.lock = flock.New(p.lockPath)
	return p.lock.TryLock()
}

func (p *Peer) Unlock() error {
	err := p.lock.Unlock()
	_ = os.Remove(p.lockPath)
	return err
}
