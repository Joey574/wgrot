package peer

import (
	"bufio"
	"fmt"
	"os"
	"strings"
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

func NewPeer() Peer {
	return Peer{}
}

func (p *Peer) IsValid() bool {
	return p.PrivateKey != "" &&
		p.PublicKey != "" &&
		p.Endpoint != "" &&
		len(p.Address) != 0 &&
		p.AllowedIPs != "" &&
		p.Keepalive != ""
}

func (p *Peer) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

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
			p.AllowedIPs = val
		case "endpoint":
			p.Endpoint = val
		case "persistentkeepalive":
			p.Keepalive = val
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if !p.IsValid() {
		return fmt.Errorf("config is invalid")
	}

	return nil
}
