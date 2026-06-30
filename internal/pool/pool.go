package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"wgrot/v2/internal/peer"
)

type Pool struct {
	Peers []peer.Peer
}

func NewPool() *Pool {
	return &Pool{}
}

func (p *Pool) Load(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".conf") {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)

	var configs []peer.Peer
	for _, n := range names {
		p := peer.NewPeer()
		if err := p.Load(filepath.Join(dir, n)); err != nil {
			return err
		}

		p.Name = n
		configs = append(configs, p)
	}
	if len(configs) == 0 {
		return fmt.Errorf("no configs present")
	}

	p.Peers = configs
	return nil
}
