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
	Dir   string
}

func NewPool(dir string) *Pool {
	return &Pool{
		Dir: dir,
	}
}

func (p *Pool) Load() error {
	entries, err := os.ReadDir(p.Dir)
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
		peer := peer.NewPeer()
		if err := peer.Load(filepath.Join(p.Dir, n)); err != nil {
			return err
		}

		peer.Name = n
		configs = append(configs, peer)
	}
	if len(configs) == 0 {
		return fmt.Errorf("no configs present")
	}

	p.Peers = configs
	return nil
}

func (p *Pool) Append(path string) {

}
