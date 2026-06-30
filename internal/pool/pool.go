package pool

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"wgrot/v2/internal/peer"
)

type Pool struct {
	mx    sync.RWMutex
	peers []peer.Peer
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

	p.mx.Lock()
	defer p.mx.Unlock()
	p.peers = configs
	return nil
}

func (p *Pool) Append(path string) {
	peer := peer.NewPeer()
	err := peer.Load(path)
	if err != nil {
		fmt.Printf("failed to load %s: %v\n", path, err)
	}

	p.mx.Lock()
	defer p.mx.Unlock()
	p.peers = append(p.peers, peer)
}

func (p *Pool) Count() int {
	p.mx.Lock()
	defer p.mx.Unlock()
	return len(p.peers)
}

func (p *Pool) At(idx int) *peer.Peer {
	p.mx.Lock()
	defer p.mx.Unlock()
	if idx < 0 || idx >= len(p.peers) {
		return nil
	}

	return &p.peers[idx]
}
