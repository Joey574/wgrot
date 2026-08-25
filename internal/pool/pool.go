package pool

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
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
	p.mx.RLock()
	defer p.mx.RUnlock()
	return len(p.peers)
}

func (p *Pool) UsableCount() int {
	p.mx.Lock()
	defer p.mx.Unlock()

	// ensure we're offset from other wgrots
	min := 10
	max := 150
	rn := rand.IntN(max-min+1) + min
	time.Sleep(time.Duration(rn) * time.Millisecond)

	count := 0
	for _, peer := range p.peers {
		if peer.IsLocked() {
			// we've already locked this peer, don't unlock it
			count++
			continue
		}

		if ok, err := peer.TryLock(); ok {
			peer.Unlock()

			if err == nil {
				count++
			}
		}
	}

	return count
}

func (p *Pool) At(idx int) *peer.Peer {
	p.mx.Lock()
	defer p.mx.Unlock()
	if idx < 0 || idx >= len(p.peers) {
		return nil
	}

	return &p.peers[idx]
}

func (p *Pool) Remove(path string) {
	for i, peer := range p.peers {
		if peer.Path == path {
			// if peer is in use by us, we'll sleep until we give it up
			for peer.IsLocked() {
				time.Sleep(time.Second)
			}

			p.peers = slices.Delete(p.peers, i, i+1)
		}
	}
}
