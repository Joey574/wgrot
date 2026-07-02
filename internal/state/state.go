package state

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
	"wgrot/v2/internal/peer"
	"wgrot/v2/internal/pool"
)

type State struct {
	LastGood int `json:"lastGood"`

	savePath string     `json:"-"`
	lastPeer *peer.Peer `json:"-"`
}

func NewState(path string) *State {
	return &State{
		savePath: path,
	}
}

func (s *State) Load(pool *pool.Pool) {
	data, err := os.ReadFile(s.savePath)
	if err != nil {
		return
	}

	_ = json.Unmarshal(data, s)

	if s.LastGood < 0 || s.LastGood >= pool.Count() {
		s.LastGood = 0
	}
}

func (s *State) Save() {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		fmt.Printf("marshal state: %v\n", err)
		return
	}

	if err := os.WriteFile(s.savePath, data, 0o600); err != nil {
		fmt.Printf("write state: %v\n", err)
	}
}

func (s *State) Next(pool *pool.Pool) *peer.Peer {
	if s.LastGood < 0 || s.LastGood >= pool.Count() {
		s.LastGood = 0
	}

	for {
		s.LastGood = (s.LastGood + 1) % pool.Count()
		p := pool.At(s.LastGood)
		ok, err := p.TryLock()
		if ok && err == nil {
			if s.lastPeer != nil {
				if err = s.lastPeer.Unlock(); err != nil {
					fmt.Printf("failed to unlock last: %v\n", err)
				}
			}

			s.lastPeer = p
			return p
		}

		if err != nil {
			fmt.Printf("failed to lock: %v\n", err)
		}

		if !ok {
			fmt.Printf("peer already in use")
		}

		time.Sleep(100 * time.Millisecond)
	}
}
