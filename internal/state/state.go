package state

import (
	"encoding/json"
	"fmt"
	"os"
	"wgrot/v2/internal/peer"
	"wgrot/v2/internal/pool"
)

type State struct {
	LastGood int    `json:"lastGood"`
	savePath string `json:"-"`
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

	s.LastGood = (s.LastGood + 1) % pool.Count()
	return pool.At(s.LastGood)
}
