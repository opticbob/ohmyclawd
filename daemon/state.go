package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
)

type State struct {
	mu   sync.RWMutex
	cur  *Usage
	body []byte
	etag string
	obs  []func()
}

// OnStore registers f to run after every Store. f must not block.
func (s *State) OnStore(f func()) {
	s.mu.Lock()
	s.obs = append(s.obs, f)
	s.mu.Unlock()
}

func NewState() *State { return &State{cur: &Usage{}} }

func (s *State) Store(u *Usage) {
	if u == nil {
		return
	}
	body, _ := json.Marshal(u)
	sum := sha256.Sum256(body)
	tag := `"` + hex.EncodeToString(sum[:8]) + `"`
	s.mu.Lock()
	cp := *u
	s.cur = &cp
	s.body = body
	s.etag = tag
	obs := s.obs
	s.mu.Unlock()
	for _, f := range obs {
		f()
	}
}

func (s *State) Load() (*Usage, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.cur == nil {
		return &Usage{}, ""
	}
	cp := *s.cur
	return &cp, s.etag
}

func (s *State) Body() ([]byte, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := make([]byte, len(s.body))
	copy(b, s.body)
	return b, s.etag
}
