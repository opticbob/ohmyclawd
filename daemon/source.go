package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Source produces a Usage snapshot. Sources are tried in order by a Chain.
type Source interface {
	Name() string
	Fetch(ctx context.Context) (*Usage, error)
	Interval() time.Duration // sleep after a successful fetch
}

// ProbeSource wraps the 1-token Haiku Prober. On 401 it reloads credentials
// from disk and retries once, since Claude Code rotates the OAuth token.
type ProbeSource struct {
	P           *Prober
	ReloadCreds func() (*Creds, error)
	Every       time.Duration
}

func (p *ProbeSource) Name() string            { return "probe" }
func (p *ProbeSource) Interval() time.Duration { return p.Every }

func (p *ProbeSource) Fetch(ctx context.Context) (*Usage, error) {
	u, err := p.P.RunContext(ctx)
	if !IsAuthError(err) || p.ReloadCreds == nil {
		return u, err
	}
	c, err2 := p.ReloadCreds()
	if err2 != nil || c.AccessToken == "" {
		return u, err
	}
	p.P.Token = c.AccessToken
	return p.P.RunContext(ctx)
}

// Chain tries each Source in order; the first success wins.
type Chain struct {
	Sources []Source

	mu      sync.Mutex
	lastErr map[string]error
}

func NewChain(sources ...Source) *Chain {
	return &Chain{Sources: sources, lastErr: make(map[string]error)}
}

// Fetch returns the winning source's Usage (with Src set) and that source's
// interval. A rate-limited result is returned as-is, partial Usage and all,
// without trying later sources. If every source fails the errors are joined.
func (c *Chain) Fetch(ctx context.Context) (*Usage, time.Duration, error) {
	var errs []error
	for _, s := range c.Sources {
		u, err := s.Fetch(ctx)
		c.setErr(s.Name(), err)
		if err == nil || IsRateLimited(err) {
			if u != nil {
				u.Src = s.Name()
			}
			return u, s.Interval(), err
		}
		errs = append(errs, fmt.Errorf("%s: %w", s.Name(), err))
		if ctx.Err() != nil {
			break
		}
	}
	if len(errs) == 0 {
		return nil, 0, errors.New("no usage sources configured")
	}
	return nil, 0, errors.Join(errs...)
}

// LastErr returns the most recent error from the named source, or nil.
func (c *Chain) LastErr(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr[name]
}

func (c *Chain) setErr(name string, err error) {
	c.mu.Lock()
	c.lastErr[name] = err
	c.mu.Unlock()
}
