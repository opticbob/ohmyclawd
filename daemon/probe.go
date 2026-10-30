package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	hdrS  = "anthropic-ratelimit-unified-5h-utilization"
	hdrSR = "anthropic-ratelimit-unified-5h-reset"
	hdrW  = "anthropic-ratelimit-unified-7d-utilization"
	hdrWR = "anthropic-ratelimit-unified-7d-reset"
	hdrST = "anthropic-ratelimit-unified-status"
)

type Prober struct {
	URL   string
	Token string
	HTTP  *http.Client
	Now   func() time.Time // injectable for tests; nil -> time.Now
}

var (
	errAuth        = errors.New("anthropic auth error")
	errRateLimited = errors.New("anthropic rate limited")
	errNoHeaders   = errors.New("anthropic response missing ratelimit headers")
)

func IsAuthError(err error) bool      { return errors.Is(err, errAuth) }
func IsRateLimited(err error) bool    { return errors.Is(err, errRateLimited) }
func IsMissingHeaders(err error) bool { return errors.Is(err, errNoHeaders) }

// Run executes a single probe call and returns the parsed Usage.
// On 401 the returned error wraps errAuth. On 429 the returned error wraps
// errRateLimited *and* a Usage is returned with whatever ST/S/W headers
// the response carried (caller can keep it as best-effort).
func (p *Prober) Run() (*Usage, error) { return p.RunContext(context.Background()) }

// RunContext is Run with a context that cancels the in-flight request.
func (p *Prober) RunContext(ctx context.Context) (*Usage, error) {
	body := []byte(`{"model":"claude-haiku-4-5","max_tokens":1,"messages":[{"role":"user","content":"."}]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.Token)
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	u := p.parseHeaders(resp.Header)

	switch {
	case resp.StatusCode == 401:
		return nil, fmt.Errorf("status 401: %w", errAuth)
	case resp.StatusCode == 429:
		u.ST = "limited"
		u.OK = false
		return u, fmt.Errorf("status 429: %w", errRateLimited)
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("upstream %d: %s", resp.StatusCode, string(raw))
	case resp.StatusCode != 200:
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(raw))
	}

	if u.S == 0 && u.W == 0 && u.ST == "" && resp.Header.Get(hdrS) == "" {
		var ignore json.RawMessage
		_ = json.Unmarshal(raw, &ignore) // keep raw separate; not part of usage
		return nil, errNoHeaders
	}
	u.OK = true
	return u, nil
}

func (p *Prober) parseHeaders(h http.Header) *Usage {
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	u := &Usage{Ts: now().Unix()}
	if v := h.Get(hdrS); v != "" {
		u.S = parseUtilization(v)
	}
	if v := h.Get(hdrW); v != "" {
		u.W = parseUtilization(v)
	}
	if v := h.Get(hdrSR); v != "" {
		if epoch, err := strconv.ParseInt(v, 10, 64); err == nil {
			u.SR = minutesUntil(epoch, now())
		}
	}
	if v := h.Get(hdrWR); v != "" {
		if epoch, err := strconv.ParseInt(v, 10, 64); err == nil {
			u.WR = minutesUntil(epoch, now())
		}
	}
	u.ST = h.Get(hdrST)
	return u
}

func minutesUntil(epoch int64, from time.Time) int {
	d := time.Unix(epoch, 0).Sub(from)
	if d < 0 {
		return 0
	}
	return int(d.Minutes())
}

// parseUtilization converts Anthropic's utilization header into a percent.
// Anthropic returns a decimal fraction (e.g. "0.88") -> 88. For forward
// compatibility we also accept an already-integer-percent string (e.g. "88")
// in case the wire format changes; the dot is the format discriminator.
func parseUtilization(v string) int {
	clamp := func(n int) int {
		if n < 0 {
			return 0
		}
		if n > 100 {
			return 100
		}
		return n
	}
	if strings.Contains(v, ".") {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return clamp(int(f*100 + 0.5))
		}
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil {
		return clamp(n)
	}
	return 0
}
