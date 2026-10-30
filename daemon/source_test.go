package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newAuthFlipServer returns 401 unless the request carries "refreshed-token".
func newAuthFlipServer(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer refreshed-token" {
			w.WriteHeader(401)
			return
		}
		w.Header().Set(hdrS, "0.45")
		w.Header().Set(hdrW, "0.28")
		w.Header().Set(hdrST, "allowed")
		w.WriteHeader(200)
	}))
}

func TestChainFetch(t *testing.T) {
	ok := func(s int) fetchResult { return fetchResult{&Usage{S: s, OK: true}, nil} }
	fail := fetchResult{nil, errors.New("boom")}
	limited := fetchResult{&Usage{S: 100, ST: "limited"}, fmt.Errorf("status 429: %w", errRateLimited)}

	cases := []struct {
		name         string
		a, b         fetchResult
		wantSrc      string
		wantS        int
		wantInterval time.Duration
		wantErr      string // substring; "" means nil
		wantLimited  bool
		wantBCalls   int
	}{
		{name: "first succeeds", a: ok(1), b: ok(2), wantSrc: "cli", wantS: 1, wantInterval: 3 * time.Minute, wantBCalls: 0},
		{name: "first fails second succeeds", a: fail, b: ok(2), wantSrc: "probe", wantS: 2, wantInterval: time.Minute, wantBCalls: 1},
		{name: "all fail", a: fail, b: fail, wantErr: "cli: boom\nprobe: boom", wantBCalls: 1},
		{name: "rate limited passthrough", a: fail, b: limited, wantSrc: "probe", wantS: 100, wantInterval: time.Minute, wantErr: "429", wantLimited: true, wantBCalls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := &fakeSource{name: "cli", interval: 3 * time.Minute, results: []fetchResult{tc.a}}
			b := &fakeSource{name: "probe", interval: time.Minute, results: []fetchResult{tc.b}}
			c := NewChain(a, b)
			u, iv, err := c.Fetch(context.Background())

			if tc.wantErr == "" && err != nil {
				t.Fatalf("err = %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
			if IsRateLimited(err) != tc.wantLimited {
				t.Fatalf("IsRateLimited = %v", IsRateLimited(err))
			}
			if b.Calls() != tc.wantBCalls {
				t.Fatalf("second source calls = %d, want %d", b.Calls(), tc.wantBCalls)
			}
			if tc.wantSrc == "" {
				if u != nil {
					t.Fatalf("usage = %+v, want nil", u)
				}
				return
			}
			if u == nil || u.Src != tc.wantSrc || u.S != tc.wantS || iv != tc.wantInterval {
				t.Fatalf("u=%+v interval=%v", u, iv)
			}
		})
	}
}

func TestChainRemembersLastErrPerSource(t *testing.T) {
	a := &fakeSource{name: "cli", results: []fetchResult{{nil, errSentinel}, {&Usage{OK: true}, nil}}}
	b := &fakeSource{name: "probe", results: []fetchResult{{&Usage{OK: true}, nil}}}
	c := NewChain(a, b)

	_, _, _ = c.Fetch(context.Background())
	if !errors.Is(c.LastErr("cli"), errSentinel) || c.LastErr("probe") != nil {
		t.Fatalf("after 1st: cli=%v probe=%v", c.LastErr("cli"), c.LastErr("probe"))
	}
	_, _, _ = c.Fetch(context.Background())
	if c.LastErr("cli") != nil {
		t.Fatalf("cli err not cleared on success: %v", c.LastErr("cli"))
	}
}

var errSentinel = errors.New("not found")

func TestLoopIntervalFollowsWinningSource(t *testing.T) {
	a := &fakeSource{name: "cli", interval: 3 * time.Minute, results: []fetchResult{
		{&Usage{OK: true}, nil},
		{nil, errors.New("cli broke")},
		{&Usage{OK: true}, nil},
	}}
	b := &fakeSource{name: "probe", interval: time.Minute, results: []fetchResult{{&Usage{OK: true}, nil}}}
	st, _, sleeps := runLoopFor(t, NewChain(a, b), 3, LoopConfig{Backoff: []time.Duration{time.Second}})
	want := []time.Duration{3 * time.Minute, time.Minute, 3 * time.Minute}
	if fmt.Sprint(sleeps) != fmt.Sprint(want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
	if u, _ := st.Load(); u.Src != "cli" {
		t.Fatalf("src = %q", u.Src)
	}
}

func TestChainAllFailAppliesBackoff(t *testing.T) {
	a := &fakeSource{name: "cli", results: []fetchResult{{nil, errors.New("x")}}}
	b := &fakeSource{name: "probe", results: []fetchResult{{nil, errors.New("y")}}}
	_, m, sleeps := runLoopFor(t, NewChain(a, b), 3, LoopConfig{Backoff: []time.Duration{1, 2, 4}})
	if fmt.Sprint(sleeps) != fmt.Sprint([]time.Duration{1, 2, 4}) {
		t.Fatalf("sleeps = %v", sleeps)
	}
	if m.fail.Load() != 3 {
		t.Fatalf("fail = %d", m.fail.Load())
	}
}

func TestBuildChainCredsFatalOnlyWhenProbeAlone(t *testing.T) {
	missing := chainOpts{CredsPath: "testdata/does_not_exist.json"}
	if _, err := buildChain([]string{"probe"}, missing); err == nil {
		t.Fatal("probe-only with missing creds: want error")
	}
	if _, err := buildChain([]string{"probe"}, chainOpts{CredsPath: "testdata/creds_valid.json"}); err != nil {
		t.Fatalf("probe-only with creds: %v", err)
	}
	c, err := buildChain([]string{"cli", "probe"}, missing)
	if err != nil {
		t.Fatalf("cli,probe with missing creds: %v", err)
	}
	if len(c.Sources) != 1 || c.Sources[0].Name() != "cli" {
		t.Fatalf("want probe dropped, got %d sources", len(c.Sources))
	}
	if _, err := buildChain([]string{"bogus"}, missing); err == nil {
		t.Fatal("unknown source: want error")
	}
}

func TestSplitList(t *testing.T) {
	cases := map[string]string{
		"cli,probe":      "[cli probe]",
		" CLI , ,probe ": "[cli probe]",
		"":               "[]",
	}
	for in, want := range cases {
		if got := fmt.Sprint(splitList(in)); got != want {
			t.Errorf("splitList(%q) = %s, want %s", in, got, want)
		}
	}
}
