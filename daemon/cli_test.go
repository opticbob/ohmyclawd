package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func mustLoc(t *testing.T, name string) *time.Location {
	t.Helper()
	l, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func fixtureResult(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	var r struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r.Result
}

func TestParseUsageText(t *testing.T) {
	chi := mustLoc(t, "America/Chicago")
	nyc := mustLoc(t, "America/New_York")
	now := time.Date(2026, 10, 30, 13, 20, 0, 0, chi)
	mins := func(t time.Time) int { return minutesUntil(t.Unix(), now) }

	cases := []struct {
		name    string
		text    string
		want    Usage
		lims    []Limit
		wantErr error
	}{
		{
			name: "real fixture",
			text: fixtureResult(t, "cli_usage_real.json"),
			want: Usage{
				S: 14, SR: mins(time.Date(2026, 10, 30, 16, 40, 0, 0, chi)),
				W: 8, WR: mins(time.Date(2026, 11, 4, 18, 0, 0, 0, chi)),
				ST: "allowed", OK: true, Ts: now.Unix(),
			},
		},
		{
			name: "per-model rows, ansi, time-only reset",
			text: fixtureResult(t, "cli_usage_permodel.json"),
			want: Usage{
				S: 42, SR: mins(time.Date(2026, 10, 30, 16, 59, 0, 0, chi)),
				W: 61, WR: mins(time.Date(2026, 11, 4, 18, 0, 0, 0, chi)),
				ST: "allowed", OK: true, Ts: now.Unix(),
			},
			lims: []Limit{
				{Label: "Sonnet only", Pct: 12, ResetMin: mins(time.Date(2026, 11, 4, 18, 0, 0, 0, chi))},
				{Label: "Fable", Pct: 100, ResetMin: mins(time.Date(2026, 11, 5, 9, 15, 0, 0, nyc))},
			},
		},
		{
			name: "no reset clause",
			text: "Current session: 5% used\nCurrent week (all models): 3% used\n",
			want: Usage{S: 5, W: 3, ST: "allowed", OK: true, Ts: now.Unix()},
		},
		{
			name: "limited at 100",
			text: "Current session: 100% used · resets 4:59pm (America/Chicago)\nCurrent week (all models): 40% used\n",
			want: Usage{S: 100, SR: mins(time.Date(2026, 10, 30, 16, 59, 0, 0, chi)), W: 40, ST: "limited", OK: true, Ts: now.Unix()},
		},
		{
			name:    "missing weekly line",
			text:    "Current session: 5% used\n",
			wantErr: errCLIParse,
		},
		{
			name:    "not logged in",
			text:    fixtureResult(t, "cli_not_logged_in.json"),
			wantErr: errCLINotLoggedIn,
		},
		{
			name:    "garbage",
			text:    "¯\\_(ツ)_/¯\n\x00\x01",
			wantErr: errCLIParse,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, lims, err := parseUsageText(tc.text, now)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				if u != nil {
					t.Fatalf("usage = %+v, want nil on error", u)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if *u != tc.want {
				t.Fatalf("usage = %+v\n want %+v", *u, tc.want)
			}
			if fmt.Sprint(lims) != fmt.Sprint(tc.lims) {
				t.Fatalf("limits = %+v, want %+v", lims, tc.lims)
			}
		})
	}
}

func TestParseReset(t *testing.T) {
	chi := mustLoc(t, "America/Chicago")
	now := time.Date(2026, 10, 30, 13, 20, 0, 0, chi)
	dec := time.Date(2026, 12, 30, 22, 0, 0, 0, chi)

	cases := []struct {
		in   string
		now  time.Time
		want time.Time
		ok   bool
	}{
		{"Oct 30, 4:40pm (America/Chicago)", now, time.Date(2026, 10, 30, 16, 40, 0, 0, chi), true},
		{"Nov 4, 6pm (America/Chicago)", now, time.Date(2026, 11, 4, 18, 0, 0, 0, chi), true},
		{"Aug 21 at 12:29am (America/Chicago)", time.Date(2026, 8, 20, 9, 0, 0, 0, chi), time.Date(2026, 8, 21, 0, 29, 0, 0, chi), true},
		{"Aug 21 at 12pm (UTC)", time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC), time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC), true},
		{"Sept 3, 9am (America/Chicago)", now, time.Date(2027, 9, 3, 9, 0, 0, 0, chi), true}, // >1 day past -> next year
		{"4:59pm (America/Chicago)", now, time.Date(2026, 10, 30, 16, 59, 0, 0, chi), true},
		{"11am (America/Chicago)", now, time.Date(2026, 10, 31, 11, 0, 0, 0, chi), true}, // already passed today -> tomorrow
		{"Jan 2, 5am (America/Chicago)", dec, time.Date(2027, 1, 2, 5, 0, 0, 0, chi), true},
		{"Dec 30, 9pm (America/Chicago)", dec, time.Date(2026, 12, 30, 21, 0, 0, 0, chi), true}, // just passed, not a year off
		{"Nov 4, 6pm (Mars/Olympus_Mons)", now, time.Date(2026, 11, 4, 18, 0, 0, 0, time.Local), true},
		{"Nov 4, 6pm", now, time.Date(2026, 11, 4, 18, 0, 0, 0, time.Local), true},
		{"13pm (UTC)", now, time.Time{}, false},
		{"soon", now, time.Time{}, false},
		{"", now, time.Time{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := parseReset(tc.in, tc.now)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if ok && !got.Equal(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// fakeClaude writes an executable shell script that stands in for claude.
func fakeClaude(t *testing.T, body string) (bin, dir string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("exec tests use a shell script")
	}
	dir = t.TempDir()
	bin = filepath.Join(dir, "claude")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

func TestCLISourceFetch(t *testing.T) {
	abs := func(name string) string {
		p, err := filepath.Abs(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	cases := []struct {
		name    string
		script  string
		wantErr error  // errors.Is target
		errHas  string // substring when wantErr is nil but an error is expected
	}{
		{name: "success", script: "cat " + abs("cli_usage_real.json")},
		{name: "non-zero exit", script: "echo boom >&2; exit 3", errHas: "exit status 3"},
		{name: "is_error true", script: "cat " + abs("cli_is_error.json"), errHas: "API Error: 500"},
		{name: "not logged in", script: "cat " + abs("cli_not_logged_in.json"), wantErr: errCLINotLoggedIn},
		{name: "not json", script: "echo hello", wantErr: errCLIParse},
		{name: "hang killed by timeout", script: "sleep 30 & sleep 30", wantErr: errCLITimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bin, dir := fakeClaude(t, tc.script)
			c := &CLISource{Bin: bin, ConfigDir: dir, Timeout: 300 * time.Millisecond}
			start := time.Now()
			u, err := c.Fetch(context.Background())
			if time.Since(start) > 5*time.Second {
				t.Fatalf("Fetch took %v", time.Since(start))
			}
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case tc.errHas != "":
				if err == nil || !strings.Contains(err.Error(), tc.errHas) {
					t.Fatalf("err = %v, want containing %q", err, tc.errHas)
				}
			default:
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				if u.S != 14 || u.W != 8 || !u.OK || u.ST != "allowed" {
					t.Fatalf("usage = %+v", u)
				}
			}
		})
	}
}

func TestCLISourcePassesArgsAndEnv(t *testing.T) {
	out := filepath.Join(t.TempDir(), "seen")
	real, _ := filepath.Abs("testdata/cli_usage_real.json")
	bin, dir := fakeClaude(t, fmt.Sprintf(
		`printf '%%s\n' "$@" > %s; echo "pwd=$(pwd)" >> %s; echo "au=$DISABLE_AUTOUPDATER" >> %s; echo "cfg=$CLAUDE_CONFIG_DIR" >> %s; cat %s`,
		out, out, out, out, real))
	c := &CLISource{Bin: bin, ConfigDir: dir}
	if _, err := c.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen, _ := os.ReadFile(out)
	want := strings.Join(cliArgs, "\n") + "\npwd=" + dir + "\nau=1\ncfg=" + dir + "\n"
	if string(seen) != want {
		t.Fatalf("seen:\n%s\nwant:\n%s", seen, want)
	}
}

func TestCLISourceNotFound(t *testing.T) {
	c := &CLISource{Bin: filepath.Join(t.TempDir(), "nope")}
	if _, err := c.Fetch(context.Background()); !errors.Is(err, errCLINotFound) {
		t.Fatalf("err = %v, want errCLINotFound", err)
	}
}
