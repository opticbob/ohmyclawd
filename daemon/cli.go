package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "time/tzdata" // reset times name IANA zones; embed the db for minimal hosts and Windows
)

var (
	errCLINotFound    = errors.New("claude binary not found")
	errCLITimeout     = errors.New("claude timed out")
	errCLIParse       = errors.New("could not parse claude /usage output")
	errCLINotLoggedIn = errors.New("claude not logged in")
)

// Limit is a per-model weekly limit row from the CLI ("Current week (X)").
type Limit struct {
	Label    string `json:"l"`
	Pct      int    `json:"p"`
	ResetMin int    `json:"r"`
}

// CLISource runs `claude -p /usage` and parses its text output.
type CLISource struct {
	Bin       string // explicit binary path; "" -> auto-detect
	ConfigDir string // passed as CLAUDE_CONFIG_DIR when non-empty
	Every     time.Duration
	Timeout   time.Duration    // 0 -> 45s
	Now       func() time.Time // injectable for tests; nil -> time.Now

	mu    sync.Mutex
	found string // cached auto-detected binary
}

func (c *CLISource) Name() string            { return "cli" }
func (c *CLISource) Interval() time.Duration { return c.Every }

var cliArgs = []string{
	"-p", "/usage",
	"--output-format", "json",
	"--no-session-persistence",
	"--strict-mcp-config",
	"--settings", `{"disableAllHooks":true}`,
}

func (c *CLISource) Fetch(ctx context.Context) (*Usage, error) {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	u, lims, err := c.fetch(ctx, now)
	if err != nil {
		c.mu.Lock()
		c.found = "" // re-detect next time in case the binary moved
		c.mu.Unlock()
		return nil, err
	}
	u.Lim = lims
	return u, nil
}

func (c *CLISource) fetch(ctx context.Context, now func() time.Time) (*Usage, []Limit, error) {
	bin, err := c.binary()
	if err != nil {
		return nil, nil, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(tctx, bin, cliArgs...)
	cmd.Dir = c.workDir()
	cmd.Env = append(os.Environ(), "DISABLE_AUTOUPDATER=1")
	if c.ConfigDir != "" {
		cmd.Env = append(cmd.Env, "CLAUDE_CONFIG_DIR="+c.ConfigDir)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	setProcessGroup(cmd)
	cmd.WaitDelay = 2 * time.Second

	runErr := cmd.Run()
	if tctx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		return nil, nil, fmt.Errorf("%w after %s", errCLITimeout, timeout)
	}
	if errors.Is(runErr, exec.ErrNotFound) || errors.Is(runErr, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("%w: %v", errCLINotFound, runErr)
	}

	var res struct {
		Result  string `json:"result"`
		IsError bool   `json:"is_error"`
	}
	if jerr := json.Unmarshal(stdout.Bytes(), &res); jerr != nil {
		if runErr != nil {
			return nil, nil, fmt.Errorf("claude: %v: %s", runErr, snippet(stderr.String()))
		}
		return nil, nil, fmt.Errorf("%w: %v", errCLIParse, jerr)
	}
	text := stripANSI(res.Result)
	if res.IsError || runErr != nil {
		if notLoggedIn(text) {
			return nil, nil, fmt.Errorf("%w: %s", errCLINotLoggedIn, snippet(text))
		}
		return nil, nil, fmt.Errorf("claude error: %s", snippet(text+" "+stderr.String()))
	}
	return parseUsageText(text, now())
}

// binary resolves the claude executable, caching auto-detection.
func (c *CLISource) binary() (string, error) {
	if c.Bin != "" {
		return c.Bin, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.found != "" {
		return c.found, nil
	}
	if p, err := exec.LookPath("claude"); err == nil {
		c.found = p
		return p, nil
	}
	home, _ := os.UserHomeDir()
	for _, p := range []string{
		filepath.Join(home, ".local", "bin", "claude"),
		filepath.Join(home, ".claude", "local", "claude"),
		"/usr/local/bin/claude",
		"/opt/homebrew/bin/claude",
	} {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			c.found = p
			return p, nil
		}
	}
	return "", errCLINotFound
}

func (c *CLISource) workDir() string {
	if c.ConfigDir != "" {
		return c.ConfigDir
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".claude")
	}
	return ""
}

var (
	ansiRe      = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07]*\x07`)
	usageLineRe = regexp.MustCompile(`(?m)^[ \t]*Current (session|week \(([^)]+)\)): <?(\d+)% used(?:[ \t]*\S[ \t]*resets (.+?))?[ \t]*$`)
	// "Total cost:" is what /usage prints with no claude.ai subscription login.
	loginRe = regexp.MustCompile(`(?im)not logged in|please run /login|/login to|invalid api key|oauth token has expired|^Total cost:`)
)

func stripANSI(s string) string { return ansiRe.ReplaceAllString(s, "") }

func notLoggedIn(s string) bool { return loginRe.MatchString(s) }

func snippet(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}

// parseUsageText extracts the session and all-models weekly utilization from
// the text of `claude -p /usage`. Both lines are required: a partial parse is
// an error so the chain falls back rather than reporting zeros.
func parseUsageText(text string, now time.Time) (*Usage, []Limit, error) {
	text = stripANSI(text)
	u := &Usage{Ts: now.Unix()}
	var lims []Limit
	haveS, haveW := false, false
	for _, m := range usageLineRe.FindAllStringSubmatch(text, -1) {
		pct, _ := strconv.Atoi(m[3])
		resetMin := 0
		if m[4] != "" {
			if t, ok := parseReset(m[4], now); ok {
				resetMin = minutesUntil(t.Unix(), now)
			}
		}
		switch {
		case m[1] == "session":
			u.S, u.SR, haveS = pct, resetMin, true
		case strings.EqualFold(m[2], "all models"):
			u.W, u.WR, haveW = pct, resetMin, true
		default:
			lims = append(lims, Limit{Label: m[2], Pct: pct, ResetMin: resetMin})
		}
	}
	if !haveS || !haveW {
		if notLoggedIn(text) {
			return nil, nil, fmt.Errorf("%w: %s", errCLINotLoggedIn, snippet(text))
		}
		return nil, nil, fmt.Errorf("%w: missing session or weekly line in %q", errCLIParse, snippet(text))
	}
	u.ST = "allowed"
	if u.S >= 100 || u.W >= 100 {
		u.ST = "limited"
	}
	u.S, u.W = clampPct(u.S), clampPct(u.W)
	u.OK = true
	return u, lims, nil
}

func clampPct(n int) int {
	if n > 100 {
		return 100
	}
	return n
}

var resetRe = regexp.MustCompile(`(?i)^(?:([a-z]{3})[a-z]*\.?\s+(\d{1,2})(?:,|\s+at)?\s+)?(\d{1,2})(?::(\d{2}))?\s*([ap]m)\s*(?:\(([^)]+)\))?$`)

// parseReset parses CLI reset times such as "Oct 30, 4:40pm (America/Chicago)",
// "Nov 4, 6pm (America/Chicago)", "Aug 21 at 12:29am" or "4:59pm (UTC)".
// The text carries no year: use now's year, rolling forward when the result
// would be more than a day in the past. A time with no date is the next
// occurrence of that wall-clock time.
func parseReset(s string, now time.Time) (time.Time, bool) {
	m := resetRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return time.Time{}, false
	}
	loc := time.Local
	if m[6] != "" {
		if l, err := time.LoadLocation(m[6]); err == nil {
			loc = l
		}
	}
	hour, _ := strconv.Atoi(m[3])
	minute, _ := strconv.Atoi(m[4]) // "" -> 0
	if hour < 1 || hour > 12 || minute > 59 {
		return time.Time{}, false
	}
	hour %= 12
	if strings.EqualFold(m[5], "pm") {
		hour += 12
	}
	ref := now.In(loc)
	if m[1] == "" {
		t := time.Date(ref.Year(), ref.Month(), ref.Day(), hour, minute, 0, 0, loc)
		if t.Before(now) {
			t = t.AddDate(0, 0, 1)
		}
		return t, true
	}
	mon, err := time.Parse("Jan", strings.ToUpper(m[1][:1])+strings.ToLower(m[1][1:]))
	if err != nil {
		return time.Time{}, false
	}
	day, _ := strconv.Atoi(m[2])
	t := time.Date(ref.Year(), mon.Month(), day, hour, minute, 0, 0, loc)
	if t.Before(now.Add(-24 * time.Hour)) {
		t = t.AddDate(1, 0, 0)
	}
	return t, true
}
