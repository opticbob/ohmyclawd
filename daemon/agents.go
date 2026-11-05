package main

import (
	"crypto/sha256"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Agent is one herdr pane with a detected agent, as published on /agents and
// the MQTT agents topic. Keys are short to keep the payload small for ESPHome.
type Agent struct {
	ID     string `json:"id"`          // herdr pane_id
	Dir    string `json:"d"`           // display dir: basename, or parent/base on collisions
	Agent  string `json:"g"`           // claude, kiro, codex, ...
	Status string `json:"s"`           // blocked|done|working|idle|unknown
	Since  int64  `json:"t"`           // unix secs this pane entered Status (daemon-observed; approximate after a restart)
	Ctx    int    `json:"c,omitempty"` // context window used, 1-100 %; 0 = not reported (only Claude reports it)

	path string // full cwd, used to resolve display-dir collisions
}

// agentStatuses is the display and sort order.
var agentStatuses = []string{"blocked", "done", "working", "idle", "unknown"}

func statusRank(s string) int {
	for i, st := range agentStatuses {
		if st == s {
			return i
		}
	}
	return len(agentStatuses)
}

// sortAgents orders by status (blocked first), then oldest Since, then ID.
func sortAgents(as []Agent) {
	sort.SliceStable(as, func(i, j int) bool {
		ri, rj := statusRank(as[i].Status), statusRank(as[j].Status)
		if ri != rj {
			return ri < rj
		}
		if as[i].Since != as[j].Since {
			return as[i].Since < as[j].Since
		}
		return as[i].ID < as[j].ID
	})
}

// setDisplayDirs fills Dir from each agent's path: the basename, or
// parent/base when two different paths share a basename. home shows as "~".
func setDisplayDirs(as []Agent, home string) {
	paths := make(map[string]map[string]bool) // basename -> distinct paths
	for _, a := range as {
		b := filepath.Base(a.path)
		if paths[b] == nil {
			paths[b] = make(map[string]bool)
		}
		paths[b][a.path] = true
	}
	for i := range as {
		p := as[i].path
		switch {
		case p == "":
			as[i].Dir = "?"
		case home != "" && p == home:
			as[i].Dir = "~"
		case len(paths[filepath.Base(p)]) > 1:
			as[i].Dir = filepath.Join(filepath.Base(filepath.Dir(p)), filepath.Base(p))
		default:
			as[i].Dir = filepath.Base(p)
		}
	}
}

// agentsHash identifies a sorted agent list, ignoring Since.
func agentsHash(as []Agent) [32]byte {
	var b strings.Builder
	for _, a := range as {
		b.WriteString(a.ID + "\x00" + a.Dir + "\x00" + a.Agent + "\x00" + a.Status + "\x00" + strconv.Itoa(a.Ctx) + "\n")
	}
	return sha256.Sum256([]byte(b.String()))
}

// countStatuses returns a count for every known status, zeros included.
func countStatuses(as []Agent) map[string]int {
	n := make(map[string]int, len(agentStatuses))
	for _, s := range agentStatuses {
		n[s] = 0
	}
	for _, a := range as {
		if _, ok := n[a.Status]; ok {
			n[a.Status]++
		} else {
			n["unknown"]++
		}
	}
	return n
}

// AgentSource is implemented by session watchers that know per-agent status
// (herdr). Agents returns nil when that backend isn't the one answering.
type AgentSource interface {
	Agents() []Agent
	Counts() map[string]int
	Changed() <-chan struct{}
}

// AgentsPayload is the body of GET /agents and the MQTT agents topic.
type AgentsPayload struct {
	Ts   int64          `json:"ts"`
	SW   string         `json:"sw"`
	N    map[string]int `json:"n"`
	A    []Agent        `json:"a"`
	More int            `json:"more"`
}

// buildAgentsPayload caps the list at max agents (0 = no cap); the rest are
// counted in More. Counts always cover every agent.
func buildAgentsPayload(sw SessionWatcher, max int, now time.Time) AgentsPayload {
	p := AgentsPayload{Ts: now.Unix(), SW: "none", A: []Agent{}}
	if sw != nil {
		p.SW = sw.Kind()
	}
	var as []Agent
	if src, ok := sw.(AgentSource); ok && p.SW == "herdr" {
		as = src.Agents()
	}
	p.N = countStatuses(as)
	if max > 0 && len(as) > max {
		p.More = len(as) - max
		as = as[:max]
	}
	if as != nil {
		p.A = as
	}
	return p
}
