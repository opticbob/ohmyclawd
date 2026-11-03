package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// publisher is the part of MQTTClient the Publisher needs; tests fake it.
type publisher interface{ Publish(MQTTMessage) }

// Publisher maps daemon state onto MQTT topics under <Prefix>/<Node>/, plus
// Home Assistant discovery configs under <Discovery>/.
type Publisher struct {
	Client      publisher
	Prefix      string // topic root, e.g. "ohmyclawd"
	Node        string // second level and part of HA unique ids
	Discovery   string // HA discovery prefix; "" disables discovery
	SW          SessionWatcher
	State       *State
	MaxAgents   int
	NotifyAfter time.Duration
	Now         func() time.Time // injectable for tests; nil -> time.Now

	fired map[string]bool // pane|status|since already announced on the event topic
}

func (p *Publisher) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Publisher) topic(name string) string { return p.Prefix + "/" + p.Node + "/" + name }

var nodeSanitizer = regexp.MustCompile(`[^a-z0-9_]+`)

// sanitizeNode lowercases s and replaces anything outside [a-z0-9_] with "_",
// so topics and HA entity ids line up.
func sanitizeNode(s string) string {
	s = strings.Trim(nodeSanitizer.ReplaceAllString(strings.ToLower(s), "_"), "_")
	if s == "" {
		return "host"
	}
	return s
}

func defaultNode() string {
	h, _ := os.Hostname()
	return sanitizeNode(h)
}

// mqttUsage is the usage topic: the /usage JSON plus absolute reset times so
// Home Assistant needn't do date math.
type mqttUsage struct {
	usageResponse
	SRA string `json:"sra,omitempty"` // RFC 3339, ts + sr*60
	WRA string `json:"wra,omitempty"` // RFC 3339, ts + wr*60
}

func (p *Publisher) usageMessage(now time.Time) MQTTMessage {
	u, _ := p.State.Load()
	m := mqttUsage{usageResponse: buildUsageResponse(u, p.SW, now)}
	if u.Ts > 0 {
		m.SRA = time.Unix(u.Ts+int64(u.SR)*60, 0).UTC().Format(time.RFC3339)
		m.WRA = time.Unix(u.Ts+int64(u.WR)*60, 0).UTC().Format(time.RFC3339)
	}
	b, _ := json.Marshal(m)
	return MQTTMessage{Topic: p.topic("usage"), Payload: b, Retain: true, Label: "usage"}
}

func (p *Publisher) agentsMessage(now time.Time) (MQTTMessage, AgentsPayload) {
	a := buildAgentsPayload(p.SW, p.MaxAgents, now)
	b, _ := json.Marshal(a)
	return MQTTMessage{Topic: p.topic("agents"), Payload: b, Retain: true, Label: "agents"}, a
}

type agentEvent struct {
	Type  string `json:"event_type"`
	ID    string `json:"id"`
	Dir   string `json:"dir"`
	Agent string `json:"agent"`
	For   int64  `json:"for"`
}

// dueEvents returns an event for each agent that has stayed blocked or done
// for NotifyAfter, once per pane per status entry. A pane that leaves and
// re-enters the status gets a new Since and so fires again.
func (p *Publisher) dueEvents(agents []Agent, now time.Time) []MQTTMessage {
	if p.fired == nil {
		p.fired = make(map[string]bool)
	}
	var out []MQTTMessage
	live := make(map[string]bool)
	for _, a := range agents {
		if a.Status != "blocked" && a.Status != "done" {
			continue
		}
		key := fmt.Sprintf("%s|%s|%d", a.ID, a.Status, a.Since)
		live[key] = true
		held := now.Unix() - a.Since
		if p.fired[key] || held < int64(p.NotifyAfter/time.Second) {
			continue
		}
		p.fired[key] = true
		b, _ := json.Marshal(agentEvent{Type: a.Status, ID: a.ID, Dir: a.Dir, Agent: a.Agent, For: held})
		out = append(out, MQTTMessage{Topic: p.topic("event"), Payload: b, Label: "event"})
	}
	for k := range p.fired {
		if !live[k] {
			delete(p.fired, k)
		}
	}
	return out
}

// allAgents is the uncapped list, so events fire for agents past MaxAgents too.
func (p *Publisher) allAgents() []Agent {
	if src, ok := p.SW.(AgentSource); ok && p.SW.Kind() == "herdr" {
		return src.Agents()
	}
	return nil
}

// Run publishes discovery and initial state, then follows changes until ctx
// is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	for _, m := range p.discoveryMessages() {
		p.Client.Publish(m)
	}
	usageCh := make(chan struct{}, 1)
	p.State.OnStore(func() {
		select {
		case usageCh <- struct{}{}:
		default:
		}
	})
	var changed <-chan struct{}
	if src, ok := p.SW.(AgentSource); ok {
		changed = src.Changed()
	}

	var lastAgents time.Time
	lastKind := ""
	attention := false
	pubAgents := func() {
		now := p.now()
		m, a := p.agentsMessage(now)
		p.Client.Publish(m)
		lastAgents, lastKind = now, a.SW
		attention = a.N["blocked"]+a.N["done"] > 0
	}
	pubAgents()
	p.Client.Publish(p.usageMessage(p.now()))

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-changed:
			pubAgents()
		case <-usageCh:
			p.Client.Publish(p.usageMessage(p.now()))
		case <-tick.C:
			now := p.now()
			// Republish when the watcher flips herdr <-> tmux, and once a
			// minute while something needs attention so HA ages stay fresh.
			if (p.SW != nil && p.SW.Kind() != lastKind) || (attention && now.Sub(lastAgents) >= time.Minute) {
				pubAgents()
			}
		}
		for _, m := range p.dueEvents(p.allAgents(), p.now()) {
			p.Client.Publish(m)
		}
	}
}

// discoveryMessages are the retained Home Assistant MQTT discovery configs.
func (p *Publisher) discoveryMessages() []MQTTMessage {
	if p.Discovery == "" {
		return nil
	}
	device := map[string]any{
		"identifiers":  []string{"ohmyclawd_" + p.Node},
		"name":         "ohmyclawd " + p.Node,
		"manufacturer": "ohmyclawd",
	}
	agents, usage := p.topic("agents"), p.topic("usage")
	type ent struct {
		component, object string
		cfg               map[string]any
	}
	ents := []ent{
		{"sensor", "waiting", map[string]any{"name": "Waiting", "state_topic": agents, "value_template": "{{ value_json.n.blocked }}", "icon": "mdi:account-clock", "state_class": "measurement"}},
		{"sensor", "done", map[string]any{"name": "Done", "state_topic": agents, "value_template": "{{ value_json.n.done }}", "icon": "mdi:check-circle-outline", "state_class": "measurement"}},
		{"sensor", "working", map[string]any{"name": "Working", "state_topic": agents, "value_template": "{{ value_json.n.working }}", "icon": "mdi:cog-outline", "state_class": "measurement"}},
		{"sensor", "agents", map[string]any{
			"name": "Agents", "state_topic": agents, "icon": "mdi:robot-outline",
			"value_template":           "{{ value_json.n.values() | sum }}",
			"json_attributes_topic":    agents,
			"json_attributes_template": `{{ {"agents": value_json.a, "sw": value_json.sw} | tojson }}`,
		}},
		{"binary_sensor", "attention", map[string]any{
			"name": "Attention", "state_topic": agents, "icon": "mdi:bell-alert-outline",
			"value_template": "{{ 'ON' if (value_json.n.blocked + value_json.n.done) > 0 else 'OFF' }}",
		}},
		{"sensor", "session", map[string]any{"name": "Session", "state_topic": usage, "value_template": "{{ value_json.s }}", "unit_of_measurement": "%", "state_class": "measurement", "icon": "mdi:timer-sand"}},
		{"sensor", "week", map[string]any{"name": "Week", "state_topic": usage, "value_template": "{{ value_json.w }}", "unit_of_measurement": "%", "state_class": "measurement", "icon": "mdi:calendar-week"}},
		{"sensor", "session_reset", map[string]any{"name": "Session reset", "state_topic": usage, "value_template": "{{ value_json.sra }}", "device_class": "timestamp"}},
		{"sensor", "week_reset", map[string]any{"name": "Week reset", "state_topic": usage, "value_template": "{{ value_json.wra }}", "device_class": "timestamp"}},
		{"sensor", "usage_source", map[string]any{"name": "Usage source", "state_topic": usage, "value_template": "{{ value_json.src }}", "entity_category": "diagnostic", "icon": "mdi:source-branch"}},
		{"event", "agent", map[string]any{"name": "Agent", "state_topic": p.topic("event"), "event_types": []string{"blocked", "done"}, "icon": "mdi:robot-confused-outline"}},
	}
	out := make([]MQTTMessage, 0, len(ents))
	for _, e := range ents {
		cfg := e.cfg
		cfg["unique_id"] = "ohmyclawd_" + p.Node + "_" + e.object
		cfg["device"] = device
		cfg["availability_topic"] = p.topic("status")
		cfg["payload_available"] = "online"
		cfg["payload_not_available"] = "offline"
		b, _ := json.Marshal(cfg)
		out = append(out, MQTTMessage{
			Topic:   p.Discovery + "/" + e.component + "/ohmyclawd_" + p.Node + "/" + e.object + "/config",
			Payload: b, Retain: true, Label: "config",
		})
	}
	return out
}
