package main

// Usage is the wire format served on /usage and consumed by the firmware.
// All fields are short keys to keep the payload tiny on the ESP32 side.
type Usage struct {
	S  int    `json:"s"`  // session-window utilization, 0..100
	SR int    `json:"sr"` // minutes until session window resets
	W  int    `json:"w"`  // weekly-window utilization, 0..100
	WR int    `json:"wr"` // minutes until weekly window resets
	ST string `json:"st"` // "allowed" | "limited"
	OK bool   `json:"ok"` // last probe succeeded
	Ts int64  `json:"ts"` // unix seconds of last probe attempt
	CS int    `json:"cs"` // claude sessions (tmux panes)
	CW int    `json:"cw"` // claude sessions waiting for input

	Src  string  `json:"src,omitempty"` // source that produced this data: cli | probe | fake
	Err  string  `json:"err,omitempty"` // last error, when the latest attempt failed
	Lim  []Limit `json:"lim,omitempty"` // per-model weekly limits (cli source only)
	OKTs int64   `json:"-"`             // unix seconds of last successful fetch (internal)
}
