package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// control holds the cutover state. Primary says which ledger answers callers; hold parks new writes so the
// two ledgers can be compared with nothing moving.
type control struct {
	mu        sync.Mutex
	toLT      bool          // LedgerTrack is the primary
	hold      bool          // new writes wait
	release   chan struct{} // closed when the hold ends
	holdAt    time.Time
	drainedAt time.Time // first moment during the hold with no write in flight and no replay pending
	gate      gate
}

// gate is the result of a reconciliation run, posted by the reconciler.
type gate struct {
	Started    time.Time `json:"started_at"`
	Checked    int       `json:"checked"`
	Mismatches int       `json:"mismatches"`
	Received   time.Time `json:"received_at"`
}

func newControl() *control { return &control{release: make(chan struct{})} }

type cutoverStatus struct {
	Primary       string    `json:"primary"`
	Hold          bool      `json:"hold"`
	HoldSince     time.Time `json:"hold_since,omitempty"`
	InflightWrite int64     `json:"inflight_writes"`
	PendingReplay int64     `json:"pending_replays"`
	Drained       bool      `json:"drained"`
	DrainedAt     time.Time `json:"drained_at,omitempty"`
	Gate          gate      `json:"reconcile"`
	GateOK        bool      `json:"reconcile_ok"`
	Ready         bool      `json:"ready_to_switch"`
	Reason        string    `json:"reason,omitempty"`
}

func (s *shadow) primaryName(toLT bool) string {
	if toLT {
		return "ledgertrack"
	}
	return "bankman"
}

// admit lets a write through, or parks it while a hold is on. It returns which ledger is primary at the
// moment the write is admitted and whether the write may go on. A parked write that outlasts the hold limit
// gets a 503 with Retry-After, which Bankman's callers already retry.
func (s *shadow) admit(w http.ResponseWriter, r *http.Request) (toLT, ok bool) {
	deadline := time.NewTimer(s.cfg.HoldMax)
	defer deadline.Stop()
	for {
		c := s.ctl
		c.mu.Lock()
		if !c.hold {
			s.inflight.Add(1)
			toLT = c.toLT
			c.mu.Unlock()
			return toLT, true
		}
		wait := c.release
		c.mu.Unlock()
		select {
		case <-wait:
		case <-deadline.C:
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"error","message":"Ledger switch in progress, retry shortly","data":null}`))
			return false, false
		case <-r.Context().Done():
			return false, false
		}
	}
}

func (s *shadow) primaryIsLT() bool {
	s.ctl.mu.Lock()
	defer s.ctl.mu.Unlock()
	return s.ctl.toLT
}

func (s *shadow) drained() bool { return s.inflight.Load() == 0 && s.pending.Load() == 0 }

// watchDrain stamps the moment a hold becomes quiet. The reconciler must start after that stamp for its
// result to count, since only then are both ledgers still.
func (s *shadow) watchDrain(stop <-chan struct{}) {
	t := time.NewTicker(25 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			c := s.ctl
			c.mu.Lock()
			if c.hold && c.drainedAt.IsZero() && s.drained() {
				c.drainedAt = time.Now()
			}
			c.mu.Unlock()
		}
	}
}

func (s *shadow) startHold() {
	c := s.ctl
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hold {
		return
	}
	c.hold, c.holdAt, c.drainedAt, c.gate = true, time.Now(), time.Time{}, gate{}
	c.release = make(chan struct{})
}

func (s *shadow) endHold() {
	c := s.ctl
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hold {
		return
	}
	c.hold = false
	close(c.release)
}

// status reports the state and, when switching is not yet allowed, why.
func (s *shadow) status(to string) cutoverStatus {
	c := s.ctl
	c.mu.Lock()
	defer c.mu.Unlock()
	st := cutoverStatus{Primary: s.primaryName(c.toLT), Hold: c.hold, HoldSince: c.holdAt, InflightWrite: s.inflight.Load(),
		PendingReplay: s.pending.Load(), DrainedAt: c.drainedAt, Gate: c.gate}
	st.Drained = c.hold && !c.drainedAt.IsZero() && s.drained()
	st.GateOK = !c.gate.Started.IsZero() && !c.drainedAt.IsZero() && !c.gate.Started.Before(c.drainedAt) &&
		c.gate.Checked > 0 && c.gate.Mismatches == 0
	switch {
	case !c.hold:
		st.Reason = "start a hold first"
	case !st.Drained:
		st.Reason = "writes or replays still in flight"
	case c.gate.Started.IsZero():
		st.Reason = "no reconciliation result yet"
	case c.gate.Started.Before(c.drainedAt):
		st.Reason = "the last reconciliation started before the ledgers went quiet; run it again"
	case c.gate.Checked == 0:
		st.Reason = "the reconciliation checked no accounts"
	case c.gate.Mismatches > 0:
		st.Reason = fmt.Sprintf("%d accounts differ", c.gate.Mismatches)
	}
	st.Ready = st.Reason == ""
	if to != "" && to == s.primaryName(c.toLT) {
		st.Ready, st.Reason = false, "already primary"
	}
	return st
}

// switchTo makes the named ledger primary. It only works inside a hold that has drained and been reconciled.
func (s *shadow) switchTo(to string) (cutoverStatus, error) {
	if to != "ledgertrack" && to != "bankman" {
		return cutoverStatus{}, fmt.Errorf("to must be ledgertrack or bankman")
	}
	st := s.status(to)
	if !st.Ready {
		return st, fmt.Errorf("not ready: %s", st.Reason)
	}
	c := s.ctl
	c.mu.Lock()
	c.toLT = to == "ledgertrack"
	c.gate = gate{} // the next switch needs its own reconciliation
	c.mu.Unlock()
	return s.status(""), nil
}

func (s *shadow) cutoverAdmin(w http.ResponseWriter, r *http.Request) bool {
	write := func(code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	switch {
	case r.URL.Path == "/shadow/cutover/status" && r.Method == http.MethodGet:
		write(http.StatusOK, s.status(r.URL.Query().Get("to")))
	case r.URL.Path == "/shadow/cutover/hold" && r.Method == http.MethodPost:
		s.startHold()
		write(http.StatusOK, s.status(""))
	case r.URL.Path == "/shadow/cutover/release" && r.Method == http.MethodPost:
		s.endHold()
		write(http.StatusOK, s.status(""))
	case r.URL.Path == "/shadow/cutover/switch" && r.Method == http.MethodPost:
		st, err := s.switchTo(r.URL.Query().Get("to"))
		if err != nil {
			write(http.StatusConflict, map[string]any{"error": err.Error(), "status": st})
			return true
		}
		write(http.StatusOK, st)
	case r.URL.Path == "/shadow/reconcile" && r.Method == http.MethodPost:
		var g gate
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&g); err != nil || g.Started.IsZero() {
			write(http.StatusBadRequest, map[string]string{"error": "body needs started_at, checked and mismatches"})
			return true
		}
		g.Received = time.Now()
		s.ctl.mu.Lock()
		s.ctl.gate = g
		s.ctl.mu.Unlock()
		write(http.StatusOK, s.status(""))
	default:
		return false
	}
	return true
}

func cutoverMetrics(s *shadow) string {
	st := s.status("")
	return "shadow_inflight_writes " + strconv.FormatInt(st.InflightWrite, 10) +
		"\nshadow_pending_replays " + strconv.FormatInt(st.PendingReplay, 10) +
		"\nshadow_hold " + strconv.Itoa(b2i(st.Hold)) +
		"\nshadow_primary_ledgertrack " + strconv.Itoa(b2i(st.Primary == "ledgertrack")) +
		"\nshadow_reconcile_mismatches " + strconv.Itoa(st.Gate.Mismatches) + "\n"
}
