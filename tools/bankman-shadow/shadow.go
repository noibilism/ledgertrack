package main

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// job is one call Bankman has answered, to be replayed against LedgerTrack.
type job struct {
	Method, Path, RawQuery string
	Header                 http.Header
	Body                   []byte
	BStatus                int
	BBody                  []byte
	Key                    string
	Enqueued               time.Time
	Reverse                bool // LedgerTrack answered the caller and Bankman is the copy
}

// record is what the diff ring and the log keep about a replay.
type record struct {
	At          time.Time `json:"at"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	BankmanCode int       `json:"bankman_status"`
	LTCode      int       `json:"ledgertrack_status"`
	Diffs       []diff    `json:"diffs,omitempty"`
	Error       string    `json:"error,omitempty"`
	LagMillis   int64     `json:"lag_ms"`
}

type stats struct {
	Mirrored, Matched, Mismatched, Dropped, Retried, Errors, Skipped atomic.Int64
	MaxLagMillis                                                     atomic.Int64
}

type shadow struct {
	cfg      config
	client   *http.Client
	ignore   map[string]bool
	revIgn   map[string]bool
	ctl      *control
	inflight atomic.Int64 // writes admitted and not yet answered
	pending  atomic.Int64 // write replays queued or running
	stop     chan struct{}
	queues   []chan job
	enabled  atomic.Bool
	st       stats
	liens    sync.Map // lien transaction id -> account id, learned from Bankman's lien replies
	mu       sync.Mutex
	ring     []record
	wg       sync.WaitGroup
	logf     func(record)
}

func newShadow(cfg config, logf func(record)) *shadow {
	if cfg.HoldMax <= 0 {
		cfg.HoldMax = 30 * time.Second
	}
	s := &shadow{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout}, ignore: map[string]bool{}, revIgn: map[string]bool{},
		ctl: newControl(), stop: make(chan struct{}), logf: logf}
	for _, f := range cfg.IgnoreFields {
		s.ignore[strings.TrimSpace(f)] = true
		s.revIgn[strings.TrimSpace(f)] = true
	}
	// Ids differ when Bankman is the copy: it mints its own for anything LedgerTrack created.
	for _, f := range cfg.ReverseIgnore {
		s.revIgn[strings.TrimSpace(f)] = true
	}
	go s.watchDrain(s.stop)
	s.enabled.Store(!cfg.StartDisabled)
	s.queues = make([]chan job, cfg.Workers)
	for i := range s.queues {
		s.queues[i] = make(chan job, cfg.QueueSize)
		s.wg.Add(1)
		go s.work(s.queues[i])
	}
	return s
}

func (s *shadow) close() {
	close(s.stop)
	for _, q := range s.queues {
		close(q)
	}
	s.wg.Wait()
}

func (s *shadow) depth() int {
	n := 0
	for _, q := range s.queues {
		n += len(q)
	}
	return n
}

// rest is the part of the path after the version segment: Bankman callers use /v1/... and /api/v1/....
func rest(path string) string {
	if i := strings.Index(path, "/v1/"); i >= 0 {
		return path[i+3:]
	}
	if strings.HasSuffix(path, "/v1") {
		return "/"
	}
	return path
}

var (
	accountPath   = regexp.MustCompile(`^/(?:transactions/(?:deposits|withdrawals|liens)|accounts|transactions/accounts)/([^/]+)(?:/history)?$`)
	lienPlacePath = regexp.MustCompile(`^/transactions/liens/([^/]+)$`)
	lienPath      = regexp.MustCompile(`^/transactions/liens/([^/]+)(?:/release-and-withdraw)?$`)
)

// partitionKey picks what orders a call: everything on one account is replayed in the order Bankman saw it.
func (s *shadow) partitionKey(method, p string) string {
	if m := lienPath.FindStringSubmatch(p); m != nil && (method == http.MethodPut || strings.HasSuffix(p, "/release-and-withdraw")) {
		if acct, ok := s.liens.Load(m[1]); ok {
			return acct.(string)
		}
		return "lien:" + m[1]
	}
	if m := accountPath.FindStringSubmatch(p); m != nil {
		return m[1]
	}
	return "misc"
}

// learn records which account a lien belongs to, from Bankman's reply to the placement.
func (s *shadow) learn(method, p string, status int, body []byte) {
	m := lienPlacePath.FindStringSubmatch(p)
	if m == nil || method != http.MethodPost || status/100 != 2 {
		return
	}
	var out struct {
		Data struct {
			TransactionID string `json:"transaction_id"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &out) == nil && out.Data.TransactionID != "" {
		s.liens.Store(out.Data.TransactionID, m[1])
	}
}

// offer queues a call for replay without ever blocking the real request.
func (s *shadow) offer(j job) {
	if !s.enabled.Load() {
		s.st.Skipped.Add(1)
		return
	}
	if j.BStatus >= 500 {
		// Bankman's own outcome is unknown, so a replay cannot be judged. The daily reconciliation covers it.
		s.st.Skipped.Add(1)
		return
	}
	if j.Method == http.MethodGet && s.cfg.ReadSample < 1 && rand.Float64() > s.cfg.ReadSample { //nolint:gosec
		s.st.Skipped.Add(1)
		return
	}
	p := rest(j.Path)
	s.learn(j.Method, p, j.BStatus, j.BBody)
	j.Key = s.partitionKey(j.Method, p)
	j.Enqueued = time.Now()
	h := fnv.New32a()
	_, _ = h.Write([]byte(j.Key))
	if j.Method != http.MethodGet {
		s.pending.Add(1)
	}
	select {
	case s.queues[int(h.Sum32())%len(s.queues)] <- j:
	default:
		if j.Method != http.MethodGet {
			s.pending.Add(-1)
		}
		s.st.Dropped.Add(1)
	}
}

func (s *shadow) work(q chan job) {
	defer s.wg.Done()
	for j := range q {
		s.replay(j)
		if j.Method != http.MethodGet {
			s.pending.Add(-1)
		}
	}
}

// resourceID is the id Bankman gave a created resource, so LedgerTrack can use the same one.
func resourceID(method, p string, body []byte) string {
	var out struct {
		Data struct {
			ID            string `json:"id"`
			TransactionID string `json:"transaction_id"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &out) != nil {
		return ""
	}
	switch {
	case method == http.MethodPost && (p == "/clients" || p == "/accounts"):
		return out.Data.ID
	case method == http.MethodPost && strings.HasPrefix(p, "/transactions/"):
		return out.Data.TransactionID
	}
	return ""
}

func (s *shadow) replay(j job) {
	p := rest(j.Path)
	target := s.cfg.LedgerTrack + s.cfg.LTPrefix + p
	ignore := s.ignore
	if j.Reverse {
		target, ignore = s.cfg.Bankman+j.Path, s.revIgn
	}
	if j.RawQuery != "" {
		target += "?" + j.RawQuery
	}
	var (
		code int
		body []byte
		err  error
	)
	attempts := 1
	if j.Method != http.MethodGet {
		attempts += s.cfg.Retries
	}
	for i := 0; i < attempts; i++ {
		code, body, err = s.send(j, target, p)
		// Bankman succeeded, so a failure here may only mean a prerequisite is still queued on another partition.
		if err == nil && (code < 500 && code != http.StatusNotFound || j.BStatus/100 != 2) {
			break
		}
		if i < attempts-1 {
			s.st.Retried.Add(1)
			time.Sleep(time.Duration(100<<i) * time.Millisecond)
		}
	}
	lag := time.Since(j.Enqueued).Milliseconds()
	for {
		cur := s.st.MaxLagMillis.Load()
		if lag <= cur || s.st.MaxLagMillis.CompareAndSwap(cur, lag) {
			break
		}
	}
	rec := record{At: time.Now().UTC(), Method: j.Method, Path: j.Path, BankmanCode: j.BStatus, LTCode: code, LagMillis: lag}
	s.st.Mirrored.Add(1)
	switch {
	case err != nil:
		s.st.Errors.Add(1)
		rec.Error = err.Error()
	case code != j.BStatus:
		s.st.Mismatched.Add(1)
		rec.Diffs = []diff{{Path: "status", Bankman: itoa(j.BStatus), LedgerTr: itoa(code)}}
		rec.Diffs = append(rec.Diffs, compareReplies(j.BBody, body, ignore)...)
	default:
		if d := compareReplies(j.BBody, body, ignore); len(d) > 0 {
			s.st.Mismatched.Add(1)
			rec.Diffs = d
		} else {
			s.st.Matched.Add(1)
		}
	}
	if err != nil || len(rec.Diffs) > 0 {
		s.keep(rec)
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func (s *shadow) keep(r record) {
	s.mu.Lock()
	s.ring = append(s.ring, r)
	if len(s.ring) > 500 {
		s.ring = s.ring[len(s.ring)-500:]
	}
	s.mu.Unlock()
	if s.logf != nil {
		s.logf(r)
	}
}

func (s *shadow) recent() []record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]record(nil), s.ring...)
}

func (s *shadow) send(j job, target, p string) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, j.Method, target, bytes.NewReader(j.Body))
	if err != nil {
		return 0, nil, err
	}
	for _, k := range []string{"Authorization", "Content-Type", "Accept"} {
		if v := j.Header.Get(k); v != "" {
			req.Header.Set(k, v)
		}
	}
	if j.Reverse && s.cfg.BankmanHost != "" {
		req.Host = s.cfg.BankmanHost
	}
	if id := resourceID(j.Method, p, j.BBody); id != "" && j.BStatus/100 == 2 && !j.Reverse {
		req.Header.Set("X-Bankman-Resource-Id", id)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, s.cfg.MaxBody))
	return resp.StatusCode, out, err
}
