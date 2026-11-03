// Command bankman-shadow sits between Bankman's callers and Bankman. It forwards every call to Bankman and
// returns Bankman's answer untouched. A copy of each answered call is replayed against LedgerTrack's Bankman
// compatibility API, in order per account, and the two answers are compared.
package main

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	logger := json.NewEncoder(os.Stdout)
	s := newShadow(cfg, func(r record) { _ = logger.Encode(map[string]any{"event": "shadow_diff", "record": r}) })
	h, err := s.handler()
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: cfg.Listen, Handler: h, ReadHeaderTimeout: 10 * time.Second}
	log.Printf("bankman-shadow listening on %s, primary %s, shadow %s", cfg.Listen, cfg.Bankman, cfg.LedgerTrack)
	log.Fatal(srv.ListenAndServe())
}

func (s *shadow) handler() (http.Handler, error) {
	bankman, err := url.Parse(s.cfg.Bankman)
	if err != nil {
		return nil, fmt.Errorf("BANKMAN_URL: %w", err)
	}
	lt, err := url.Parse(s.cfg.LedgerTrack)
	if err != nil {
		return nil, fmt.Errorf("LEDGERTRACK_URL: %w", err)
	}
	toBankman := httputil.NewSingleHostReverseProxy(bankman)
	director := toBankman.Director
	toBankman.Director = func(r *http.Request) {
		director(r)
		if s.cfg.BankmanHost != "" {
			r.Host = s.cfg.BankmanHost
		}
	}
	// With LedgerTrack primary the caller's path is mapped onto LedgerTrack's compatibility prefix.
	toLT := httputil.NewSingleHostReverseProxy(lt)
	ltDirector := toLT.Director
	toLT.Director = func(r *http.Request) {
		ltDirector(r)
		r.URL.Path = s.cfg.LTPrefix + rest(r.URL.Path)
		r.URL.RawPath = ""
	}
	capture := func(resp *http.Response) error {
		raw, err := io.ReadAll(io.LimitReader(resp.Body, s.cfg.MaxBody+1))
		if err != nil {
			return err
		}
		// A reply too large to hold is passed through whole and not compared.
		tooBig := int64(len(raw)) > s.cfg.MaxBody
		rest := io.MultiReader(bytes.NewReader(raw), resp.Body)
		resp.Body = struct {
			io.Reader
			io.Closer
		}{rest, resp.Body}
		if tooBig {
			return nil
		}
		if v, ok := resp.Request.Context().Value(ctxKey{}).(*job); ok {
			v.BStatus, v.BBody = resp.StatusCode, raw
			s.offer(*v)
		}
		return nil
	}
	toBankman.ModifyResponse, toLT.ModifyResponse = capture, capture
	mux := http.NewServeMux()
	mux.HandleFunc("/shadow/", s.admin)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		write := r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions
		var primaryLT bool
		if write {
			var ok bool
			if primaryLT, ok = s.admit(w, r); !ok {
				return
			}
			defer s.inflight.Add(-1)
			// Capture the request body for replay and give the proxy its own copy.
			body, err := io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxBody+1))
			if err != nil || int64(len(body)) > s.cfg.MaxBody {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r = withJob(r, &job{Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery, Header: r.Header.Clone(), Body: body, Reverse: primaryLT})
		} else {
			primaryLT = s.primaryIsLT()
			if r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/health") {
				r = withJob(r, &job{Method: r.Method, Path: r.URL.Path, RawQuery: r.URL.RawQuery, Header: r.Header.Clone(), Reverse: primaryLT})
			}
		}
		if primaryLT {
			toLT.ServeHTTP(w, r)
			return
		}
		toBankman.ServeHTTP(w, r)
	})
	return mux, nil
}

func (s *shadow) admin(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if s.cfg.AdminToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AdminToken)) != 1 {
		http.NotFound(w, r)
		return
	}
	if s.cutoverAdmin(w, r) {
		return
	}
	switch {
	case r.URL.Path == "/shadow/metrics":
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "shadow_mirrored_total %d\nshadow_matched_total %d\nshadow_mismatched_total %d\nshadow_errors_total %d\n"+
			"shadow_dropped_total %d\nshadow_retried_total %d\nshadow_skipped_total %d\nshadow_queue_depth %d\nshadow_max_lag_ms %d\nshadow_enabled %d\n",
			s.st.Mirrored.Load(), s.st.Matched.Load(), s.st.Mismatched.Load(), s.st.Errors.Load(), s.st.Dropped.Load(),
			s.st.Retried.Load(), s.st.Skipped.Load(), s.depth(), s.st.MaxLagMillis.Load(), b2i(s.enabled.Load()))
		_, _ = fmt.Fprint(w, cutoverMetrics(s))
	case r.URL.Path == "/shadow/diffs":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.recent())
	case r.URL.Path == "/shadow/pause" && r.Method == http.MethodPost:
		s.enabled.Store(false)
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == "/shadow/resume" && r.Method == http.MethodPost:
		s.enabled.Store(true)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
