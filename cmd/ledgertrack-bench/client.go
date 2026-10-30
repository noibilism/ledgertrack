package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Class is the outcome category of one request.
type Class int

const (
	ClassOK           Class = iota // 2xx
	ClassInsufficient              // 402, or 400 with INSUFFICIENT_FUND
	ClassConflict                  // 409
	Class4xx                       // other 4xx (and unexpected 3xx)
	Class5xx                       // 5xx
	ClassTimeout                   // client timeout or deadline
	ClassNetwork                   // connection level failure
	ClassDropped                   // open loop arrival never sent (backlog overflow or run ended)
	numClasses
)

var classNames = [numClasses]string{"2xx", "402_insufficient", "409_conflict", "4xx_other", "5xx", "timeout", "network", "dropped"}

func (c Class) String() string { return classNames[c] }

// classify maps a response (or transport error) to a Class.
func classify(status int, body []byte, err error) Class {
	if err != nil {
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			return ClassTimeout
		}
		return ClassNetwork
	}
	switch {
	case status >= 200 && status < 300:
		return ClassOK
	case status == http.StatusPaymentRequired:
		return ClassInsufficient
	case status == http.StatusBadRequest && bytes.Contains(body, []byte("INSUFFICIENT_FUND")):
		return ClassInsufficient
	case status == http.StatusConflict:
		return ClassConflict
	case status >= 500:
		return Class5xx
	default:
		return Class4xx
	}
}

// isError reports whether a class counts against the error rate for an op.
// Insufficient funds is an expected business outcome for ops that spend
// from a wallet, and a defect for the others.
func isError(op Op, c Class) bool {
	switch c {
	case ClassOK:
		return false
	case ClassInsufficient:
		return op == OpCredit || op == OpLienRelease || op == OpLienPay
	default:
		return true
	}
}

// definite reports whether the class proves the server committed nothing.
func definitelyNotCommitted(c Class) bool { return c == ClassInsufficient }

type client struct {
	http   *http.Client
	base   string
	ledger string
	token  string
}

func newClient(cfg *Config) *client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        cfg.Concurrency + cfg.SetupConcurrency + cfg.CheckConcurrency,
		MaxIdleConnsPerHost: cfg.Concurrency + cfg.SetupConcurrency + cfg.CheckConcurrency,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.Insecure}, //nolint:gosec // opt-in flag for lab environments
	}
	return &client{
		http:   &http.Client{Transport: tr},
		base:   cfg.URL,
		ledger: cfg.Ledger,
		token:  cfg.AuthToken,
	}
}

func (c *client) ledgerPath(format string, args ...any) string {
	return c.base + "/" + url.PathEscape(c.ledger) + fmt.Sprintf(format, args...)
}

// response is a fully read HTTP response.
type response struct {
	Status int
	Body   []byte
}

func (c *client) do(ctx context.Context, method, fullURL string, body []byte, idemKey string) (response, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, rdr)
	if err != nil {
		return response{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return response{Status: resp.StatusCode}, err
	}
	return response{Status: resp.StatusCode, Body: data}, nil
}

// getJSON performs a GET and decodes the body with json.Number preserved.
func (c *client) getJSON(ctx context.Context, fullURL string) (int, map[string]any, error) {
	resp, err := c.do(ctx, http.MethodGet, fullURL, nil, "")
	if err != nil {
		return 0, nil, err
	}
	m, _ := decodeObject(resp.Body)
	return resp.Status, m, nil
}

func decodeObject(b []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// dig walks nested "data" envelopes looking for key and returns the first hit.
func dig(m map[string]any, key string) (any, bool) {
	for depth := 0; m != nil && depth < 4; depth++ {
		if v, ok := m[key]; ok {
			return v, true
		}
		next, _ := m["data"].(map[string]any)
		m = next
	}
	return nil, false
}

func digString(m map[string]any, key string) string {
	v, ok := dig(m, key)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

// toInt64 converts a decoded JSON value (json.Number or string) to int64.
func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i, true
		}
		f, err := t.Float64()
		return int64(f), err == nil
	case string:
		var n json.Number = json.Number(strings.TrimSpace(t))
		i, err := n.Int64()
		return i, err == nil
	case float64:
		return int64(t), true
	}
	return 0, false
}
