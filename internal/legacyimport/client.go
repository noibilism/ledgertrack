package legacyimport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// APIError is a non-2xx answer from LedgerTrack.
type APIError struct {
	Method  string
	Path    string
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d %s: %s", e.Method, e.Path, e.Status, e.Code, e.Message)
}

// AmbiguousError means a non-idempotent request failed in a way that leaves its
// outcome unknown (timeout, connection reset or 5xx). The caller must look the
// object up before deciding to retry.
type AmbiguousError struct {
	Method string
	Path   string
	Err    error
}

func (e *AmbiguousError) Error() string {
	return fmt.Sprintf("%s %s: outcome unknown: %v", e.Method, e.Path, e.Err)
}
func (e *AmbiguousError) Unwrap() error { return e.Err }

func isStatus(err error, status int) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == status
}

// IsConflict reports an HTTP 409.
func IsConflict(err error) bool { return isStatus(err, http.StatusConflict) }

// IsNotFound reports an HTTP 404.
func IsNotFound(err error) bool { return isStatus(err, http.StatusNotFound) }

// ClientConfig configures the API client.
type ClientConfig struct {
	BaseURL     string
	Ledger      string
	Token       string
	HTTP        *http.Client
	Rate        float64 // requests per second, 0 = unlimited
	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// Sleep is replaceable in tests.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Client is a minimal LedgerTrack HTTP API client.
type Client struct {
	cfg     ClientConfig
	base    *url.URL
	limiter *limiter
	rngMu   sync.Mutex
	rng     *rand.Rand
}

// NewClient validates the configuration and builds a client.
func NewClient(cfg ClientConfig) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid base URL %q", cfg.BaseURL)
	}
	if cfg.Ledger == "" {
		cfg.Ledger = "ledgertrack"
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = 250 * time.Millisecond
	}
	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = 8 * time.Second
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	return &Client{cfg: cfg, base: u, limiter: newLimiter(cfg.Rate), rng: rand.New(rand.NewSource(time.Now().UnixNano()))}, nil
}

// Target identifies the server and ledger, used to bind a state file to its target.
func (c *Client) Target() string { return c.base.String() + "|" + c.cfg.Ledger }

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// limiter spaces requests at a fixed rate.
type limiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newLimiter(rate float64) *limiter {
	if rate <= 0 {
		return &limiter{}
	}
	return &limiter{interval: time.Duration(float64(time.Second) / rate)}
}

func (l *limiter) wait(ctx context.Context) error {
	if l.interval == 0 {
		return ctx.Err()
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	at := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()
	return sleepCtx(ctx, time.Until(at))
}

type envelope struct {
	Data   json.RawMessage `json:"data"`
	Cursor *struct {
		Data    json.RawMessage `json:"data"`
		HasMore bool            `json:"hasMore"`
		Next    string          `json:"next"`
	} `json:"cursor"`
}

type callOpts struct {
	// idempotent marks a request that is safe to resend verbatim: reads, state
	// transitions that are no-ops when repeated, and writes carrying a
	// deterministic reference plus Idempotency-Key.
	idempotent bool
	query      url.Values
	header     map[string]string
}

func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	d := c.cfg.BackoffBase << uint(attempt)
	if d > c.cfg.BackoffMax || d <= 0 {
		d = c.cfg.BackoffMax
	}
	c.rngMu.Lock()
	j := time.Duration(c.rng.Int63n(int64(d)/2 + 1))
	c.rngMu.Unlock()
	return d/2 + j
}

func (c *Client) path(p string) string { return "/v2/" + url.PathEscape(c.cfg.Ledger) + p }

func (c *Client) call(ctx context.Context, method, p string, body any, o callOpts) (*envelope, http.Header, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return nil, nil, err
		}
	}
	full := c.path(p)
	u := *c.base
	rawPath := strings.TrimRight(u.EscapedPath(), "/") + full
	plainPath, perr := url.PathUnescape(rawPath)
	if perr != nil {
		return nil, nil, perr
	}
	u.Path, u.RawPath = plainPath, rawPath
	if len(o.query) > 0 {
		u.RawQuery = o.query.Encode()
	}
	safe := o.idempotent || method == http.MethodGet
	maxAttempts := 1 + c.cfg.MaxRetries

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.limiter.wait(ctx); err != nil {
			return nil, nil, err
		}
		var rdr io.Reader
		if payload != nil {
			rdr = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), rdr)
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Formance-Bigint-As-String", "true")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.cfg.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
		}
		for k, v := range o.header {
			req.Header.Set(k, v)
		}

		resp, err := c.cfg.HTTP.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			lastErr = err
			if safe && attempt+1 < maxAttempts {
				if serr := c.cfg.Sleep(ctx, c.backoff(attempt, 0)); serr != nil {
					return nil, nil, serr
				}
				continue
			}
			if !safe {
				return nil, nil, &AmbiguousError{Method: method, Path: full, Err: err}
			}
			return nil, nil, fmt.Errorf("%s %s: %w", method, full, err)
		}
		raw, rerr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		_ = resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			if safe && attempt+1 < maxAttempts {
				if serr := c.cfg.Sleep(ctx, c.backoff(attempt, 0)); serr != nil {
					return nil, nil, serr
				}
				continue
			}
			if !safe {
				return nil, nil, &AmbiguousError{Method: method, Path: full, Err: rerr}
			}
			return nil, nil, fmt.Errorf("%s %s: reading body: %w", method, full, rerr)
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var env envelope
			if len(bytes.TrimSpace(raw)) > 0 {
				if err := json.Unmarshal(raw, &env); err != nil {
					return nil, nil, fmt.Errorf("%s %s: decoding response: %w", method, full, err)
				}
			}
			return &env, resp.Header, nil
		}

		apiErr := &APIError{Method: method, Path: full, Status: resp.StatusCode}
		var er struct {
			ErrorCode    string `json:"errorCode"`
			ErrorMessage string `json:"errorMessage"`
		}
		if json.Unmarshal(raw, &er) == nil {
			apiErr.Code, apiErr.Message = er.ErrorCode, er.ErrorMessage
		}
		if apiErr.Message == "" {
			apiErr.Message = strings.TrimSpace(string(truncate(raw, 300)))
		}
		lastErr = apiErr

		// 429 is a definite rejection (nothing executed), so it is always
		// safe to resend. Other retryable answers only for idempotent calls.
		retryable := resp.StatusCode == http.StatusTooManyRequests || (safe && resp.StatusCode >= 500)
		if retryable && attempt+1 < maxAttempts {
			if serr := c.cfg.Sleep(ctx, c.backoff(attempt, retryAfter(resp.Header))); serr != nil {
				return nil, nil, serr
			}
			continue
		}
		if resp.StatusCode >= 500 && !safe {
			return nil, nil, &AmbiguousError{Method: method, Path: full, Err: apiErr}
		}
		return nil, nil, apiErr
	}
	return nil, nil, lastErr
}

func truncate(b []byte, n int) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}

func retryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil && s >= 0 && s <= 60 {
		return time.Duration(s) * time.Second
	}
	return 0
}

func decodeData(env *envelope, out any) error {
	if out == nil {
		return nil
	}
	if env == nil || len(env.Data) == 0 {
		return errors.New("response has no data")
	}
	return json.Unmarshal(env.Data, out)
}

// ---- Remote object types -------------------------------------------------

// CurrencyInfo is an entry of GET /currencies.
type CurrencyInfo struct {
	Code      string `json:"code"`
	Precision int    `json:"precision"`
	Enabled   bool   `json:"enabled"`
}

// RemoteProduct is a product as returned by the API.
type RemoteProduct struct {
	ID          string `json:"id"`
	Code        string `json:"code"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	Currency    string `json:"currency"`
	Status      string `json:"status"`
}

// RemoteClient is a client as returned by the API.
type RemoteClient struct {
	ID           string         `json:"id"`
	ClientNumber string         `json:"client_number"`
	Type         string         `json:"type"`
	Status       string         `json:"status"`
	KYCLevel     int            `json:"kyc_level"`
	KYCStatus    string         `json:"kyc_status"`
	KYCData      map[string]any `json:"kyc_data"`
	Contact      struct {
		Email string `json:"email"`
		Phone string `json:"phone"`
	} `json:"contact"`
	CorporateData *struct {
		LegalName          string `json:"legal_name"`
		RegistrationNumber string `json:"registration_number"`
	} `json:"corporate_data"`
}

// LegacyID is the source_id stored by the import in kyc_data, if any.
func (c *RemoteClient) LegacyID() string {
	if c == nil || c.KYCData == nil {
		return ""
	}
	s, _ := c.KYCData[metaLegacySourceID].(string)
	return s
}

func (c *RemoteClient) legalName() string {
	if c.CorporateData == nil {
		return ""
	}
	return c.CorporateData.LegalName
}

// RemoteKYC is a KYC record.
type RemoteKYC struct {
	ID       string         `json:"id"`
	ClientID string         `json:"client_id"`
	Level    int            `json:"level"`
	Status   string         `json:"status"`
	Payload  map[string]any `json:"payload"`
}

// RemoteAccount is a CBA account as returned by the API.
type RemoteAccount struct {
	ID            string         `json:"id"`
	AccountNumber string         `json:"account_number"`
	ClientID      string         `json:"client_id"`
	ProductID     string         `json:"product_id"`
	Currency      string         `json:"currency"`
	Status        string         `json:"status"`
	WalletID      string         `json:"wallet_id"`
	FreezeDebits  bool           `json:"freeze_debits"`
	Metadata      map[string]any `json:"metadata"`
}

// LegacyID is the source_id stored in account metadata.
func (a *RemoteAccount) LegacyID() string {
	if a == nil || a.Metadata == nil {
		return ""
	}
	s, _ := a.Metadata[metaLegacySourceID].(string)
	return s
}

// bigNum decodes a JSON number or a string holding an integer.
type bigNum struct{ *big.Int }

func (b *bigNum) UnmarshalJSON(data []byte) error {
	s := strings.Trim(strings.TrimSpace(string(data)), `"`)
	if s == "" || s == "null" {
		b.Int = new(big.Int)
		return nil
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return fmt.Errorf("invalid integer %q", s)
	}
	b.Int = n
	return nil
}

// Posting is a ledger posting.
type Posting struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Asset       string `json:"asset"`
	Amount      bigNum `json:"amount"`
}

// Tx is the subset of a ledger transaction the importer reads.
type Tx struct {
	ID        json.Number       `json:"id"`
	Reference string            `json:"reference"`
	Metadata  map[string]string `json:"metadata"`
	Postings  []Posting         `json:"postings"`
}

// Volume is the input/output volume of one asset.
type Volume struct{ Input, Output *big.Int }

// Balance is input minus output.
func (v Volume) Balance() *big.Int {
	in, out := v.Input, v.Output
	if in == nil {
		in = new(big.Int)
	}
	if out == nil {
		out = new(big.Int)
	}
	return new(big.Int).Sub(in, out)
}

// ---- Endpoints -----------------------------------------------------------

// Currencies lists the wallet currency registry.
func (c *Client) Currencies(ctx context.Context) ([]CurrencyInfo, error) {
	env, _, err := c.call(ctx, http.MethodGet, "/currencies", nil, callOpts{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Currencies []CurrencyInfo `json:"currencies"`
	}
	if err := decodeData(env, &out); err != nil {
		return nil, err
	}
	return out.Currencies, nil
}

// ListProducts returns all products.
func (c *Client) ListProducts(ctx context.Context) ([]RemoteProduct, error) {
	env, _, err := c.call(ctx, http.MethodGet, "/products", nil, callOpts{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Products []RemoteProduct `json:"products"`
	}
	return out.Products, decodeData(env, &out)
}

// CreateProduct is NOT idempotent: a duplicate code answers 409, an unknown
// outcome is reported as AmbiguousError.
func (c *Client) CreateProduct(ctx context.Context, body any) (*RemoteProduct, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/products", body, callOpts{})
	if err != nil {
		return nil, err
	}
	var out RemoteProduct
	return &out, decodeData(env, &out)
}

func (c *Client) ActivateProduct(ctx context.Context, id string) (*RemoteProduct, error) {
	return c.productAction(ctx, id, "activate")
}

func (c *Client) RetireProduct(ctx context.Context, id string) (*RemoteProduct, error) {
	return c.productAction(ctx, id, "retire")
}

func (c *Client) productAction(ctx context.Context, id, action string) (*RemoteProduct, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/products/"+url.PathEscape(id)+"/"+action, nil, callOpts{idempotent: true})
	if err != nil {
		return nil, err
	}
	var out RemoteProduct
	return &out, decodeData(env, &out)
}

// ListClients returns all clients.
func (c *Client) ListClients(ctx context.Context) ([]RemoteClient, error) {
	env, _, err := c.call(ctx, http.MethodGet, "/clients", nil, callOpts{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Clients []RemoteClient `json:"clients"`
	}
	return out.Clients, decodeData(env, &out)
}

// CreateClient is NOT idempotent and the API has no natural key to detect a repeat.
func (c *Client) CreateClient(ctx context.Context, body any) (*RemoteClient, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/clients", body, callOpts{})
	if err != nil {
		return nil, err
	}
	var out RemoteClient
	return &out, decodeData(env, &out)
}

func (c *Client) GetClient(ctx context.Context, id string) (*RemoteClient, error) {
	env, _, err := c.call(ctx, http.MethodGet, "/clients/"+url.PathEscape(id), nil, callOpts{})
	if err != nil {
		return nil, err
	}
	var out RemoteClient
	return &out, decodeData(env, &out)
}

func (c *Client) ListKYC(ctx context.Context, clientID string) ([]RemoteKYC, error) {
	env, _, err := c.call(ctx, http.MethodGet, "/clients/"+url.PathEscape(clientID)+"/kyc", nil, callOpts{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Records []RemoteKYC `json:"records"`
	}
	return out.Records, decodeData(env, &out)
}

// SubmitKYC appends a record each time it succeeds, so it is NOT idempotent.
func (c *Client) SubmitKYC(ctx context.Context, clientID string, body any) (*RemoteKYC, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/clients/"+url.PathEscape(clientID)+"/kyc", body, callOpts{})
	if err != nil {
		return nil, err
	}
	var out RemoteKYC
	return &out, decodeData(env, &out)
}

// VerifyKYC is a no-op on an already verified record.
func (c *Client) VerifyKYC(ctx context.Context, clientID, kycID, verifier string) (*RemoteKYC, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/clients/"+url.PathEscape(clientID)+"/kyc/"+url.PathEscape(kycID)+"/verify",
		map[string]string{"verifier": verifier}, callOpts{idempotent: true})
	if err != nil {
		return nil, err
	}
	var out RemoteKYC
	return &out, decodeData(env, &out)
}

func (c *Client) clientAction(ctx context.Context, id, action string, body any) (*RemoteClient, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/clients/"+url.PathEscape(id)+"/"+action, body, callOpts{idempotent: true})
	if err != nil {
		return nil, err
	}
	var out RemoteClient
	return &out, decodeData(env, &out)
}

func (c *Client) ActivateClient(ctx context.Context, id string) (*RemoteClient, error) {
	return c.clientAction(ctx, id, "activate", nil)
}

func (c *Client) SuspendClient(ctx context.Context, id, reason string) (*RemoteClient, error) {
	return c.clientAction(ctx, id, "suspend", map[string]string{"reason": reason})
}

func (c *Client) CloseClient(ctx context.Context, id string) (*RemoteClient, error) {
	return c.clientAction(ctx, id, "close", nil)
}

// ListAccounts returns all CBA accounts.
func (c *Client) ListAccounts(ctx context.Context) ([]RemoteAccount, error) {
	env, _, err := c.call(ctx, http.MethodGet, "/accounts", nil, callOpts{})
	if err != nil {
		return nil, err
	}
	var out struct {
		Accounts []RemoteAccount `json:"accounts"`
	}
	return out.Accounts, decodeData(env, &out)
}

// OpenAccount is NOT idempotent; the wallet id is deterministic so a repeat answers 409.
func (c *Client) OpenAccount(ctx context.Context, body any) (*RemoteAccount, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/accounts", body, callOpts{})
	if err != nil {
		return nil, err
	}
	var out RemoteAccount
	return &out, decodeData(env, &out)
}

func (c *Client) accountAction(ctx context.Context, id, action string) (*RemoteAccount, error) {
	env, _, err := c.call(ctx, http.MethodPost, "/accounts/"+url.PathEscape(id)+"/"+action, nil, callOpts{idempotent: true})
	if err != nil {
		return nil, err
	}
	var out RemoteAccount
	return &out, decodeData(env, &out)
}

func (c *Client) ActivateAccount(ctx context.Context, id string) (*RemoteAccount, error) {
	return c.accountAction(ctx, id, "activate")
}
func (c *Client) SuspendAccount(ctx context.Context, id string) (*RemoteAccount, error) {
	return c.accountAction(ctx, id, "suspend")
}
func (c *Client) FreezeAccount(ctx context.Context, id string) (*RemoteAccount, error) {
	return c.accountAction(ctx, id, "freeze")
}
func (c *Client) DormantAccount(ctx context.Context, id string) (*RemoteAccount, error) {
	return c.accountAction(ctx, id, "dormant")
}
func (c *Client) CloseAccount(ctx context.Context, id string) (*RemoteAccount, error) {
	return c.accountAction(ctx, id, "close")
}

// TxRequest is the body of POST /transactions (numscript flavour).
type TxRequest struct {
	Script    TxScript          `json:"script"`
	Reference string            `json:"reference"`
	Metadata  map[string]string `json:"metadata"`
	Runtime   string            `json:"runtime,omitempty"`
}

// TxScript holds the numscript source.
type TxScript struct {
	Plain string         `json:"plain"`
	Vars  map[string]any `json:"vars"`
}

// CreateTransaction posts through the native endpoint. The reference is
// deterministic and also sent as Idempotency-Key, so the call is safe to
// resend. A 409 means the reference already exists: the caller must compare.
func (c *Client) CreateTransaction(ctx context.Context, req TxRequest) (*Tx, bool, error) {
	if req.Reference == "" {
		return nil, false, errors.New("refusing to post a transaction without a deterministic reference")
	}
	env, hdr, err := c.call(ctx, http.MethodPost, "/transactions", req, callOpts{
		idempotent: true,
		header:     map[string]string{"Idempotency-Key": req.Reference},
	})
	if err != nil {
		return nil, false, err
	}
	var tx Tx
	if err := decodeData(env, &tx); err != nil {
		return nil, false, err
	}
	return &tx, strings.EqualFold(hdr.Get("Idempotency-Hit"), "true"), nil
}

// FindTransactionByReference returns nil when no transaction has the reference.
func (c *Client) FindTransactionByReference(ctx context.Context, ref string) (*Tx, error) {
	q := url.Values{}
	q.Set("pageSize", "2")
	qb, _ := json.Marshal(map[string]any{"$match": map[string]string{"reference": ref}})
	q.Set("query", string(qb))
	env, _, err := c.call(ctx, http.MethodGet, "/transactions", nil, callOpts{query: q})
	if err != nil {
		return nil, err
	}
	if env.Cursor == nil {
		return nil, errors.New("transactions listing has no cursor")
	}
	var txs []Tx
	if err := json.Unmarshal(env.Cursor.Data, &txs); err != nil {
		return nil, err
	}
	for i := range txs {
		if txs[i].Reference == ref {
			return &txs[i], nil
		}
	}
	return nil, nil
}

// LedgerVolumes reads the volumes of a raw ledger address. found is false for
// an address that has never been used (404), which equals a zero balance.
func (c *Client) LedgerVolumes(ctx context.Context, address string) (map[string]Volume, bool, error) {
	q := url.Values{}
	q.Set("expand", "volumes")
	env, _, err := c.call(ctx, http.MethodGet, "/accounts/"+url.PathEscape(address), nil, callOpts{query: q})
	if err != nil {
		if IsNotFound(err) {
			return map[string]Volume{}, false, nil
		}
		return nil, false, err
	}
	var out struct {
		Volumes map[string]struct {
			Input  bigNum `json:"input"`
			Output bigNum `json:"output"`
		} `json:"volumes"`
	}
	if err := decodeData(env, &out); err != nil {
		return nil, false, err
	}
	ret := make(map[string]Volume, len(out.Volumes))
	for asset, v := range out.Volumes {
		ret[asset] = Volume{Input: v.Input.Int, Output: v.Output.Int}
	}
	return ret, true, nil
}
