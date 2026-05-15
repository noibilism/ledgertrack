package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	baseURL   string
	ledger    string
	token     string
	rps       int
	duration  time.Duration
	workers   int
	accounts  int
	currency  string
	runID     string
	seedOnly  bool
	runOnly   bool
	outFile   string
	liquidity int64
	reqTimeout time.Duration
}

type apiClient struct {
	cfg    config
	client *http.Client
}

type apiErr struct {
	status int
	body   string
}

func (e *apiErr) Error() string {
	return fmt.Sprintf("http %d: %s", e.status, e.body)
}

func newClient(cfg config) *apiClient {
	tr := &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
	}
	return &apiClient{
		cfg: cfg,
		client: &http.Client{
			Timeout:   0,
			Transport: tr,
		},
	}
}

func (c *apiClient) req(ctx context.Context, method, path string, body any) (*http.Response, []byte, error) {
	if c.cfg.reqTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.reqTimeout)
		defer cancel()
	}
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, nil, err
		}
		rdr = bytes.NewReader(b)
	}
	u := strings.TrimRight(c.cfg.baseURL, "/") + path
	r, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, nil, err
	}
	r.Header.Set("Accept", "application/json")
	r.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(c.cfg.token) != "" {
		r.Header.Set("Authorization", "Bearer "+c.cfg.token)
	}

	resp, err := c.client.Do(r)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
	}()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp, b, &apiErr{status: resp.StatusCode, body: string(b)}
	}
	return resp, b, nil
}

func extractData[T any](b []byte, out *T) error {
	var env struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return err
	}
	return json.Unmarshal(env.Data, out)
}

func (c *apiClient) ensureLedger(ctx context.Context, name string) error {
	_, b, err := c.req(ctx, http.MethodPost, "/v2/"+name, nil)
	if err == nil {
		return nil
	}
	var payload struct {
		ErrorCode string `json:"errorCode"`
	}
	if json.Unmarshal(b, &payload) == nil && payload.ErrorCode == "LEDGER_ALREADY_EXISTS" {
		return nil
	}
	return err
}

func (c *apiClient) createProduct(ctx context.Context, currency, code, name string) (string, error) {
	body := map[string]any{
		"code":        code,
		"name":        name,
		"description": "perf product",
		"category":    "savings",
		"currency":    currency,
		"rules": map[string]any{
			"requires_kyc_level":    1,
			"min_opening_balance":   "0.00",
			"min_balance":           "0",
			"allow_negative_balance": false,
			"allow_debits":          true,
			"allow_credits":         true,
			"transaction_limits": map[string]any{
				"single_debit_limit":  "1000000.00",
				"single_credit_limit": "1000000.00",
			},
		},
	}
	_, b, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/products", body)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := extractData(b, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *apiClient) activateProduct(ctx context.Context, productID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/products/"+productID+"/activate", nil)
	return err
}

func (c *apiClient) retireProduct(ctx context.Context, productID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/products/"+productID+"/retire", nil)
	return err
}

func (c *apiClient) createClient(ctx context.Context, email, phone, runID string) (string, error) {
	body := map[string]any{
		"type": "individual",
		"contact": map[string]any{
			"email": email,
			"phone": phone,
		},
		"individual_data": map[string]any{
			"first_name":        "Perf",
			"last_name":         runID[:8],
			"national_id_type":  "NIN",
			"national_id_number": "NIN-" + runID[:10],
			"nationality":       "NG",
			"occupation":        "Perf",
		},
	}
	_, b, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/clients", body)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := extractData(b, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *apiClient) submitKYC(ctx context.Context, clientID, runID string) (string, error) {
	body := map[string]any{
		"level": 1,
		"documents": []map[string]any{
			{
				"type":      "id",
				"reference": "doc-" + runID,
				"provider":  "perf",
			},
		},
		"payload": map[string]any{
			"run_id": runID,
		},
	}
	_, b, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/clients/"+clientID+"/kyc", body)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := extractData(b, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *apiClient) verifyKYC(ctx context.Context, clientID, kycID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/clients/"+clientID+"/kyc/"+kycID+"/verify", map[string]any{"verifier": "perf"})
	return err
}

func (c *apiClient) activateClient(ctx context.Context, clientID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/clients/"+clientID+"/activate", nil)
	return err
}

func (c *apiClient) closeClient(ctx context.Context, clientID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/clients/"+clientID+"/close", nil)
	return err
}

func (c *apiClient) openAccount(ctx context.Context, clientID, productID, openingDeposit string) (string, error) {
	body := map[string]any{
		"client_id":       clientID,
		"product_id":      productID,
		"opening_deposit": openingDeposit,
	}
	_, b, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts", body)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := extractData(b, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

func (c *apiClient) activateAccount(ctx context.Context, accountID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/activate", nil)
	return err
}

func (c *apiClient) closeAccount(ctx context.Context, accountID string) error {
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/close", nil)
	return err
}

func (c *apiClient) accountBalance(ctx context.Context, accountID string) (int64, error) {
	_, b, err := c.req(ctx, http.MethodGet, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/balance", nil)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Balance int64 `json:"balance"`
	}
	if err := extractData(b, &resp); err != nil {
		return 0, err
	}
	return resp.Balance, nil
}

func (c *apiClient) credit(ctx context.Context, accountID string, amount int64, reference string) error {
	body := map[string]any{
		"amount":    strconv.FormatInt(amount, 10),
		"reference": reference,
		"metadata":  map[string]any{},
	}
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/credit", body)
	return err
}

func (c *apiClient) debit(ctx context.Context, accountID string, amount int64, reference string) error {
	body := map[string]any{
		"amount":    strconv.FormatInt(amount, 10),
		"reference": reference,
		"metadata":  map[string]any{},
	}
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/debit", body)
	return err
}

func (c *apiClient) lien(ctx context.Context, accountID string, amount int64, reference string) error {
	body := map[string]any{
		"amount":    strconv.FormatInt(amount, 10),
		"reference": reference,
		"metadata":  map[string]any{},
	}
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/lien", body)
	return err
}

func (c *apiClient) releaseLien(ctx context.Context, accountID string, amount int64, reference string) error {
	body := map[string]any{
		"amount":    strconv.FormatInt(amount, 10),
		"reference": reference,
		"mode":      "RELEASE",
	}
	_, _, err := c.req(ctx, http.MethodPost, "/v2/"+c.cfg.ledger+"/accounts/"+accountID+"/lien/release", body)
	return err
}

type accountState struct {
	id          string
	available   int64
	lienBalance int64
	mu          sync.Mutex
}

type action string

const (
	actCredit       action = "credit"
	actDebit        action = "debit"
	actLien         action = "lien"
	actLienRelease  action = "lien_release"
)

type sample struct {
	ok      bool
	code    int
	latency time.Duration
}

type stats struct {
	mu      sync.Mutex
	samples map[action][]time.Duration
	ok      map[action]int64
	fail    map[action]int64
	codes   map[action]map[int]int64
}

func newStats() *stats {
	return &stats{
		samples: map[action][]time.Duration{
			actCredit:      {},
			actDebit:       {},
			actLien:        {},
			actLienRelease: {},
		},
		ok: map[action]int64{
			actCredit:      0,
			actDebit:       0,
			actLien:        0,
			actLienRelease: 0,
		},
		fail: map[action]int64{
			actCredit:      0,
			actDebit:       0,
			actLien:        0,
			actLienRelease: 0,
		},
		codes: map[action]map[int]int64{
			actCredit:      {},
			actDebit:       {},
			actLien:        {},
			actLienRelease: {},
		},
	}
}

func (s *stats) add(a action, ok bool, code int, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ok {
		s.ok[a]++
	} else {
		s.fail[a]++
	}
	if code != 0 {
		if s.codes[a] == nil {
			s.codes[a] = map[int]int64{}
		}
		s.codes[a][code]++
	}
	s.samples[a] = append(s.samples[a], d)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := int(float64(len(sorted)-1) * (p / 100.0))
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

type resultRow struct {
	Action    action        `json:"action"`
	OK        int64         `json:"ok"`
	Fail      int64         `json:"fail"`
	Codes     map[int]int64 `json:"codes"`
	P50       time.Duration `json:"p50"`
	P95       time.Duration `json:"p95"`
	P99       time.Duration `json:"p99"`
	Min       time.Duration `json:"min"`
	Max       time.Duration `json:"max"`
	Samples   int           `json:"samples"`
}

func (s *stats) summarize() []resultRow {
	s.mu.Lock()
	defer s.mu.Unlock()

	actions := []action{actCredit, actDebit, actLien, actLienRelease}
	out := make([]resultRow, 0, len(actions))
	for _, a := range actions {
		ss := append([]time.Duration(nil), s.samples[a]...)
		sort.Slice(ss, func(i, j int) bool { return ss[i] < ss[j] })
		var mn, mx time.Duration
		if len(ss) > 0 {
			mn = ss[0]
			mx = ss[len(ss)-1]
		}
		out = append(out, resultRow{
			Action:  a,
			OK:      s.ok[a],
			Fail:    s.fail[a],
			Codes:   cloneCodes(s.codes[a]),
			P50:     percentile(ss, 50),
			P95:     percentile(ss, 95),
			P99:     percentile(ss, 99),
			Min:     mn,
			Max:     mx,
			Samples: len(ss),
		})
	}
	return out
}

func cloneCodes(m map[int]int64) map[int]int64 {
	if m == nil {
		return map[int]int64{}
	}
	out := make(map[int]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

type provisioned struct {
	ProductID string   `json:"product_id"`
	ClientIDs []string `json:"client_ids"`
	AccountIDs []string `json:"account_ids"`
}

func provision(ctx context.Context, c *apiClient, cfg config) (*provisioned, []*accountState, error) {
	if err := c.ensureLedger(ctx, cfg.ledger); err != nil {
		return nil, nil, err
	}
	if err := c.ensureLedger(ctx, "channels-"+cfg.currency); err != nil {
		return nil, nil, err
	}
	if err := c.ensureLedger(ctx, "revenue-"+cfg.currency); err != nil {
		return nil, nil, err
	}

	productCode := "PERF-" + cfg.currency + "-" + cfg.runID
	productID, err := c.createProduct(ctx, cfg.currency, productCode, "Perf "+cfg.currency+" "+cfg.runID)
	if err != nil {
		return nil, nil, err
	}
	if err := c.activateProduct(ctx, productID); err != nil {
		return nil, nil, err
	}

	clientIDs := make([]string, 0, cfg.accounts)
	accountIDs := make([]string, 0, cfg.accounts)
	accounts := make([]*accountState, 0, cfg.accounts)

	for i := 0; i < cfg.accounts; i++ {
		suffix := fmt.Sprintf("%s-%d", cfg.runID, i)
		email := fmt.Sprintf("perf-%s@example.com", suffix)
		phone := fmt.Sprintf("+234900%06d", rand.Intn(999999))
		clientID, err := c.createClient(ctx, email, phone, suffix)
		if err != nil {
			return nil, nil, err
		}
		kycID, err := c.submitKYC(ctx, clientID, suffix)
		if err != nil {
			return nil, nil, err
		}
		if err := c.verifyKYC(ctx, clientID, kycID); err != nil {
			return nil, nil, err
		}
		if err := c.activateClient(ctx, clientID); err != nil {
			return nil, nil, err
		}
		accountID, err := c.openAccount(ctx, clientID, productID, "10000.00")
		if err != nil {
			return nil, nil, err
		}
		if err := c.activateAccount(ctx, accountID); err != nil {
			return nil, nil, err
		}
		bal, err := c.accountBalance(ctx, accountID)
		if err != nil {
			return nil, nil, err
		}

		clientIDs = append(clientIDs, clientID)
		accountIDs = append(accountIDs, accountID)
		accounts = append(accounts, &accountState{id: accountID, available: bal})
	}

	return &provisioned{
		ProductID: productID,
		ClientIDs: clientIDs,
		AccountIDs: accountIDs,
	}, accounts, nil
}

type job struct{}

func runLoad(ctx context.Context, c *apiClient, cfg config, accounts []*accountState) (*stats, error) {
	stats := newStats()
	var refCounter uint64

	planned := int(float64(cfg.rps) * cfg.duration.Seconds())
	if planned < 1 {
		planned = 1
	}
	work := make(chan job, planned+cfg.workers*4)
	var wg sync.WaitGroup

	doAction := func(r *rand.Rand) action {
		x := r.Intn(4)
		switch x {
		case 0:
			return actCredit
		case 1:
			return actDebit
		case 2:
			return actLien
		default:
			return actLienRelease
		}
	}

	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*997)))
			for range work {
				idx := r.Intn(len(accounts))
				acc := accounts[idx]

				var a action
				var amt int64 = 100
				var lienAmt int64
				var ok bool
				var code int
				start := time.Now()

				acc.mu.Lock()
				chosen := doAction(r)
				if acc.available < 1000 {
					chosen = actCredit
				}
				if chosen == actLienRelease {
					if acc.lienBalance <= 0 {
						chosen = actLien
					} else {
						lienAmt = min64(amt, acc.lienBalance)
					}
				}
				if chosen == actLien {
					if acc.available < amt {
						chosen = actCredit
					}
				}
				if chosen == actDebit {
					if acc.available < amt {
						chosen = actCredit
					}
				}
				a = chosen

				ref := fmt.Sprintf("perf-%s-%d", cfg.runID, atomic.AddUint64(&refCounter, 1))
				var err error

				switch a {
				case actCredit:
					err = c.credit(ctx, acc.id, amt, ref)
				case actDebit:
					err = c.debit(ctx, acc.id, amt, ref)
				case actLien:
					err = c.lien(ctx, acc.id, amt, ref)
				case actLienRelease:
					if lienAmt <= 0 {
						lienAmt = amt
					}
					err = c.releaseLien(ctx, acc.id, lienAmt, ref)
				}

				elapsed := time.Since(start)
				if err == nil {
					ok = true
					switch a {
					case actCredit:
						acc.available += amt
					case actDebit:
						acc.available -= amt
					case actLien:
						acc.available -= amt
						acc.lienBalance += amt
					case actLienRelease:
						acc.available += lienAmt
						acc.lienBalance -= lienAmt
						if acc.lienBalance < 0 {
							acc.lienBalance = 0
						}
					}
				} else {
					var ae *apiErr
					if ok2 := errors.As(err, &ae); ok2 {
						code = ae.status
					} else {
						code = -1
					}
				}
				acc.mu.Unlock()

				stats.add(a, ok, code, elapsed)
			}
		}(w)
	}

	start := time.Now()
	end := start.Add(cfg.duration)
	interval := time.Second / time.Duration(cfg.rps)
	next := start
	for time.Now().Before(end) {
		select {
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return stats, ctx.Err()
		default:
		}
		now := time.Now()
		if now.Before(next) {
			time.Sleep(next.Sub(now))
		}
		next = next.Add(interval)
		select {
		case work <- job{}:
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return stats, ctx.Err()
		}
	}
	close(work)
	wg.Wait()
	return stats, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func main() {
	var cfg config
	flag.StringVar(&cfg.baseURL, "base-url", "http://127.0.0.1:3068", "base url")
	flag.StringVar(&cfg.ledger, "ledger", "ledgertrack", "ledger")
	flag.StringVar(&cfg.token, "token", "local-no-auth", "token")
	flag.IntVar(&cfg.rps, "rps", 1000, "rps")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "duration")
	flag.IntVar(&cfg.workers, "workers", 200, "workers")
	flag.IntVar(&cfg.accounts, "accounts", 100, "accounts")
	flag.StringVar(&cfg.currency, "currency", "USD", "currency")
	flag.StringVar(&cfg.runID, "run-id", "", "run id")
	flag.BoolVar(&cfg.seedOnly, "seed-only", false, "seed only")
	flag.BoolVar(&cfg.runOnly, "run-only", false, "run only")
	flag.StringVar(&cfg.outFile, "out", "/tmp/ledgertrack_accounts_perf.json", "out")
	flag.DurationVar(&cfg.reqTimeout, "request-timeout", 5*time.Second, "per-request timeout")
	flag.Parse()

	if strings.TrimSpace(cfg.runID) == "" {
		cfg.runID = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	client := newClient(cfg)

	var prov *provisioned
	var accounts []*accountState

	if !cfg.runOnly {
		p, accs, err := provision(ctx, client, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		prov, accounts = p, accs
		b, _ := json.MarshalIndent(prov, "", "  ")
		_ = os.WriteFile("/tmp/ledgertrack_accounts_perf_seed.json", b, 0o600)
	} else {
		seedBytes, err := os.ReadFile("/tmp/ledgertrack_accounts_perf_seed.json")
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		var p provisioned
		if err := json.Unmarshal(seedBytes, &p); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		prov = &p
		accounts = make([]*accountState, 0, len(p.AccountIDs))
		for _, id := range p.AccountIDs {
			bal, err := client.accountBalance(ctx, id)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
			accounts = append(accounts, &accountState{id: id, available: bal})
		}
	}

	if cfg.seedOnly {
		fmt.Printf("seeded %d accounts\n", len(accounts))
		return
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.duration+30*time.Second)
	defer cancel()
	st, err := runLoad(runCtx, client, cfg, accounts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	summary := st.summarize()
	var total int64
	for _, row := range summary {
		total += row.OK + row.Fail
	}
	achievedRPS := float64(total) / cfg.duration.Seconds()

	out := map[string]any{
		"base_url":  cfg.baseURL,
		"ledger":    cfg.ledger,
		"currency":  cfg.currency,
		"run_id":    cfg.runID,
		"rps":       cfg.rps,
		"achieved_rps": achievedRPS,
		"duration":  cfg.duration.String(),
		"workers":   cfg.workers,
		"accounts":  cfg.accounts,
		"summary":   summary,
		"seed_file": "/tmp/ledgertrack_accounts_perf_seed.json",
		"product_id": prov.ProductID,
	}

	b, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile(cfg.outFile, b, 0o600)
	fmt.Println(string(b))
}
