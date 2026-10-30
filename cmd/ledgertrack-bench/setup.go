package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/hotrows"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// baselines are the balances read after setup and before the load starts.
// The consistency check compares final balances with baseline + expected delta,
// which makes reruns against the same wallets (same -prefix) valid. It assumes
// nothing else writes to the benchmark accounts while the run is in progress.
type baselines struct {
	avail, lien []int64 // per wallet
	control     []int64 // per currency, all system:control shards summed
	world       []int64 // per currency
	revAcc      []int64 // per currency, revenue ledger
	revProc     []int64
}

// parallelFor runs fn(0..n-1) with the given parallelism and returns the first error.
func parallelFor(ctx context.Context, n, workers int, fn func(i int) error) error {
	if workers > n {
		workers = n
	}
	inner, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var once sync.Once
	var first error
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for inner.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				if err := fn(i); err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

func walletAddr(user, ccy, kind string) string {
	return fmt.Sprintf("users:%s:wallets:%s:%s", user, ccy, kind)
}

// accountBalance returns the balance of one account for asset, 0 if unknown.
func (c *client) accountBalance(ctx context.Context, address, asset string) (int64, error) {
	status, m, err := c.getJSON(ctx, c.ledgerPath("/accounts/%s?expand=volumes", url.PathEscape(address)))
	if err != nil {
		return 0, err
	}
	switch status {
	case http.StatusNotFound:
		return 0, nil
	case http.StatusOK:
	default:
		return 0, fmt.Errorf("read account %s: unexpected status %d", address, status)
	}
	vols, ok := dig(m, "volumes")
	if !ok {
		return 0, nil // account exists without volumes for this asset
	}
	vm, _ := vols.(map[string]any)
	av, _ := vm[asset].(map[string]any)
	if av == nil {
		return 0, nil
	}
	bal, ok := toInt64(av["balance"])
	if !ok {
		in, ok1 := toInt64(av["input"])
		out, ok2 := toInt64(av["output"])
		if !ok1 || !ok2 {
			return 0, fmt.Errorf("read account %s: no balance in response", address)
		}
		bal = in - out
	}
	return bal, nil
}

// aggregateBalance sums the balances of every account in ledger matching the
// address pattern (partial patterns allowed, see storage filterAccountAddress).
func (c *client) aggregateBalance(ctx context.Context, ledger, pattern, asset string) (int64, error) {
	q, _ := json.Marshal(map[string]any{"$match": map[string]any{"address": pattern}})
	u := c.base + "/" + url.PathEscape(ledger) + "/aggregate/balances?query=" + url.QueryEscape(string(q))
	status, m, err := c.getJSON(ctx, u)
	if err != nil {
		return 0, err
	}
	switch status {
	case http.StatusNotFound:
		return 0, nil
	case http.StatusOK:
	default:
		return 0, fmt.Errorf("aggregate %s %q: unexpected status %d", ledger, pattern, status)
	}
	v, ok := dig(m, asset)
	if !ok {
		return 0, nil
	}
	n, ok := toInt64(v)
	if !ok {
		return 0, fmt.Errorf("aggregate %s %q: bad balance %v", ledger, pattern, v)
	}
	return n, nil
}

func (c *client) ensureLedger(ctx context.Context, name string) error {
	status, _, err := c.getJSON(ctx, c.base+"/"+url.PathEscape(name))
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		return nil
	}
	resp, err := c.do(ctx, http.MethodPost, c.base+"/"+url.PathEscape(name), []byte("{}"), "")
	if err != nil {
		return err
	}
	if resp.Status/100 == 2 || strings.Contains(string(resp.Body), "ALREADY_EXISTS") {
		return nil
	}
	return fmt.Errorf("create ledger %s: status %d: %s", name, resp.Status, truncate(resp.Body))
}

func truncate(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

func minorString(v int64) string { return fmt.Sprintf("%d.%02d", v/100, v%100) }

// setup prepares ledgers, channels and wallets, funds the wallets and reads
// the baselines. It is safe to run repeatedly with the same -prefix.
func setup(ctx context.Context, cfg *Config, cl *client, runID string) (*universe, *baselines, error) {
	uni := newUniverse(cfg)
	nc := len(cfg.Currencies)

	logf("setup: ensuring ledger %q", cfg.Ledger)
	if err := cl.ensureLedger(ctx, cfg.Ledger); err != nil {
		return nil, nil, err
	}

	bl := &baselines{
		avail:   make([]int64, cfg.Wallets),
		lien:    make([]int64, cfg.Wallets),
		control: make([]int64, nc),
		world:   make([]int64, nc),
		revAcc:  make([]int64, nc),
		revProc: make([]int64, nc),
	}

	if cfg.mixUsesChannel() {
		uni.channels = make([][]string, nc)
		for ci, ccy := range cfg.Currencies {
			if err := cl.ensureLedger(ctx, "revenue-"+ccy); err != nil {
				return nil, nil, err
			}
			logf("setup: creating %d channels for %s", cfg.Channels, ccy)
			for k := 0; k < cfg.Channels; k++ {
				id, err := cl.createChannel(ctx, ccy)
				if err != nil {
					return nil, nil, err
				}
				uni.channels[ci] = append(uni.channels[ci], id)
				if cfg.FeeUser > 0 || cfg.FeeProc > 0 {
					if err := cl.configureFees(ctx, id, ccy, cfg); err != nil {
						logf("setup: WARNING fee config for channel %s not applied (%v); fees fall back to amount-channelAmount", id, err)
					}
				}
			}
		}
	}

	logf("setup: creating and funding %d wallets across %v (fund=%d minor units)", cfg.Wallets, cfg.Currencies, cfg.FundAmount)
	var funded atomic.Int64
	err := parallelFor(ctx, cfg.Wallets, cfg.SetupConcurrency, func(i int) error {
		w := uni.wallets[i]
		ccy := cfg.Currencies[w.ccy]
		asset := ccy + "/2"
		resp, err := cl.do(ctx, http.MethodPost, cl.ledgerPath("/wallets"),
			fmt.Appendf(nil, `{"userID":%q,"currency":%q}`, w.user, ccy), "")
		if err != nil {
			return fmt.Errorf("create wallet %s: %w", w.id, err)
		}
		if resp.Status/100 != 2 {
			return fmt.Errorf("create wallet %s: status %d: %s", w.id, resp.Status, truncate(resp.Body))
		}
		avail, err := cl.accountBalance(ctx, walletAddr(w.user, ccy, "available"), asset)
		if err != nil {
			return err
		}
		lien, err := cl.accountBalance(ctx, walletAddr(w.user, ccy, "lien"), asset)
		if err != nil {
			return err
		}
		if avail < cfg.FundAmount {
			// A never-funded wallet uses a fixed reference, so a repeated or
			// concurrent setup cannot fund it twice (the second call is a 409).
			ref := fmt.Sprintf("%s-fund-%s", cfg.Prefix, w.id)
			if avail != 0 || lien != 0 {
				ref = fmt.Sprintf("%s-topup-%s-%s", cfg.Prefix, w.id, runID)
			}
			resp, err := cl.do(ctx, http.MethodPost, cl.ledgerPath("/wallets/%s/credit", w.id),
				fmt.Appendf(nil, `{"amount":%d,"reference":%q}`, cfg.FundAmount-avail, ref), ref)
			if err != nil {
				return fmt.Errorf("fund wallet %s: %w", w.id, err)
			}
			switch {
			case resp.Status/100 == 2:
				avail = cfg.FundAmount
				funded.Add(1)
			case resp.Status == http.StatusConflict:
				// already funded by an earlier or concurrent setup: re-read
				if avail, err = cl.accountBalance(ctx, walletAddr(w.user, ccy, "available"), asset); err != nil {
					return err
				}
			default:
				return fmt.Errorf("fund wallet %s: status %d: %s", w.id, resp.Status, truncate(resp.Body))
			}
		}
		bl.avail[i], bl.lien[i] = avail, lien
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	logf("setup: %d wallets funded or topped up, %d already had enough", funded.Load(), cfg.Wallets-int(funded.Load()))

	for ci, ccy := range cfg.Currencies {
		asset := ccy + "/2"
		if bl.control[ci], err = cl.aggregateBalance(ctx, cl.ledger, systemControlPattern(ccy), asset); err != nil {
			return nil, nil, err
		}
		if bl.world[ci], err = cl.accountBalance(ctx, "world", asset); err != nil {
			return nil, nil, err
		}
		if cfg.mixUsesChannel() {
			rev := "revenue-" + ccy
			if bl.revAcc[ci], err = cl.aggregateBalance(ctx, rev, hotrows.Pattern(hotrows.AccumulatedBase), asset); err != nil {
				return nil, nil, err
			}
			if bl.revProc[ci], err = cl.aggregateBalance(ctx, rev, hotrows.Pattern(hotrows.ProcessingCostBase), asset); err != nil {
				return nil, nil, err
			}
		}
	}
	return uni, bl, nil
}

// systemControlPattern selects system:control:{CCY} and every shard
// system:control:{CCY}:{n}: a trailing empty segment makes the filter a prefix
// match, the same mechanism the finance reports use.
func systemControlPattern(ccy string) string { return "system:control:" + ccy + ":" }

func (c *client) createChannel(ctx context.Context, ccy string) (string, error) {
	resp, err := c.do(ctx, http.MethodPost, c.ledgerPath("/channels"), fmt.Appendf(nil, `{"currency":%q}`, ccy), "")
	if err != nil {
		return "", fmt.Errorf("create channel: %w", err)
	}
	if resp.Status/100 != 2 {
		return "", fmt.Errorf("create channel: status %d: %s", resp.Status, truncate(resp.Body))
	}
	m, _ := decodeObject(resp.Body)
	id := digString(m, "channel_id")
	if id == "" {
		return "", fmt.Errorf("create channel: no channel_id in %s", truncate(resp.Body))
	}
	return id, nil
}

func (c *client) configureFees(ctx context.Context, channelID, ccy string, cfg *Config) error {
	body := fmt.Appendf(nil,
		`{"currency":%q,"enabled":true,"user_fee":{"type":"flat","flat":%q},"processing_fee":{"type":"flat","flat":%q}}`,
		ccy, minorString(cfg.FeeUser), minorString(cfg.FeeProc))
	resp, err := c.do(ctx, http.MethodPut, c.ledgerPath("/channels/%s/fees/config", url.PathEscape(channelID)), body, "")
	if err != nil {
		return err
	}
	if resp.Status/100 != 2 {
		return fmt.Errorf("status %d: %s", resp.Status, truncate(resp.Body))
	}
	return nil
}
