// Command bankman-reconcile compares every account balance in Bankman's database with what LedgerTrack's
// Bankman-compatible API reports for the same account. Run it from the gateway's quiet point before a switch,
// and daily while the two run in parallel. It exits 1 when anything differs.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("BANKMAN_DSN", "")
	ltURL := strings.TrimRight(env("LEDGERTRACK_URL", ""), "/")
	if dsn == "" || ltURL == "" {
		log.Fatal("BANKMAN_DSN (a read replica) and LEDGERTRACK_URL are required")
	}
	prefix := "/" + strings.Trim(env("LEDGERTRACK_PREFIX", "/bankman/api/v1"), "/")
	token := env("LEDGERTRACK_TOKEN", "")
	workers, _ := strconv.Atoi(env("RECONCILE_WORKERS", "32"))
	if workers < 1 {
		workers = 32
	}
	maxReport, _ := strconv.Atoi(env("RECONCILE_MAX_REPORT", "1000"))
	client := &http.Client{Timeout: 15 * time.Second}

	fetch := func(ctx context.Context, id string) (remote, error) {
		var last error
		for try := 0; try < 3; try++ {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ltURL+prefix+"/accounts/"+url.PathEscape(id), nil)
			if err != nil {
				return remote{}, err
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := client.Do(req)
			if err != nil {
				last = err
				time.Sleep(time.Duration(200<<try) * time.Millisecond)
				continue
			}
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 500 {
				last = fmt.Errorf("status %d", resp.StatusCode)
				time.Sleep(time.Duration(200<<try) * time.Millisecond)
				continue
			}
			out := remote{Status: resp.StatusCode}
			var body struct {
				Data struct {
					Currency  string `json:"currency"`
					Available string `json:"available_balance"`
					Actual    string `json:"actual_balance"`
					Locked    string `json:"locked_amount"`
				} `json:"data"`
			}
			if resp.StatusCode/100 == 2 {
				if err := json.Unmarshal(raw, &body); err != nil {
					return remote{}, err
				}
				out.Currency, out.Available, out.Actual, out.Locked = body.Data.Currency, body.Data.Available, body.Data.Actual, body.Data.Locked
			}
			return out, nil
		}
		return remote{}, last
	}

	ctx := context.Background()
	started := time.Now().UTC()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("bankman database: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `select id::text, currency, available_balance::text, actual_balance::text, locked_amount::text
		from accounts where deleted_at is null order by id`)
	if err != nil {
		log.Fatalf("read accounts: %v", err)
	}
	feed := make(chan account, workers*4)
	go func() {
		defer close(feed)
		defer rows.Close()
		for rows.Next() {
			var a account
			if err := rows.Scan(&a.ID, &a.Currency, &a.Available, &a.Actual, &a.Locked); err != nil {
				log.Fatalf("scan: %v", err)
			}
			feed <- a
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("read accounts: %v", err)
		}
	}()

	enc := json.NewEncoder(os.Stdout)
	reported := 0
	sum := run(ctx, feed, fetch, workers, func(m mismatch) {
		if reported < maxReport {
			reported++
			_ = enc.Encode(m)
		}
	})
	_ = enc.Encode(map[string]any{"summary": sum, "started_at": started, "finished_at": time.Now().UTC()})

	// Tell the gateway, so a cutover can be gated on this run.
	if gw := strings.TrimRight(env("GATEWAY_URL", ""), "/"); gw != "" {
		body, _ := json.Marshal(map[string]any{"started_at": started, "checked": sum.Checked, "mismatches": sum.Mismatches})
		req, _ := http.NewRequest(http.MethodPost, gw+"/shadow/reconcile", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+env("SHADOW_ADMIN_TOKEN", ""))
		resp, err := client.Do(req)
		if err != nil {
			log.Fatalf("report to gateway: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			log.Fatalf("report to gateway: status %d", resp.StatusCode)
		}
	}
	if sum.Mismatches > 0 {
		os.Exit(1)
	}
}
