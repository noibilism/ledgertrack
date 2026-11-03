package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// config is read from the environment so the same binary runs in staging and prod.
type config struct {
	Listen        string        // LISTEN, default :8080
	Bankman       string        // BANKMAN_URL, base of Bankman (scheme://host[:port])
	BankmanHost   string        // BANKMAN_HOST, optional Host header override for Bankman
	LedgerTrack   string        // LEDGERTRACK_URL, base of LedgerTrack (scheme://host[:port])
	LTPrefix      string        // LEDGERTRACK_PREFIX, default /bankman/api/v1
	Workers       int           // SHADOW_WORKERS, partitions replayed in parallel
	QueueSize     int           // SHADOW_QUEUE_SIZE, per partition
	Timeout       time.Duration // SHADOW_TIMEOUT, per replayed call
	MaxBody       int64         // SHADOW_MAX_BODY bytes captured per request or reply
	ReadSample    float64       // SHADOW_READ_SAMPLE, 0..1 share of GETs replayed
	Retries       int           // SHADOW_RETRIES for replays that fail while Bankman succeeded
	IgnoreFields  []string      // SHADOW_IGNORE_FIELDS, JSON keys left out of comparisons
	AdminToken    string        // SHADOW_ADMIN_TOKEN, protects /shadow/*; empty disables those routes
	StartDisabled bool          // SHADOW_DISABLED=true starts as a plain proxy
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func loadConfig() (config, error) {
	c := config{
		Listen: envOr("LISTEN", ":8080"), Bankman: strings.TrimRight(envOr("BANKMAN_URL", ""), "/"),
		BankmanHost: envOr("BANKMAN_HOST", ""), LedgerTrack: strings.TrimRight(envOr("LEDGERTRACK_URL", ""), "/"),
		LTPrefix:   "/" + strings.Trim(envOr("LEDGERTRACK_PREFIX", "/bankman/api/v1"), "/"),
		AdminToken: envOr("SHADOW_ADMIN_TOKEN", ""), StartDisabled: envOr("SHADOW_DISABLED", "") == "true",
		IgnoreFields: strings.FieldsFunc(envOr("SHADOW_IGNORE_FIELDS",
			"created_at,updated_at,approved_at,timestamp,last_activity_at,account_number,message_id"), func(r rune) bool { return r == ',' }),
	}
	if c.Bankman == "" || c.LedgerTrack == "" {
		return c, fmt.Errorf("BANKMAN_URL and LEDGERTRACK_URL are required")
	}
	var err error
	if c.Workers, err = envInt("SHADOW_WORKERS", 32); err != nil || c.Workers < 1 {
		return c, fmt.Errorf("SHADOW_WORKERS must be at least 1")
	}
	if c.QueueSize, err = envInt("SHADOW_QUEUE_SIZE", 10000); err != nil || c.QueueSize < 1 {
		return c, fmt.Errorf("SHADOW_QUEUE_SIZE must be at least 1")
	}
	secs, err := envInt("SHADOW_TIMEOUT_SECONDS", 10)
	if err != nil {
		return c, err
	}
	c.Timeout = time.Duration(secs) * time.Second
	mb, err := envInt("SHADOW_MAX_BODY", 1<<20)
	if err != nil {
		return c, err
	}
	c.MaxBody = int64(mb)
	if c.Retries, err = envInt("SHADOW_RETRIES", 5); err != nil {
		return c, err
	}
	c.ReadSample = 1
	if v := envOr("SHADOW_READ_SAMPLE", ""); v != "" {
		f, perr := strconv.ParseFloat(v, 64)
		if perr != nil || f < 0 || f > 1 {
			return c, fmt.Errorf("SHADOW_READ_SAMPLE must be between 0 and 1")
		}
		c.ReadSample = f
	}
	return c, nil
}
