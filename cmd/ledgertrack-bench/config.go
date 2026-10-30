package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Op is one kind of wallet API operation the harness can issue.
type Op int

const (
	OpCredit Op = iota
	OpDebit
	OpLien
	OpLienRelease // release_only: lien -> available
	OpLienPay     // PAY mode with channel + fees: lien -> world, channel and revenue legs
	OpChannelDebit
	numOps
)

var opNames = [numOps]string{"credit", "debit", "lien", "lien_release", "lien_pay", "channel_debit"}

func (o Op) String() string { return opNames[o] }

func (o Op) usesChannel() bool { return o == OpLienPay || o == OpChannelDebit }

// parseOp accepts the canonical names plus a few aliases.
func parseOp(s string) (Op, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "-", "_")
	switch s {
	case "release", "lienrelease":
		s = "lien_release"
	case "pay", "lienpay":
		s = "lien_pay"
	case "channel", "channeldebit", "channel_debit_fees":
		s = "channel_debit"
	}
	for i, n := range opNames {
		if n == s {
			return Op(i), true
		}
	}
	return 0, false
}

type MixEntry struct {
	Op     Op
	Weight float64
}

const defaultMix = "credit=35,debit=35,lien=10,lien_release=8,channel_debit=12"

// parseMix parses "credit=30,debit=40,..." into weighted entries.
func parseMix(s string) ([]MixEntry, error) {
	var out []MixEntry
	seen := map[Op]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("mix entry %q must look like op=weight", part)
		}
		op, ok := parseOp(k)
		if !ok {
			return nil, fmt.Errorf("unknown op %q in mix (valid: %s)", k, strings.Join(opNames[:], ", "))
		}
		w, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || w < 0 {
			return nil, fmt.Errorf("invalid weight %q for %s", v, k)
		}
		if seen[op] {
			return nil, fmt.Errorf("op %s appears twice in mix", op)
		}
		seen[op] = true
		if w > 0 {
			out = append(out, MixEntry{Op: op, Weight: w})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("mix needs at least one op with a positive weight")
	}
	return out, nil
}

// Config holds every run parameter. Secrets are never serialised in reports.
type Config struct {
	URL       string
	Ledger    string
	AuthToken string
	Insecure  bool

	Duration    time.Duration
	Warmup      time.Duration
	Timeout     time.Duration
	Rate        float64 // target TPS, 0 = closed loop (max throughput)
	Arrival     string  // constant | poisson (open loop only)
	Concurrency int
	MaxBacklog  int

	Wallets    int
	Currencies []string
	Channels   int // channels per currency
	Mix        []MixEntry
	MixSpec    string
	Dist       string // zipf | uniform
	ZipfS      float64

	FundAmount int64
	AmountMin  int64
	AmountMax  int64
	FeeUser    int64 // flat user fee in minor units configured on channels
	FeeProc    int64 // flat processing fee in minor units configured on channels

	Prefix           string
	Seed             int64
	Idempotency      bool
	SetupConcurrency int

	Check            bool
	CheckConcurrency int

	Label  string
	Shards int // informational only: the server reads CBA_SYSTEM_CONTROL_SHARDS

	JSONOut string
	CSVOut  string
	Quiet   bool

	// SLO gates. Zero (or negative for the error rate) disables a gate.
	MinTPS             float64
	MinWindowTPS       float64
	MaxP99             time.Duration
	MaxErrorRate       float64
	MaxInsufficientPct float64

	// MaxUncertainFrac makes the consistency check fail when more than this fraction of the operations
	// (shed ones excluded) had an unknown outcome. Negative disables the cap.
	MaxUncertainFrac float64
}

func (c *Config) openLoop() bool { return c.Rate > 0 }

func (c *Config) mixUsesChannel() bool {
	for _, m := range c.Mix {
		if m.Op.usesChannel() {
			return true
		}
	}
	return false
}

func (c *Config) mixUses(op Op) bool {
	for _, m := range c.Mix {
		if m.Op == op {
			return true
		}
	}
	return false
}

func parseConfig(args []string, stderr io.Writer) (*Config, error) {
	c := &Config{}
	fs := flag.NewFlagSet("ledgertrack-bench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: ledgertrack-bench [flags]\n\nLoad generator and consistency checker for the LedgerTrack wallet API.\n\n")
		fs.PrintDefaults()
	}

	var currencies string
	fs.StringVar(&c.URL, "url", "http://localhost:3068/v2", "API base URL including the version prefix, e.g. https://host/v2")
	fs.StringVar(&c.Ledger, "ledger", "ledgertrack", "ledger that holds the wallets (created if missing)")
	fs.StringVar(&c.AuthToken, "auth-token", "", "bearer token sent as the Authorization header (env LEDGERTRACK_BENCH_TOKEN is used when empty)")
	fs.BoolVar(&c.Insecure, "insecure", false, "skip TLS certificate verification")

	fs.DurationVar(&c.Duration, "duration", 60*time.Second, "measured run length, after warmup")
	fs.DurationVar(&c.Warmup, "warmup", 10*time.Second, "warmup length; traffic is sent but not measured")
	fs.DurationVar(&c.Timeout, "timeout", 10*time.Second, "per request timeout")
	fs.Float64Var(&c.Rate, "rate", 0, "target TPS for open-loop mode (arrivals are scheduled independently of responses); 0 selects closed-loop max-throughput mode")
	fs.StringVar(&c.Arrival, "arrival", "constant", "open-loop arrival process: constant or poisson")
	fs.IntVar(&c.Concurrency, "concurrency", 256, "max in-flight requests (worker count); in open loop, arrivals beyond it queue and the queue time counts as latency")
	fs.IntVar(&c.MaxBacklog, "max-backlog", 100000, "open loop: queued arrivals beyond this are dropped and counted as errors")

	fs.IntVar(&c.Wallets, "wallets", 1000, "number of wallets, spread round-robin over the currencies")
	fs.StringVar(&currencies, "currencies", "USD", "comma separated currency list; all share the same -ledger")
	fs.IntVar(&c.Channels, "channels", 4, "channels created per currency (used by channel_debit and lien_pay)")
	fs.StringVar(&c.MixSpec, "mix", defaultMix, "workload mix as op=weight list. ops: "+strings.Join(opNames[:], ", "))
	fs.StringVar(&c.Dist, "dist", "uniform", "wallet selection distribution: uniform or zipf (a few hot wallets)")
	fs.Float64Var(&c.ZipfS, "zipf-s", 1.2, "zipf exponent, must be > 1; larger means hotter hot keys")

	fs.Int64Var(&c.FundAmount, "fund", 100000000, "target opening balance per wallet in minor units; wallets below it are topped up")
	fs.Int64Var(&c.AmountMin, "amount-min", 100, "min operation amount (minor units)")
	fs.Int64Var(&c.AmountMax, "amount-max", 10000, "max operation amount (minor units)")
	fs.Int64Var(&c.FeeUser, "fee-user", 50, "flat user fee configured on each channel (minor units); 0 disables channel fee configuration")
	fs.Int64Var(&c.FeeProc, "fee-processing", 20, "flat processing fee configured on each channel (minor units)")

	fs.StringVar(&c.Prefix, "prefix", "bench", "wallet user id prefix; reuse it to reuse wallets across runs")
	fs.Int64Var(&c.Seed, "seed", 0, "random seed (0 = time based)")
	fs.BoolVar(&c.Idempotency, "idempotency-key", false, "also send the reference as an Idempotency-Key header")
	fs.IntVar(&c.SetupConcurrency, "setup-concurrency", 32, "parallelism for wallet creation, funding and baseline reads")

	fs.Float64Var(&c.MaxUncertainFrac, "max-uncertain-frac", 0.01, "consistency check fails when more than this fraction of the operations (503 OVERLOADED excluded) had an unknown outcome (timeout, 5xx, network); those operations widen the tolerances, so too many of them make the check meaningless. Negative disables the cap")
	fs.BoolVar(&c.Check, "check", true, "run the post-run consistency check")
	fs.IntVar(&c.CheckConcurrency, "check-concurrency", 32, "parallelism for the consistency check reads")

	fs.StringVar(&c.Label, "label", "", "free text scenario label stored in the report, e.g. shards=16,dist=zipf")
	fs.IntVar(&c.Shards, "shards", 0, "CBA_SYSTEM_CONTROL_SHARDS value the server runs with; recorded in the report only")

	fs.StringVar(&c.JSONOut, "json-out", "", "write the JSON report to this file, or - for stdout (the table then goes to stderr)")
	fs.StringVar(&c.CSVOut, "csv-out", "", "write the per-operation CSV to this file, or - for stdout")
	fs.BoolVar(&c.Quiet, "quiet", false, "no progress lines")

	fs.Float64Var(&c.MinTPS, "min-tps", 0, "gate: minimum achieved TPS (2xx responses per second over the measured window)")
	fs.Float64Var(&c.MinWindowTPS, "min-window-tps", 0, "gate: minimum TPS in every 10s window, which proves the rate is sustained")
	fs.DurationVar(&c.MaxP99, "max-p99", 0, "gate: maximum overall p99 latency")
	fs.Float64Var(&c.MaxErrorRate, "max-error-rate", -1, "gate: maximum error rate as a fraction, e.g. 0.001 for 0.1%")
	fs.Float64Var(&c.MaxInsufficientPct, "max-insufficient-rate", 0, "gate: maximum share of expected 402 insufficient-funds responses (protects against a run that is mostly rejections)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	for _, cur := range strings.Split(currencies, ",") {
		cur = strings.ToUpper(strings.TrimSpace(cur))
		if cur != "" {
			c.Currencies = append(c.Currencies, cur)
		}
	}
	mix, err := parseMix(c.MixSpec)
	if err != nil {
		return nil, err
	}
	c.Mix = mix
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	c.URL = strings.TrimRight(strings.TrimSpace(c.URL), "/")
	switch {
	case c.URL == "":
		return errors.New("-url is required")
	case !strings.HasPrefix(c.URL, "http://") && !strings.HasPrefix(c.URL, "https://"):
		return errors.New("-url must start with http:// or https://")
	case c.Ledger == "":
		return errors.New("-ledger is required")
	case c.Duration <= 0:
		return errors.New("-duration must be positive")
	case c.Warmup < 0:
		return errors.New("-warmup must not be negative")
	case c.Timeout <= 0:
		return errors.New("-timeout must be positive")
	case c.Rate < 0:
		return errors.New("-rate must not be negative")
	case c.Arrival != "constant" && c.Arrival != "poisson":
		return errors.New("-arrival must be constant or poisson")
	case c.Concurrency <= 0:
		return errors.New("-concurrency must be positive")
	case c.MaxBacklog <= 0:
		return errors.New("-max-backlog must be positive")
	case c.Wallets <= 0:
		return errors.New("-wallets must be positive")
	case len(c.Currencies) == 0:
		return errors.New("-currencies needs at least one currency")
	case c.Dist != "uniform" && c.Dist != "zipf":
		return errors.New("-dist must be uniform or zipf")
	case c.Dist == "zipf" && c.ZipfS <= 1:
		return errors.New("-zipf-s must be greater than 1")
	case c.AmountMin <= 0 || c.AmountMax < c.AmountMin:
		return errors.New("-amount-min must be positive and <= -amount-max")
	case c.FundAmount < c.AmountMax:
		return errors.New("-fund must be at least -amount-max")
	case c.FeeUser < 0 || c.FeeProc < 0:
		return errors.New("fees must not be negative")
	case c.MaxUncertainFrac > 1:
		return errors.New("-max-uncertain-frac must be at most 1 (or negative to disable)")
	case c.SetupConcurrency <= 0 || c.CheckConcurrency <= 0:
		return errors.New("setup and check concurrency must be positive")
	}
	if c.Wallets < len(c.Currencies) {
		return errors.New("-wallets must be at least the number of currencies")
	}
	if c.mixUsesChannel() {
		if c.Channels <= 0 {
			return errors.New("-channels must be positive when the mix contains channel_debit or lien_pay")
		}
		if c.AmountMin <= c.FeeUser {
			return errors.New("-amount-min must exceed -fee-user when the mix contains channel_debit or lien_pay")
		}
	}
	sort.Strings(c.Currencies)
	for i := 1; i < len(c.Currencies); i++ {
		if c.Currencies[i] == c.Currencies[i-1] {
			return fmt.Errorf("duplicate currency %s", c.Currencies[i])
		}
	}
	if c.Seed == 0 {
		c.Seed = time.Now().UnixNano()
	}
	return nil
}
