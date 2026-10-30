package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"text/tabwriter"
	"time"
)

// LatencyMS is a latency summary in milliseconds.
type LatencyMS struct {
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func summarize(h *Histogram) LatencyMS {
	return LatencyMS{
		P50: ms(h.Quantile(0.50)), P90: ms(h.Quantile(0.90)), P95: ms(h.Quantile(0.95)),
		P99: ms(h.Quantile(0.99)), Max: ms(h.Max()), Mean: ms(h.Mean()),
	}
}

// OpReport is the per-operation (or overall) summary.
type OpReport struct {
	Op         string            `json:"op"`
	Attempts   uint64            `json:"attempts"`
	Classes    map[string]uint64 `json:"classes"`
	Errors     uint64            `json:"errors"`
	ErrorRate  float64           `json:"error_rate"`
	OKTPS      float64           `json:"ok_tps"`
	LedgerTxPS float64           `json:"ledger_tx_per_sec_est"`
	Latency    LatencyMS         `json:"latency_ms"`
	LatencyOK  LatencyMS         `json:"latency_ok_ms"`
	Fallbacks  uint64            `json:"fallback_to_lien,omitempty"`
}

type Gate struct {
	Name   string `json:"name"`
	Limit  string `json:"limit"`
	Actual string `json:"actual"`
	Pass   bool   `json:"pass"`
}

type TimelinePoint struct {
	Second int    `json:"second"`
	OK     uint64 `json:"ok"`
	Errors uint64 `json:"errors"`
}

type RunInfo struct {
	Label       string   `json:"label,omitempty"`
	Shards      int      `json:"shards,omitempty"`
	StartedAt   string   `json:"started_at"`
	URL         string   `json:"url"`
	Ledger      string   `json:"ledger"`
	Mode        string   `json:"mode"`
	TargetTPS   float64  `json:"target_tps,omitempty"`
	Arrival     string   `json:"arrival,omitempty"`
	Concurrency int      `json:"concurrency"`
	Duration    string   `json:"duration"`
	Warmup      string   `json:"warmup"`
	Wallets     int      `json:"wallets"`
	Currencies  []string `json:"currencies"`
	Channels    int      `json:"channels_per_currency"`
	Dist        string   `json:"dist"`
	ZipfS       float64  `json:"zipf_s,omitempty"`
	Mix         string   `json:"mix"`
	Seed        int64    `json:"seed"`
}

type Report struct {
	Run             RunInfo         `json:"run"`
	WindowSeconds   float64         `json:"window_seconds"`
	Total           OpReport        `json:"total"`
	Ops             []OpReport      `json:"ops"`
	Dropped         uint64          `json:"dropped_arrivals"`
	Shed            uint64          `json:"shed"`
	ShedRate        float64         `json:"shed_rate"`
	OfferedTPS      float64         `json:"offered_tps"`
	ProcessedTPS    float64         `json:"processed_tps"`
	InsufficientPct float64         `json:"insufficient_funds_rate"`
	ServiceTime     LatencyMS       `json:"service_time_ms"`
	MinWindow10sTPS float64         `json:"min_10s_window_tps"`
	Timeline        []TimelinePoint `json:"timeline"`
	Gates           []Gate          `json:"gates"`
	// GatesEvaluated is false when no gate flag was given. GatesPassed is then null: "not evaluated" is
	// not a pass, and a script that requires gates_passed == true fails instead of passing silently.
	GatesEvaluated bool         `json:"gates_evaluated"`
	GatesPassed    *bool        `json:"gates_passed"`
	Check          *CheckResult `json:"consistency_check,omitempty"`
}

func buildReport(cfg *Config, rt runTimes, rec *recorder, m *merged, started time.Time) *Report {
	window := rt.effEnd.Sub(rt.winStart).Seconds()
	if window <= 0 {
		window = cfg.Duration.Seconds()
	}
	rep := &Report{WindowSeconds: window, Dropped: rec.dropped.Load()}
	rep.Run = RunInfo{
		Label: cfg.Label, Shards: cfg.Shards, StartedAt: started.UTC().Format(time.RFC3339),
		URL: cfg.URL, Ledger: cfg.Ledger, Concurrency: cfg.Concurrency,
		Duration: cfg.Duration.String(), Warmup: cfg.Warmup.String(),
		Wallets: cfg.Wallets, Currencies: cfg.Currencies, Channels: cfg.Channels,
		Dist: cfg.Dist, Mix: cfg.MixSpec, Seed: cfg.Seed, Mode: "closed-loop",
	}
	if cfg.Dist == "zipf" {
		rep.Run.ZipfS = cfg.ZipfS
	}
	if cfg.openLoop() {
		rep.Run.Mode = "open-loop"
		rep.Run.TargetTPS = cfg.Rate
		rep.Run.Arrival = cfg.Arrival
	}

	var all, allOK Histogram
	total := OpReport{Op: "ALL", Classes: map[string]uint64{}}
	var totalTxs uint64
	for op := Op(0); op < numOps; op++ {
		st := &m.ops[op]
		var attempts uint64
		for _, c := range st.classes {
			attempts += c
		}
		if attempts == 0 {
			continue
		}
		r := OpReport{
			Op: op.String(), Attempts: attempts, Classes: map[string]uint64{},
			Latency: summarize(&st.all), LatencyOK: summarize(&st.ok), Fallbacks: st.fallbacks,
		}
		for c := Class(0); c < numClasses; c++ {
			if st.classes[c] > 0 {
				r.Classes[c.String()] = st.classes[c]
				total.Classes[c.String()] += st.classes[c]
			}
			if isError(op, c) {
				r.Errors += st.classes[c]
			}
		}
		r.ErrorRate = float64(r.Errors) / float64(attempts)
		r.OKTPS = float64(st.classes[ClassOK]) / window
		r.LedgerTxPS = float64(st.ledgerTxs) / window
		rep.Ops = append(rep.Ops, r)

		total.Attempts += attempts
		total.Errors += r.Errors
		total.Fallbacks += st.fallbacks
		totalTxs += st.ledgerTxs
		all.Merge(&st.all)
		allOK.Merge(&st.ok)
	}
	// Dropped arrivals never reached the server: they count as errors.
	if rep.Dropped > 0 {
		total.Classes[ClassDropped.String()] = rep.Dropped
		total.Errors += rep.Dropped
	}
	offered := total.Attempts + rep.Dropped
	if offered > 0 {
		total.ErrorRate = float64(total.Errors) / float64(offered)
	}
	okCount := total.Classes[ClassOK.String()]
	total.OKTPS = float64(okCount) / window
	total.LedgerTxPS = float64(totalTxs) / window
	total.Latency = summarize(&all)
	total.LatencyOK = summarize(&allOK)
	rep.Total = total
	rep.OfferedTPS = float64(offered) / window
	rep.ProcessedTPS = float64(okCount+total.Classes[ClassInsufficient.String()]) / window
	if total.Attempts > 0 {
		rep.InsufficientPct = float64(total.Classes[ClassInsufficient.String()]) / float64(total.Attempts)
	}
	rep.ServiceTime = summarize(&m.svc)

	// Timeline by completion second inside the window, and the slowest 10s span.
	nsec := int(window)
	if nsec > len(m.tlOK) {
		nsec = len(m.tlOK)
	}
	for i := 0; i < nsec; i++ {
		rep.Timeline = append(rep.Timeline, TimelinePoint{Second: i, OK: m.tlOK[i], Errors: m.tlErr[i]})
	}
	rep.MinWindow10sTPS = minWindowTPS(m.tlOK[:nsec], 10)

	rep.Shed = total.Classes[ClassShed.String()]
	if offered > 0 {
		rep.ShedRate = float64(rep.Shed) / float64(offered)
	}
	rep.Gates, rep.GatesEvaluated, rep.GatesPassed = evaluateGates(cfg, rep)
	return rep
}

// minWindowTPS returns the lowest average per-second rate over any run of w
// consecutive seconds. For runs shorter than w it is the overall average.
func minWindowTPS(perSec []uint64, w int) float64 {
	n := len(perSec)
	if n == 0 {
		return 0
	}
	if n < w {
		w = n
	}
	var sum uint64
	for i := 0; i < w; i++ {
		sum += perSec[i]
	}
	best := sum
	for i := w; i < n; i++ {
		sum += perSec[i] - perSec[i-w]
		if sum < best {
			best = sum
		}
	}
	return float64(best) / float64(w)
}

// evaluateGates returns the gates, whether any was evaluated, and the verdict (nil when none was).
func evaluateGates(cfg *Config, rep *Report) ([]Gate, bool, *bool) {
	var gates []Gate
	add := func(name, limit, actual string, pass bool) {
		gates = append(gates, Gate{Name: name, Limit: limit, Actual: actual, Pass: pass})
	}
	if cfg.MinTPS > 0 {
		add("min-tps", fmt.Sprintf(">= %.1f", cfg.MinTPS), fmt.Sprintf("%.1f", rep.Total.OKTPS), rep.Total.OKTPS >= cfg.MinTPS)
	}
	if cfg.MinWindowTPS > 0 {
		add("min-window-tps", fmt.Sprintf(">= %.1f", cfg.MinWindowTPS), fmt.Sprintf("%.1f", rep.MinWindow10sTPS), rep.MinWindow10sTPS >= cfg.MinWindowTPS)
	}
	if cfg.MaxP99 > 0 {
		add("max-p99", "<= "+cfg.MaxP99.String(), fmt.Sprintf("%.2fms", rep.Total.Latency.P99), rep.Total.Latency.P99 <= ms(cfg.MaxP99))
	}
	if cfg.MaxErrorRate >= 0 {
		add("max-error-rate", fmt.Sprintf("<= %.4f%%", cfg.MaxErrorRate*100), fmt.Sprintf("%.4f%%", rep.Total.ErrorRate*100), rep.Total.ErrorRate <= cfg.MaxErrorRate)
	}
	if cfg.MaxInsufficientPct > 0 {
		add("max-insufficient-rate", fmt.Sprintf("<= %.2f%%", cfg.MaxInsufficientPct*100), fmt.Sprintf("%.2f%%", rep.InsufficientPct*100), rep.InsufficientPct <= cfg.MaxInsufficientPct)
	}
	if len(gates) == 0 {
		return gates, false, nil
	}
	ok := true
	for _, g := range gates {
		ok = ok && g.Pass
	}
	return gates, true, &ok
}

// gatesFailed reports an evaluated gate that did not pass.
func (r *Report) gatesFailed() bool {
	return r.GatesEvaluated && r.GatesPassed != nil && !*r.GatesPassed
}

func fmtMS(v float64) string {
	switch {
	case v >= 100:
		return strconv.FormatFloat(v, 'f', 0, 64)
	case v >= 10:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return strconv.FormatFloat(v, 'f', 2, 64)
	}
}

func (r *Report) writeTable(w io.Writer) {
	run := r.Run
	fmt.Fprintf(w, "\nLedgerTrack bench  %s\n", run.StartedAt)
	if run.Label != "" {
		fmt.Fprintf(w, "scenario : %s\n", run.Label)
	}
	target := "max throughput (closed loop)"
	if run.Mode == "open-loop" {
		target = fmt.Sprintf("%.0f TPS target, %s arrivals (open loop)", run.TargetTPS, run.Arrival)
	}
	fmt.Fprintf(w, "target   : %s, %d workers\n", target, run.Concurrency)
	fmt.Fprintf(w, "workload : %d wallets, %v, dist=%s, mix=%s\n", run.Wallets, run.Currencies, run.Dist, run.Mix)
	fmt.Fprintf(w, "window   : %.1fs measured after %s warmup\n\n", r.WindowSeconds, run.Warmup)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "op\tattempts\t2xx\t402\t409\t4xx\t5xx\ttimeout\tnet\tshed\terr%\tok/s\tp50ms\tp90ms\tp95ms\tp99ms\tmaxms\t")
	row := func(o OpReport) {
		c := o.Classes
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.3f\t%.1f\t%s\t%s\t%s\t%s\t%s\t\n",
			o.Op, o.Attempts, c["2xx"], c["402_insufficient"], c["409_conflict"], c["4xx_other"], c["5xx"], c["timeout"], c["network"], c["shed"],
			o.ErrorRate*100, o.OKTPS, fmtMS(o.Latency.P50), fmtMS(o.Latency.P90), fmtMS(o.Latency.P95), fmtMS(o.Latency.P99), fmtMS(o.Latency.Max))
	}
	ops := append([]OpReport(nil), r.Ops...)
	sort.Slice(ops, func(i, j int) bool { return ops[i].Op < ops[j].Op })
	for _, o := range ops {
		row(o)
	}
	row(r.Total)
	tw.Flush()

	fmt.Fprintf(w, "\nachieved TPS (2xx)        : %.1f   (processed incl. 402: %.1f, offered: %.1f)\n", r.Total.OKTPS, r.ProcessedTPS, r.OfferedTPS)
	fmt.Fprintf(w, "est. ledger tx/s          : %.1f   (channel ops write 2-4 ledger transactions)\n", r.Total.LedgerTxPS)
	fmt.Fprintf(w, "slowest 10s window        : %.1f TPS\n", r.MinWindow10sTPS)
	fmt.Fprintf(w, "error rate                : %.4f%%   insufficient funds (expected): %.2f%%   dropped arrivals: %d\n", r.Total.ErrorRate*100, r.InsufficientPct*100, r.Dropped)
	fmt.Fprintf(w, "shed (503 OVERLOADED)     : %d (%.2f%% of offered), not counted as errors\n", r.Shed, r.ShedRate*100)
	fmt.Fprintf(w, "latency ms (from intended): p50 %s  p90 %s  p95 %s  p99 %s  max %s\n",
		fmtMS(r.Total.Latency.P50), fmtMS(r.Total.Latency.P90), fmtMS(r.Total.Latency.P95), fmtMS(r.Total.Latency.P99), fmtMS(r.Total.Latency.Max))
	fmt.Fprintf(w, "service time ms (on wire) : p50 %s  p99 %s  max %s\n", fmtMS(r.ServiceTime.P50), fmtMS(r.ServiceTime.P99), fmtMS(r.ServiceTime.Max))
	if r.Total.Fallbacks > 0 {
		fmt.Fprintf(w, "note: %d lien_release/lien_pay draws ran as lien because no lien was on hold\n", r.Total.Fallbacks)
	}

	if !r.GatesEvaluated {
		fmt.Fprintln(w, "\nSLO gates: not evaluated (no -min-tps, -min-window-tps, -max-p99, -max-error-rate or -max-insufficient-rate given)")
	} else {
		fmt.Fprintln(w, "\nSLO gates")
		for _, g := range r.Gates {
			status := "PASS"
			if !g.Pass {
				status = "FAIL"
			}
			fmt.Fprintf(w, "  %-22s %-14s actual %-14s %s\n", g.Name, g.Limit, g.Actual, status)
		}
	}

	if c := r.Check; c != nil {
		fmt.Fprintln(w, "\nConsistency check")
		if !c.Ran {
			fmt.Fprintln(w, "  skipped")
			return
		}
		verdict := "PASS"
		if !c.Passed {
			verdict = "FAIL"
		}
		fmt.Fprintf(w, "  result: %s   wallets checked %d, mismatches %d, negative balances %d, channels checked %d (mismatches %d), unexpected 409s %d, uncertain ops %d (%.2f%%), shed %d\n",
			verdict, c.WalletsChecked, c.WalletMismatches, c.NegativeBalances, c.ChannelsChecked, c.ChannelMismatches, c.Conflicts409, c.UncertainOps, c.UncertainFrac*100, c.ShedOps)
		for _, cc := range c.Currencies {
			fmt.Fprintf(w, "  %s: control delta %d (expected %d +-%d), world delta %d (expected %d +-%d), conservation %d",
				cc.Currency, cc.ControlDelta, cc.ControlExpected, cc.ControlTolerance, cc.WorldDelta, cc.WorldExpected, cc.WorldTolerance, cc.Conservation)
			if cc.RevenueChecked {
				fmt.Fprintf(w, ", revenue acc %d/%d proc %d/%d", cc.RevenueAccDelta, cc.RevenueAccExpected, cc.RevenueProcDelta, cc.RevenueProcExpected)
			}
			fmt.Fprintln(w)
		}
		for _, f := range c.Failures {
			fmt.Fprintf(w, "  - %s\n", f)
		}
		for _, n := range c.Notes {
			fmt.Fprintf(w, "  note: %s\n", n)
		}
	}
}

func (r *Report) writeJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// writeCSV writes one row per operation plus an ALL row.
func (r *Report) writeCSV(w io.Writer) error {
	cw := csv.NewWriter(w)
	header := []string{"label", "mode", "target_tps", "shards", "dist", "op", "attempts", "2xx", "402_insufficient", "409_conflict",
		"4xx_other", "5xx", "timeout", "network", "dropped", "shed", "error_rate", "ok_tps", "ledger_tx_per_sec_est",
		"p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms", "ok_p50_ms", "ok_p99_ms"}
	if err := cw.Write(header); err != nil {
		return err
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
	u := func(v uint64) string { return strconv.FormatUint(v, 10) }
	for _, o := range append(append([]OpReport(nil), r.Ops...), r.Total) {
		c := o.Classes
		rec := []string{r.Run.Label, r.Run.Mode, f(r.Run.TargetTPS), strconv.Itoa(r.Run.Shards), r.Run.Dist, o.Op, u(o.Attempts),
			u(c["2xx"]), u(c["402_insufficient"]), u(c["409_conflict"]), u(c["4xx_other"]), u(c["5xx"]), u(c["timeout"]), u(c["network"]), u(c["dropped"]), u(c["shed"]),
			f(o.ErrorRate), f(o.OKTPS), f(o.LedgerTxPS),
			f(o.Latency.P50), f(o.Latency.P90), f(o.Latency.P95), f(o.Latency.P99), f(o.Latency.Max), f(o.Latency.Mean),
			f(o.LatencyOK.P50), f(o.LatencyOK.P99)}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
