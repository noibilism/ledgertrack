package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestHistogramQuantiles(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1))
	var h Histogram
	vals := make([]time.Duration, 0, 20000)
	for i := 0; i < 20000; i++ {
		// log-uniform between 100us and 2s
		d := time.Duration(100*float64(time.Microsecond)) * time.Duration(1+rng.Intn(20000))
		vals = append(vals, d)
		h.Record(d)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		exact := vals[int(q*float64(len(vals)))]
		got := h.Quantile(q)
		diff := float64(got-exact) / float64(exact)
		if diff < -0.02 || diff > 0.02 {
			t.Fatalf("q=%v exact=%v got=%v (%.2f%% off)", q, exact, got, diff*100)
		}
	}
	if h.Max() != vals[len(vals)-1] {
		t.Fatalf("max = %v, want %v", h.Max(), vals[len(vals)-1])
	}
	var other Histogram
	other.Record(10 * time.Second)
	h.Merge(&other)
	if h.Max() != 10*time.Second || h.Count() != 20001 {
		t.Fatalf("merge broken: max=%v count=%d", h.Max(), h.Count())
	}
	if (&Histogram{}).Quantile(0.99) != 0 {
		t.Fatal("empty histogram must return 0")
	}
}

func TestHistogramIndexBounds(t *testing.T) {
	t.Parallel()
	for _, us := range []int64{0, 1, 63, 64, 127, 128, 129, 1000, 1 << 20, 1 << 39, 1 << 40, 1 << 62} {
		i := histIndex(us)
		if i < 0 || i >= histBuckets {
			t.Fatalf("index %d out of range for %d", i, us)
		}
		if us < 1<<40 && histUpper(i) < us {
			t.Fatalf("upper bound %d below value %d", histUpper(i), us)
		}
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		status int
		body   string
		err    error
		want   Class
	}{
		{"created", 201, "", nil, ClassOK},
		{"ok", 200, "", nil, ClassOK},
		{"402", 402, "", nil, ClassInsufficient},
		{"lien insufficient is a 400", 400, `{"errorCode":"INSUFFICIENT_FUND"}`, nil, ClassInsufficient},
		{"plain 400", 400, `{"errorCode":"VALIDATION"}`, nil, Class4xx},
		{"409", 409, "", nil, ClassConflict},
		{"404", 404, "", nil, Class4xx},
		{"500", 500, "", nil, Class5xx},
		{"503 serialization", 503, "", nil, Class5xx},
		{"deadline", 0, "", context.DeadlineExceeded, ClassTimeout},
		{"net timeout", 0, "", &net.DNSError{IsTimeout: true}, ClassTimeout},
		{"refused", 0, "", errors.New("connection refused"), ClassNetwork},
	}
	for _, tc := range cases {
		if got := classify(tc.status, []byte(tc.body), tc.err); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
	if isError(OpDebit, ClassInsufficient) || isError(OpChannelDebit, ClassInsufficient) || isError(OpLien, ClassInsufficient) {
		t.Error("402 must be expected for spending ops")
	}
	if !isError(OpCredit, ClassInsufficient) || !isError(OpLienRelease, ClassInsufficient) {
		t.Error("402 must be an error for credit and release")
	}
	if !isError(OpDebit, ClassConflict) || !isError(OpDebit, ClassTimeout) || isError(OpDebit, ClassOK) {
		t.Error("error accounting wrong")
	}
}

func TestParseMix(t *testing.T) {
	t.Parallel()
	mix, err := parseMix("credit=30, debit=40,lien-release=10,release=0,channel=20")
	if err == nil {
		t.Fatalf("release and lien-release are the same op, expected duplicate error, got %v", mix)
	}
	mix, err = parseMix("credit=30,debit=40,lien-release=10,channel=20,pay=0")
	if err != nil {
		t.Fatal(err)
	}
	if len(mix) != 4 {
		t.Fatalf("zero weights must be dropped: %+v", mix)
	}
	for _, bad := range []string{"", "credit", "credit=x", "bogus=1", "credit=-1", "credit=0"} {
		if _, err := parseMix(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
	ch := newChooser(mix)
	rng := rand.New(rand.NewSource(3))
	counts := map[Op]int{}
	for i := 0; i < 100000; i++ {
		counts[ch.pick(rng)]++
	}
	for _, m := range mix {
		got := float64(counts[m.Op]) / 100000
		want := m.Weight / 100
		if got < want-0.01 || got > want+0.01 {
			t.Errorf("%s frequency %.3f, want about %.3f", m.Op, got, want)
		}
	}
}

func TestParseConfigValidation(t *testing.T) {
	t.Parallel()
	var sink bytes.Buffer
	if _, err := parseConfig([]string{"-dist", "zipf", "-zipf-s", "1"}, &sink); err == nil {
		t.Error("zipf-s <= 1 must be rejected")
	}
	if _, err := parseConfig([]string{"-mix", "channel_debit=1", "-channels", "0"}, &sink); err == nil {
		t.Error("channel ops without channels must be rejected")
	}
	if _, err := parseConfig([]string{"-mix", "channel_debit=1", "-amount-min", "10", "-fee-user", "50"}, &sink); err == nil {
		t.Error("amount-min below the fee must be rejected")
	}
	if _, err := parseConfig([]string{"-currencies", "USD,usd"}, &sink); err == nil {
		t.Error("duplicate currencies must be rejected")
	}
	c, err := parseConfig([]string{"-rate", "1200", "-currencies", "ngn, usd", "-duration", "10m"}, &sink)
	if err != nil {
		t.Fatal(err)
	}
	if !c.openLoop() || c.Currencies[0] != "NGN" || c.Currencies[1] != "USD" || c.Duration != 10*time.Minute || c.Seed == 0 {
		t.Fatalf("unexpected config %+v", c)
	}
	if (&Config{}).openLoop() {
		t.Error("rate 0 must be closed loop")
	}
}

func TestZipfIsSkewedAndUniformIsFlat(t *testing.T) {
	t.Parallel()
	const n, draws = 1000, 200000
	count := func(dist string) []int {
		cfg := &Config{Wallets: n, Dist: dist, ZipfS: 1.2}
		kp := newKeyPicker(cfg, rand.New(rand.NewSource(7)))
		c := make([]int, n)
		for i := 0; i < draws; i++ {
			c[kp.pick()]++
		}
		return c
	}
	z, u := count("zipf"), count("uniform")
	if float64(z[0])/draws < 0.08 {
		t.Errorf("zipf rank 0 share %.3f, want a clearly hot key", float64(z[0])/draws)
	}
	top10 := 0
	for i := 0; i < 10; i++ {
		top10 += z[i]
	}
	if float64(top10)/draws < 0.25 {
		t.Errorf("zipf top-10 share %.3f too low", float64(top10)/draws)
	}
	for i, c := range u {
		if c < draws/n/3 || c > draws/n*3 {
			t.Fatalf("uniform wallet %d got %d draws, expected about %d", i, c, draws/n)
		}
	}
}

func TestRefGenUnique(t *testing.T) {
	t.Parallel()
	g := refGen{runID: "abc"}
	seen := map[string]bool{}
	for op := Op(0); op < numOps; op++ {
		for seq := uint64(1); seq <= 5000; seq++ {
			r := g.ref(op, seq)
			if seen[r] {
				t.Fatalf("duplicate reference %s", r)
			}
			seen[r] = true
		}
	}
}

func TestBookCommitAndUncertain(t *testing.T) {
	t.Parallel()
	b := newBook(2, 1, 2)
	b.commit(effect{op: OpCredit, wallet: 0, total: 1000, channel: -1})
	b.commit(effect{op: OpDebit, wallet: 0, total: 300, channel: -1})
	b.commit(effect{op: OpLien, wallet: 0, total: 200, channel: -1})
	b.commit(effect{op: OpChannelDebit, wallet: 1, channel: 1, total: 150, principal: 100, userFee: 50, procFee: 20})
	b.commit(effect{op: OpLienPay, wallet: 0, channel: 0, total: 120, principal: 100, userFee: 20})
	b.commit(effect{op: OpLienRelease, wallet: 0, total: 80, channel: -1})
	if got := b.wAvail[0].Load(); got != 1000-300-200+80 {
		t.Errorf("wallet 0 available %d", got)
	}
	if got := b.wLien[0].Load(); got != 200-120-80 {
		t.Errorf("wallet 0 lien %d", got)
	}
	if got := b.wAvail[1].Load(); got != -150 {
		t.Errorf("wallet 1 available %d", got)
	}
	c := &b.ccy[0]
	if c.control.Load() != -1000+300+150 || c.world.Load() != 120 || c.revAcc.Load() != 70 || c.revProc.Load() != 20 {
		t.Errorf("currency book wrong: control=%d world=%d revAcc=%d revProc=%d", c.control.Load(), c.world.Load(), c.revAcc.Load(), c.revProc.Load())
	}
	if b.chanDelta[1].Load() != -100 || b.chanDelta[0].Load() != -100 {
		t.Errorf("channel deltas wrong")
	}
	b.uncertain(effect{op: OpChannelDebit, wallet: 1, channel: 1, total: 150, principal: 100, userFee: 50, procFee: 20})
	if b.uncertainOps.Load() != 1 || b.wUncAvail[1].Load() != 150 || b.chanUnc[1].Load() != 100 || c.uncControl.Load() != 150 {
		t.Error("uncertain bookkeeping wrong")
	}
	var p lienPool
	if _, ok := p.pop(); ok {
		t.Fatal("empty pool")
	}
	p.push(lienItem{wallet: 1, amount: 5})
	if it, ok := p.pop(); !ok || it.amount != 5 {
		t.Fatal("pool pop")
	}
}

func TestMinWindowTPS(t *testing.T) {
	t.Parallel()
	per := make([]uint64, 30)
	for i := range per {
		per[i] = 100
	}
	for i := 12; i < 22; i++ {
		per[i] = 40
	}
	if got := minWindowTPS(per, 10); got != 40 {
		t.Errorf("min window %v want 40", got)
	}
	if got := minWindowTPS(per[:5], 10); got != 100 {
		t.Errorf("short run %v want 100", got)
	}
	if minWindowTPS(nil, 10) != 0 {
		t.Error("empty")
	}
}

func TestGates(t *testing.T) {
	t.Parallel()
	rep := &Report{}
	rep.Total.OKTPS = 1250
	rep.Total.Latency.P99 = 180
	rep.Total.ErrorRate = 0.0004
	rep.MinWindow10sTPS = 1190
	cfg := &Config{MinTPS: 1200, MaxP99: 250 * time.Millisecond, MaxErrorRate: 0.001, MinWindowTPS: 1200}
	gates, evaluated, ok := evaluateGates(cfg, rep)
	if !evaluated || ok == nil || *ok || len(gates) != 4 {
		t.Fatalf("expected 4 gates with one failure, got %d evaluated=%v ok=%v", len(gates), evaluated, ok)
	}
	for _, g := range gates {
		if g.Pass == (g.Name == "min-window-tps") {
			t.Errorf("gate %s pass=%v unexpected", g.Name, g.Pass)
		}
	}
	cfg = &Config{MaxErrorRate: -1}
	// No gate flags: nothing was evaluated, which must not be reported as a pass.
	if g, evaluated, ok := evaluateGates(cfg, rep); evaluated || ok != nil || len(g) != 0 {
		t.Errorf("no configured gates must be reported as not evaluated, got evaluated=%v ok=%v", evaluated, ok)
	}
}

// runBench runs the real entry point against the fake server.
func runBench(t *testing.T, args ...string) (code int, rep *Report, out string) {
	t.Helper()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "r.json")
	csvPath := filepath.Join(dir, "r.csv")
	var stdout, stderr bytes.Buffer
	full := append([]string{"-quiet", "-json-out", jsonPath, "-csv-out", csvPath}, args...)
	code = realMain(full, &stdout, &stderr)
	data, err := os.ReadFile(jsonPath) //nolint:gosec // path comes from the operator/CLI, not from request input
	if err == nil {
		rep = &Report{}
		if err := json.Unmarshal(data, rep); err != nil {
			t.Fatalf("bad JSON report: %v", err)
		}
		csvData, err := os.ReadFile(csvPath) //nolint:gosec // path comes from the operator/CLI, not from request input
		if err != nil || !strings.HasPrefix(string(csvData), "label,mode,") || !strings.Contains(string(csvData), ",ALL,") {
			t.Fatalf("bad CSV report: %v %q", err, csvData)
		}
	}
	return code, rep, stdout.String() + stderr.String()
}

var smallRun = []string{
	"-wallets", "24", "-currencies", "USD,NGN", "-channels", "2", "-concurrency", "8",
	"-mix", "credit=30,debit=35,lien=12,lien_release=8,lien_pay=5,channel_debit=10",
	"-fund", "100000", "-amount-min", "100", "-amount-max", "900", "-setup-concurrency", "4",
}

func TestEndToEndOpenLoopConsistent(t *testing.T) {
	f := newFakeServer(t)
	args := append([]string{"-url", f.url(), "-rate", "120", "-duration", "1s", "-warmup", "0s", "-dist", "zipf", "-label", "t=1", "-shards", "4",
		"-min-tps", "50", "-max-error-rate", "0.001", "-max-p99", "2s"}, smallRun...)
	code, rep, out := runBench(t, args...)
	t.Log(out)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if rep.Run.Mode != "open-loop" || rep.Run.Label != "t=1" || rep.Run.Shards != 4 {
		t.Errorf("run info wrong: %+v", rep.Run)
	}
	if rep.Total.OKTPS < 50 || rep.Total.Attempts < 80 {
		t.Errorf("too little traffic: %+v", rep.Total)
	}
	if rep.Check == nil || !rep.Check.Ran || !rep.Check.Passed || rep.Check.WalletsChecked != 24 {
		t.Fatalf("check should pass: %+v", rep.Check)
	}
	if len(rep.Ops) < 4 || len(rep.Gates) != 3 || !rep.GatesEvaluated || rep.GatesPassed == nil || !*rep.GatesPassed {
		t.Errorf("ops=%d gates=%d passed=%v", len(rep.Ops), len(rep.Gates), rep.GatesPassed)
	}
	for _, cc := range rep.Check.Currencies {
		if cc.Conservation != 0 {
			t.Errorf("%s conservation %d", cc.Currency, cc.Conservation)
		}
	}
	if rep.Total.Latency.P99 <= 0 || rep.Total.Latency.P50 > rep.Total.Latency.P99 {
		t.Errorf("latency summary wrong: %+v", rep.Total.Latency)
	}
	if !strings.Contains(out, "Consistency check") || !strings.Contains(out, "achieved TPS") {
		t.Errorf("table output missing sections:\n%s", out)
	}
}

func TestEndToEndRerunReusesWalletsIdempotently(t *testing.T) {
	f := newFakeServer(t)
	args := append([]string{"-url", f.url(), "-rate", "60", "-duration", "500ms", "-warmup", "0s"}, smallRun...)
	for i := 0; i < 2; i++ {
		code, rep, out := runBench(t, args...)
		if code != exitOK || !rep.Check.Passed {
			t.Fatalf("run %d: exit %d\n%s", i, code, out)
		}
	}
	// funding must have used a fixed reference for the never-funded wallets
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.refs["ledgertrack|bench-fund-bench-w000000-NGN"] {
		t.Error("expected deterministic funding reference to be recorded")
	}
}

func TestEndToEndClosedLoop(t *testing.T) {
	f := newFakeServer(t)
	args := append([]string{"-url", f.url(), "-duration", "500ms", "-warmup", "100ms", "-concurrency", "4"}, smallRun[2:]...)
	args = append(args, "-wallets", "12", "-currencies", "USD")
	code, rep, out := runBench(t, args...)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if rep.Run.Mode != "closed-loop" || rep.Total.Attempts == 0 || !rep.Check.Passed {
		t.Fatalf("closed loop run wrong: %+v", rep.Total)
	}
}

func TestSLOGateFailureExitCode(t *testing.T) {
	f := newFakeServer(t)
	args := append([]string{"-url", f.url(), "-rate", "40", "-duration", "500ms", "-warmup", "0s", "-min-tps", "100000"}, smallRun...)
	code, rep, out := runBench(t, args...)
	if code != exitGate || !rep.GatesEvaluated || rep.GatesPassed == nil || *rep.GatesPassed || !rep.Check.Passed {
		t.Fatalf("want exit %d with consistent data, got %d\n%s", exitGate, code, out)
	}
	if !strings.Contains(out, "FAIL") {
		t.Error("table must show the failing gate")
	}
}

func TestConsistencyCheckCatchesLostWrites(t *testing.T) {
	f := newFakeServer(t)
	f.dropEveryNthCredit = 4 // acknowledged credits that never hit the ledger
	args := append([]string{"-url", f.url(), "-rate", "80", "-duration", "1s", "-warmup", "0s"}, smallRun...)
	code, rep, out := runBench(t, args...)
	if code != exitConsistency || rep.Check.Passed || rep.Check.WalletMismatches == 0 {
		t.Fatalf("lost writes must fail the check: exit %d\n%s", code, out)
	}
}

func TestUncertainOutcomesWidenTolerance(t *testing.T) {
	f := newFakeServer(t)
	f.failEveryNthDebit = 5 // 500 without applying: outcome treated as unknown
	// about 7% of the operations: tolerated only because the threshold is raised explicitly
	args := append([]string{"-url", f.url(), "-rate", "80", "-duration", "1s", "-warmup", "0s", "-max-uncertain-frac", "0.25"}, smallRun...)
	code, rep, out := runBench(t, args...)
	if code != exitOK || !rep.Check.Passed || rep.Check.UncertainOps == 0 {
		t.Fatalf("5xx must be tolerated as unknown outcomes, exit %d\n%s", code, out)
	}
	if rep.Total.Classes["5xx"] == 0 || rep.Total.ErrorRate == 0 {
		t.Errorf("5xx must still count as errors: %+v", rep.Total)
	}
}

func TestSetupFailureExitCode(t *testing.T) {
	code, _, _ := runBench(t, "-url", "http://127.0.0.1:1/v2", "-duration", "100ms", "-warmup", "0s", "-wallets", "2", "-timeout", "500ms")
	if code != exitSetup {
		t.Fatalf("want exit %d, got %d", exitSetup, code)
	}
}

func TestOpenLoopSchedulerKeepsRateWithSlowServer(t *testing.T) {
	// A server that answers slowly must not reduce the offered load: arrivals
	// stay on schedule and the wait shows up as latency (no coordinated omission).
	f := newFakeServer(t)
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/credit") || strings.Contains(r.URL.Path, "/debit") {
			time.Sleep(150 * time.Millisecond)
		}
		f.srv.Config.Handler.ServeHTTP(w, r)
	})
	front := newFrontServer(t, slow)
	args := append([]string{"-url", front + "/v2", "-rate", "100", "-duration", "1s", "-warmup", "0s", "-concurrency", "4", "-check=false",
		"-mix", "credit=1,debit=1", "-wallets", "8", "-currencies", "USD", "-fund", "1000000"}, "-setup-concurrency", "4")
	code, rep, out := runBench(t, args...)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, out)
	}
	offered := rep.Total.Attempts + rep.Dropped
	if offered < 80 {
		t.Fatalf("offered load collapsed to %d arrivals in 1s at 100/s with a slow server", offered)
	}
	// 4 workers x 150ms service time can only complete ~27/s, so queueing delay
	// must appear in the latency percentiles rather than being hidden.
	if rep.Total.Latency.P99 < 300 {
		t.Errorf("p99 %.1fms does not reflect queueing delay", rep.Total.Latency.P99)
	}
	if rep.ServiceTime.P50 > 400 {
		t.Errorf("service time p50 %.1fms unexpectedly high", rep.ServiceTime.P50)
	}
}

func newFrontServer(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// (a) too many unknown outcomes must fail the check instead of widening the tolerance until it passes.
func TestTooManyUncertainOpsFailTheCheck(t *testing.T) {
	f := newFakeServer(t)
	f.failEveryNthDebit = 5                                                                                   // roughly 7% of the operations get a 500 without applying
	args := append([]string{"-url", f.url(), "-rate", "80", "-duration", "1s", "-warmup", "0s"}, smallRun...) // default threshold: 1%
	code, rep, out := runBench(t, args...)
	if code != exitConsistency {
		t.Fatalf("want exit %d (consistency), got %d\n%s", exitConsistency, code, out)
	}
	c := rep.Check
	if c.Passed || !c.UncertainExceeded || c.UncertainOps == 0 || c.MaxUncertainFrac != 0.01 || c.UncertainFrac <= 0.01 {
		t.Fatalf("check must fail on uncertain ops: %+v", c)
	}
	if !strings.Contains(strings.Join(c.Failures, "\n"), "INCONCLUSIVE") || !strings.Contains(out, "-max-uncertain-frac") {
		t.Errorf("failure must say why:\n%s", out)
	}
	// The balances themselves are fine: the failure is about the evidence, not about a mismatch.
	if c.WalletMismatches != 0 {
		t.Errorf("unexpected wallet mismatches: %d", c.WalletMismatches)
	}
}

func TestUncertainFractionThresholds(t *testing.T) {
	t.Parallel()
	res := &CheckResult{UncertainOps: 1, SentOps: 100, UncertainFrac: 0.01}
	if uncertainVerdict(res, 0.01) != "" {
		t.Error("exactly at the threshold is allowed")
	}
	res = &CheckResult{UncertainOps: 2, SentOps: 100, UncertainFrac: 0.02}
	if uncertainVerdict(res, 0.01) == "" {
		t.Error("above the threshold must fail")
	}
	if uncertainVerdict(res, -1) != "" {
		t.Error("a negative threshold disables the cap")
	}
	if uncertainVerdict(&CheckResult{}, 0) != "" {
		t.Error("no uncertain ops can never fail, even with a zero threshold")
	}
	if uncertainVerdict(&CheckResult{UncertainOps: 3}, 0.5) == "" {
		t.Error("uncertain ops without any considered op must fail")
	}
}

// (b) no gate flags: gates are reported as not evaluated, never as passed.
func TestNoGateFlagsReportsNotEvaluated(t *testing.T) {
	f := newFakeServer(t)
	args := append([]string{"-url", f.url(), "-rate", "40", "-duration", "500ms", "-warmup", "0s"}, smallRun...)
	code, rep, out := runBench(t, args...)
	if code != exitOK {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if rep.GatesEvaluated || rep.GatesPassed != nil || len(rep.Gates) != 0 {
		t.Errorf("gates must be not evaluated: evaluated=%v passed=%v", rep.GatesEvaluated, rep.GatesPassed)
	}
	if !strings.Contains(out, "not evaluated") {
		t.Errorf("table must say gates were not evaluated:\n%s", out)
	}
	raw, _ := json.Marshal(rep)
	if !strings.Contains(string(raw), `"gates_passed":null`) || !strings.Contains(string(raw), `"gates_evaluated":false`) {
		t.Errorf("JSON must carry gates_passed:null: %s", raw)
	}
}

// (c) 503 OVERLOADED is shed load: separate from errors, not uncertain.
func TestClassifyShed(t *testing.T) {
	t.Parallel()
	overloaded := []byte(`{"errorCode":"OVERLOADED","errorMessage":"write queue full"}`)
	if c := classify(503, overloaded, nil); c != ClassShed {
		t.Errorf("503 OVERLOADED = %v, want shed", c)
	}
	if c := classify(503, []byte(`{"errorCode":"INTERNAL"}`), nil); c != Class5xx {
		t.Errorf("plain 503 = %v, want 5xx", c)
	}
	if c := classify(500, overloaded, nil); c != Class5xx {
		t.Errorf("500 = %v, want 5xx", c)
	}
	if isError(OpDebit, ClassShed) || !definitelyNotCommitted(ClassShed) {
		t.Error("shed is neither an error nor an uncertain outcome")
	}
}

func TestShedLoadIsReportedSeparatelyAndNotUncertain(t *testing.T) {
	f := newFakeServer(t)
	f.shedEveryNthDebit = 3 // 503 OVERLOADED before anything is applied
	args := append([]string{"-url", f.url(), "-rate", "80", "-duration", "1s", "-warmup", "0s", "-max-error-rate", "0.001"}, smallRun...)
	code, rep, out := runBench(t, args...)
	if code != exitOK {
		t.Fatalf("shed load must neither fail the check nor the gates: exit %d\n%s", code, out)
	}
	if rep.Shed == 0 || rep.Total.Classes["shed"] != rep.Shed || rep.ShedRate <= 0 {
		t.Fatalf("shed not reported: shed=%d rate=%v classes=%v", rep.Shed, rep.ShedRate, rep.Total.Classes)
	}
	if rep.Total.Errors != 0 || rep.Total.Classes["5xx"] != 0 {
		t.Errorf("shed must not count as errors: errors=%d classes=%v", rep.Total.Errors, rep.Total.Classes)
	}
	if rep.Check.UncertainOps != 0 || rep.Check.ShedOps != rep.Shed || !rep.Check.Passed {
		t.Errorf("shed must not be uncertain: %+v", rep.Check)
	}
	if !strings.Contains(out, "shed (503 OVERLOADED)") {
		t.Errorf("table must show the shed line:\n%s", out)
	}
}
