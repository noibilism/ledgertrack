// Command ledgertrack-bench is a load generator and consistency checker for
// the LedgerTrack wallet API (credit, debit, lien, lien release, channel debit
// with fees). It supports an open-loop mode with a fixed arrival rate, which
// avoids coordinated omission, and a closed-loop max-throughput mode.
//
// Exit codes: 0 success, 1 usage or setup failure, 2 an SLO gate failed,
// 3 the post-run consistency check failed (3 wins over 2).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	exitOK          = 0
	exitSetup       = 1
	exitGate        = 2
	exitConsistency = 3
)

// logOut receives progress and setup messages.
var logOut io.Writer = os.Stderr

func logf(format string, args ...any) {
	fmt.Fprintf(logOut, format+"\n", args...)
}

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr))
}

func realMain(args []string, stdout, stderr io.Writer) int {
	logOut = stderr
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		fmt.Fprintf(stderr, "error: %v\n", err)
		return exitSetup
	}
	if cfg.AuthToken == "" {
		cfg.AuthToken = os.Getenv("LEDGERTRACK_BENCH_TOKEN")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl := newClient(cfg)
	runID := strconvBase36(uint64(time.Now().UnixNano()) / 1000)

	uni, bl, err := setup(ctx, cfg, cl, runID)
	if err != nil {
		fmt.Fprintf(stderr, "setup failed: %v\n", err)
		return exitSetup
	}

	bk := newBook(cfg.Wallets, len(cfg.Currencies), cfg.Channels)
	r := newRunner(cfg, cl, uni, bk, runID)

	started := time.Now()
	logf("run: %s, warmup %s, measure %s, %d workers", modeString(cfg), cfg.Warmup, cfg.Duration, cfg.Concurrency)
	rt := r.run(ctx)
	rep := buildReport(cfg, rt, r.rec, r.rec.merge(), started)

	if cfg.Check {
		logf("check: reading back %d wallets", cfg.Wallets)
		// Use a fresh context so an interrupted run can still be verified.
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		rep.Check = runCheck(cctx, cfg, cl, uni, bl, bk, r.unexpected409.Load())
		cancel()
	} else {
		rep.Check = &CheckResult{Ran: false, Passed: true}
	}

	// Human table goes to stdout unless stdout carries JSON or CSV.
	table := stdout
	if cfg.JSONOut == "-" || cfg.CSVOut == "-" {
		table = stderr
	}
	rep.writeTable(table)

	if err := emit(cfg.JSONOut, stdout, rep.writeJSON); err != nil {
		fmt.Fprintf(stderr, "writing JSON report: %v\n", err)
		return exitSetup
	}
	if err := emit(cfg.CSVOut, stdout, rep.writeCSV); err != nil {
		fmt.Fprintf(stderr, "writing CSV report: %v\n", err)
		return exitSetup
	}

	switch {
	case !rep.Check.Passed:
		return exitConsistency
	case !rep.GatesPassed:
		return exitGate
	}
	return exitOK
}

func modeString(cfg *Config) string {
	if cfg.openLoop() {
		return fmt.Sprintf("open loop at %.0f TPS (%s arrivals)", cfg.Rate, cfg.Arrival)
	}
	return "closed loop, max throughput"
}

// emit writes via fn to path ("-" is stdout, "" is skipped).
func emit(path string, stdout io.Writer, fn func(io.Writer) error) error {
	switch path {
	case "":
		return nil
	case "-":
		return fn(stdout)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := fn(f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
