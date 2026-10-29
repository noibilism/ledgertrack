// Command ledgertrack-import migrates a legacy ledger (Bankman) into a running
// LedgerTrack over its HTTP API. It never touches the database.
//
//	ledgertrack-import validate --input ./export
//	ledgertrack-import plan     --input ./export --url https://staging --auth-token $TOKEN
//	ledgertrack-import apply    --input ./export --url https://staging --auth-token $TOKEN --yes
//	ledgertrack-import verify   --input ./export --url https://staging --auth-token $TOKEN --out reconciliation.csv
//	ledgertrack-import status
//
// See docs/import/README.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/internal/legacyimport"
)

// exitError carries a process exit code.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

type flags struct {
	url, ledger, token   string
	rate                 float64
	concurrency          int
	timeout              time.Duration
	maxRetries           int
	statePath            string
	input                string
	trimZeros            bool
	synthesizeKYC        bool
	assetTemplate        string
	sourceTemplate       string
	precision            []string
	planOut              string
	verifyOut            string
	deep                 bool
	yes                  bool
	failFast             bool
	adoptUnmarkedClients bool
	statusLimit          int
}

func main() {
	if err := newRoot().Execute(); err != nil {
		var ee *exitError
		if errors.As(err, &ee) {
			if ee.msg != "" {
				fmt.Fprintln(os.Stderr, ee.msg)
			}
			os.Exit(ee.code)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	f := &flags{}
	root := &cobra.Command{
		Use:           "ledgertrack-import",
		Short:         "Import a legacy ledger into LedgerTrack through its HTTP API",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.StringVar(&f.url, "url", envOr("LEDGERTRACK_URL", "http://localhost:3068"), "LedgerTrack base URL (env LEDGERTRACK_URL)")
	pf.StringVar(&f.ledger, "ledger", "ledgertrack", "ledger name")
	pf.StringVar(&f.token, "auth-token", os.Getenv("LEDGERTRACK_AUTH_TOKEN"), "bearer token (env LEDGERTRACK_AUTH_TOKEN)")
	pf.Float64Var(&f.rate, "rate", 0, "maximum requests per second, 0 for unlimited")
	pf.IntVar(&f.concurrency, "concurrency", 4, "parallel workers")
	pf.DurationVar(&f.timeout, "timeout", 30*time.Second, "per-request timeout")
	pf.IntVar(&f.maxRetries, "max-retries", 5, "retries with backoff for idempotent requests on timeouts, 429 and 5xx")
	pf.StringVar(&f.statePath, "state", "import-state.json", "checkpoint file used to resume (one per environment)")

	inputFlags := func(c *cobra.Command) {
		c.Flags().StringVar(&f.input, "input", "", "directory holding products.jsonl, clients.jsonl, accounts.jsonl, balances.jsonl, liens.jsonl")
		_ = c.MarkFlagRequired("input")
		c.Flags().BoolVar(&f.trimZeros, "trim-trailing-zeros", false, "tolerate all-zero fractional digits beyond the currency precision (10.5000 for a 2dp currency)")
		c.Flags().BoolVar(&f.synthesizeKYC, "allow-synthesized-kyc-evidence", false, "allow kyc_level 2 and 3 by synthesizing placeholder documents/evidence tagged provider=legacy-import")
	}
	ledgerFlags := func(c *cobra.Command) {
		c.Flags().StringVar(&f.assetTemplate, "asset-template", legacyimport.DefaultAssetTemplate, "ledger asset for a currency; {CCY} and {PREC} are substituted")
		c.Flags().StringVar(&f.sourceTemplate, "source-template", legacyimport.DefaultSourceTemplate, "migration source account per currency; {CCY} is substituted")
	}

	validate := &cobra.Command{
		Use:   "validate",
		Short: "Check schemas, references and totals. No network access.",
		RunE:  func(cmd *cobra.Command, _ []string) error { return runValidate(cmd, f) },
	}
	inputFlags(validate)
	validate.Flags().StringArrayVar(&f.precision, "precision", nil, "currency precision as CCY=N, repeatable (otherwise products.jsonl precision, then 2)")

	plan := &cobra.Command{
		Use:   "plan",
		Short: "Dry run against the API: what would be created, skipped or conflict",
		RunE:  func(cmd *cobra.Command, _ []string) error { return runPlan(cmd, f) },
	}
	inputFlags(plan)
	ledgerFlags(plan)
	plan.Flags().StringVar(&f.planOut, "out", "", "also write the full plan as JSON to this file")
	plan.Flags().BoolVar(&f.adoptUnmarkedClients, "adopt-unmarked-clients", false, "plan to adopt pending clients that match by legal name and registration number")

	apply := &cobra.Command{
		Use:   "apply",
		Short: "Import. Safe to re-run: imported items are skipped.",
		RunE:  func(cmd *cobra.Command, _ []string) error { return runApply(cmd, f) },
	}
	inputFlags(apply)
	ledgerFlags(apply)
	apply.Flags().BoolVar(&f.yes, "yes", false, "confirm that writing to the target is intended")
	apply.Flags().BoolVar(&f.failFast, "fail-fast", false, "stop at the first failure")
	apply.Flags().BoolVar(&f.adoptUnmarkedClients, "adopt-unmarked-clients", false, "adopt a pending client that matches by legal name and registration number instead of reporting a conflict (recovery after a crash with a lost state file)")

	verify := &cobra.Command{
		Use:   "verify",
		Short: "Read everything back and reconcile with zero tolerance",
		RunE:  func(cmd *cobra.Command, _ []string) error { return runVerify(cmd, f) },
	}
	inputFlags(verify)
	ledgerFlags(verify)
	verify.Flags().StringVar(&f.verifyOut, "out", "reconciliation.csv", "reconciliation report CSV")
	verify.Flags().BoolVar(&f.deep, "deep", false, "also check every opening and lien transaction by reference")

	status := &cobra.Command{
		Use:   "status",
		Short: "Show progress recorded in the state file",
		RunE:  func(cmd *cobra.Command, _ []string) error { return runStatus(cmd, f) },
	}
	status.Flags().IntVar(&f.statusLimit, "limit", 50, "maximum number of problem items to list, 0 for all")

	root.AddCommand(validate, plan, apply, verify, status)
	return root
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func (f *flags) options() legacyimport.Options {
	return legacyimport.Options{
		Concurrency:          f.concurrency,
		FailFast:             f.failFast,
		AssetTemplate:        f.assetTemplate,
		SourceTemplate:       f.sourceTemplate,
		AdoptUnmarkedClients: f.adoptUnmarkedClients,
		Logf:                 logf,
	}
}

func (f *flags) client() (*legacyimport.Client, error) {
	return legacyimport.NewClient(legacyimport.ClientConfig{
		BaseURL:    f.url,
		Ledger:     f.ledger,
		Token:      f.token,
		HTTP:       &http.Client{Timeout: f.timeout},
		Rate:       f.rate,
		MaxRetries: f.maxRetries,
	})
}

func printIssues(issues []legacyimport.Issue) (errs, warns int) {
	for _, i := range issues {
		fmt.Fprintln(os.Stderr, i.String())
		if i.Severity == "error" {
			errs++
		} else {
			warns++
		}
	}
	return errs, warns
}

func parsePrecisions(specs []string) (map[string]int, error) {
	out := map[string]int{}
	for _, s := range specs {
		k, v, ok := strings.Cut(s, "=")
		n, err := strconv.Atoi(v)
		if !ok || err != nil || n < 0 || n > 18 || k == "" {
			return nil, fmt.Errorf("invalid --precision %q, expected CCY=N", s)
		}
		out[strings.ToUpper(k)] = n
	}
	return out, nil
}

func runValidate(cmd *cobra.Command, f *flags) error {
	prec, err := parsePrecisions(f.precision)
	if err != nil {
		return err
	}
	ds, issues := legacyimport.LoadDataset(f.input)
	if legacyimport.HasErrors(issues) {
		errs, warns := printIssues(issues)
		return &exitError{1, fmt.Sprintf("invalid input: %d error(s), %d warning(s)", errs, warns)}
	}
	prep, more := legacyimport.Prepare(ds, legacyimport.PrepareOptions{
		Precision: prec, TrimZeros: f.trimZeros, AllowSynthesizedEvidence: f.synthesizeKYC,
	})
	issues = append(issues, more...)
	errs, warns := printIssues(issues)
	fmt.Printf("products=%d clients=%d accounts=%d balances=%d liens=%d\n",
		len(ds.Products), len(ds.Clients), len(ds.Accounts), len(ds.Balances), len(ds.Liens))
	for _, l := range prep.TotalsSummary() {
		fmt.Println(l)
	}
	if errs > 0 {
		return &exitError{1, fmt.Sprintf("invalid input: %d error(s), %d warning(s)", errs, warns)}
	}
	fmt.Printf("valid (%d warning(s)). Precisions were not checked against a server; plan does that.\n", warns)
	return nil
}

// prepareOnline loads the input and validates it against the server currency registry.
func prepareOnline(ctx context.Context, f *flags, c *legacyimport.Client) (*legacyimport.Prepared, *legacyimport.Snapshot, error) {
	opt := f.options()
	if err := opt.Validate(); err != nil {
		return nil, nil, err
	}
	ds, issues := legacyimport.LoadDataset(f.input)
	if legacyimport.HasErrors(issues) {
		errs, warns := printIssues(issues)
		return nil, nil, &exitError{1, fmt.Sprintf("invalid input: %d error(s), %d warning(s); run validate", errs, warns)}
	}
	snap, err := legacyimport.LoadSnapshot(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	prep, more := legacyimport.Prepare(ds, legacyimport.PrepareOptions{
		Precision: snap.PrecisionMap(), Authoritative: true, TrimZeros: f.trimZeros, AllowSynthesizedEvidence: f.synthesizeKYC,
	})
	errs, warns := printIssues(append(issues, more...))
	if errs > 0 {
		return nil, nil, &exitError{1, fmt.Sprintf("invalid input: %d error(s), %d warning(s)", errs, warns)}
	}
	return prep, snap, nil
}

func runPlan(cmd *cobra.Command, f *flags) error {
	ctx, stop := signalContext()
	defer stop()
	c, err := f.client()
	if err != nil {
		return err
	}
	prep, snap, err := prepareOnline(ctx, f, c)
	if err != nil {
		return err
	}
	var st *legacyimport.State
	if _, statErr := os.Stat(f.statePath); statErr == nil {
		if st, err = legacyimport.LoadState(f.statePath, c.Target()); err != nil {
			return err
		}
		defer st.Close()
	}
	pl, err := legacyimport.BuildPlan(ctx, c, prep, snap, st, f.options())
	if err != nil {
		return err
	}
	fmt.Print(pl.Summary())
	for _, l := range prep.TotalsSummary() {
		fmt.Println(l)
	}
	if f.planOut != "" {
		b, _ := json.MarshalIndent(pl, "", "  ")
		if err := os.WriteFile(f.planOut, b, 0o644); err != nil {
			return err
		}
	}
	if n := pl.Conflicts(); n > 0 {
		return &exitError{3, fmt.Sprintf("%d conflict(s): nothing will be overwritten, resolve them before apply", n)}
	}
	return nil
}

func runApply(cmd *cobra.Command, f *flags) error {
	if !f.yes {
		return &exitError{2, fmt.Sprintf("apply writes to %s (ledger %s). Run plan first, then re-run with --yes.", f.url, f.ledger)}
	}
	ctx, stop := signalContext()
	defer stop()
	c, err := f.client()
	if err != nil {
		return err
	}
	prep, snap, err := prepareOnline(ctx, f, c)
	if err != nil {
		return err
	}
	st, err := legacyimport.LoadState(f.statePath, c.Target())
	if err != nil {
		return err
	}
	defer st.Close()
	res, err := legacyimport.NewEngine(c, prep, snap, st, f.options()).Apply(ctx)
	fmt.Print(res.Summary())
	if err != nil {
		return &exitError{1, "interrupted: " + err.Error() + ". Re-run apply to resume."}
	}
	if n := res.Failed(); n > 0 {
		return &exitError{1, fmt.Sprintf("%d item(s) failed, conflict or are blocked. Fix them and re-run apply, then run verify.", n)}
	}
	fmt.Println("import complete. Run verify next.")
	return nil
}

func runVerify(cmd *cobra.Command, f *flags) error {
	ctx, stop := signalContext()
	defer stop()
	c, err := f.client()
	if err != nil {
		return err
	}
	prep, snap, err := prepareOnline(ctx, f, c)
	if err != nil {
		return err
	}
	rep, err := legacyimport.Verify(ctx, c, prep, snap, f.options(), f.deep)
	if err != nil {
		return err
	}
	out, err := os.Create(f.verifyOut)
	if err != nil {
		return err
	}
	if err := rep.WriteCSV(out); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	fmt.Print(rep.Summary())
	fmt.Println("report:", f.verifyOut)
	if !rep.Pass() {
		return &exitError{1, "reconciliation FAILED"}
	}
	return nil
}

func runStatus(cmd *cobra.Command, f *flags) error {
	st, err := legacyimport.ReadStateReadOnly(f.statePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &exitError{1, "no state file at " + f.statePath + ": nothing has been applied yet"}
		}
		return err
	}
	fmt.Print(legacyimport.StatusReport(st, f.statusLimit))
	return nil
}
