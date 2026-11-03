package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/service"

	"github.com/formancehq/ledger/internal/storage/loghash"
)

const WalletLedgerHashModeFlag = "wallet-ledger-hash-mode"

func addWalletLedgerHashModeFlag(cmd *cobra.Command) {
	cmd.Flags().String(WalletLedgerHashModeFlag, "",
		"HASH_LOGS mode given at creation to the wallet ledgers (ledgertrack, channels-*, revenue-*): SYNC or ASYNC. "+
			"Empty keeps the default (SYNC). ASYNC removes the per ledger lock from the commit path and chains the logs "+
			"in the background (worker), see docs/design/async-log-hashing.md. Never changes an existing ledger, use hashchain set-mode")
}

// Exit codes of `hashchain verify` and `hashchain status`.
const (
	exitChainBroken = 2
	exitLagExceeded = 3
)

func NewHashChainCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hashchain",
		Short: "Verify and operate the log hash chain of a ledger",
	}
	cmd.AddCommand(newHashChainVerifyCommand(), newHashChainStatusCommand(), newHashChainSetModeCommand())
	return cmd
}

func addHashChainFlags(cmd *cobra.Command) {
	cmd.Flags().String("ledger", "", "Ledger name")
	_ = cmd.MarkFlagRequired("ledger")
	cmd.Flags().Bool("json", false, "JSON output")
	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
}

func newHashChainVerifyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Recompute the hash chain of a ledger and report the first mismatch and the unhashed tail",
		Long: `Walks the logs of the ledger in id order and checks, for each hashed log, that its hash is the hash of its content
chained to the stored hash of the previous log. Read only.

Reports: logs verified, the first mismatch (if any), the chain head, the number of holes in the id sequence (normal: rolled
back transactions), and the unhashed tail (logs committed but not chained yet) with its lag.

--anchor-id/--anchor-hash additionally check that the hash of one log equals a value you stored outside the database. This is
the only check that detects an attacker who rewrote the chain consistently.

Exit code: 0 chain intact, 2 mismatch or anchor failure, 3 tail older than --max-lag, 1 error.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			name, _ := cmd.Flags().GetString("ledger")
			asJSON, _ := cmd.Flags().GetBool("json")
			anchorID, _ := cmd.Flags().GetUint64("anchor-id")
			anchorHash, _ := cmd.Flags().GetString("anchor-hash")
			maxLag, _ := cmd.Flags().GetDuration("max-lag")
			pageSize, _ := cmd.Flags().GetInt("page-size")

			opts := loghash.VerifyOptions{PageSize: pageSize}
			if cmd.Flags().Changed("anchor-id") || anchorHash != "" {
				if !cmd.Flags().Changed("anchor-id") || anchorHash == "" {
					return fmt.Errorf("--anchor-id and --anchor-hash go together")
				}
				opts.AnchorID = &anchorID
				opts.AnchorHash = anchorHash
			}

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}
			db, err := bunconnect.OpenSQLDB(cmd.Context(), *connectionOptions)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			l, err := loghash.GetLedger(cmd.Context(), db, name)
			if err != nil {
				return err
			}
			report, err := loghash.Verify(cmd.Context(), db, l.Bucket, l.Name, l.Features["HASH_LOGS"], opts)
			if err != nil {
				return err
			}

			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				if err := enc.Encode(report); err != nil {
					return err
				}
			} else {
				printReport(cmd, report)
			}

			switch {
			case !report.OK():
				os.Exit(exitChainBroken)
			case maxLag > 0 && time.Duration(report.Tail.LagSeconds*float64(time.Second)) > maxLag:
				os.Exit(exitLagExceeded)
			}
			return nil
		},
	}
	cmd.Flags().Uint64("anchor-id", 0, "Log id of an externally stored chain head")
	cmd.Flags().String("anchor-hash", "", "Hex hash expected for --anchor-id")
	cmd.Flags().Duration("max-lag", 0, "Exit with code 3 when the oldest unhashed log is older than this (0 disables)")
	cmd.Flags().Int("page-size", 5000, "Logs read per query")
	addHashChainFlags(cmd)
	return cmd
}

func printReport(cmd *cobra.Command, r *loghash.Report) {
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "ledger:        %s (HASH_LOGS=%s)\n", r.Ledger, r.HashMode)
	_, _ = fmt.Fprintf(out, "verified:      %d logs\n", r.Verified)
	if r.HeadID != nil {
		_, _ = fmt.Fprintf(out, "chain head:    id=%d hash=%s\n", *r.HeadID, r.HeadHash)
	} else {
		_, _ = fmt.Fprintf(out, "chain head:    (empty chain)\n")
	}
	_, _ = fmt.Fprintf(out, "id holes:      %d (rolled back transactions, informational)\n", r.IDHoles)
	if r.Tail.Count > 0 {
		_, _ = fmt.Fprintf(out, "unhashed tail: %d logs, ids %d..%d, oldest %s, lag %.1fs\n",
			r.Tail.Count, *r.Tail.FirstID, *r.Tail.LastID, r.Tail.Oldest.Format(time.RFC3339), r.Tail.LagSeconds)
	} else {
		_, _ = fmt.Fprintf(out, "unhashed tail: none\n")
	}
	if r.Anchor != nil {
		if r.Anchor.Match {
			_, _ = fmt.Fprintf(out, "anchor:        id=%d MATCH\n", r.Anchor.ID)
		} else {
			_, _ = fmt.Fprintf(out, "anchor:        id=%d MISMATCH expected=%s actual=%s\n", r.Anchor.ID, r.Anchor.Expected, r.Anchor.Actual)
		}
	}
	if r.FirstMismatch != nil {
		_, _ = fmt.Fprintf(out, "RESULT:        CHAIN BROKEN, %d problem(s), first at log id %d (%s) stored=%s expected=%s\n",
			r.Mismatches, r.FirstMismatch.ID, r.FirstMismatch.Reason, r.FirstMismatch.Stored, r.FirstMismatch.Expected)
	} else if r.Anchor != nil && !r.Anchor.Match {
		_, _ = fmt.Fprintf(out, "RESULT:        ANCHOR MISMATCH\n")
	} else {
		_, _ = fmt.Fprintf(out, "RESULT:        OK\n")
	}
}

func newHashChainStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the chain head and how far behind the chain is (cheap, suitable for monitoring)",
		Long: `Reads the head of the chain and the size and age of the backlog. Does not recompute anything.
Exit code: 0, 3 when the oldest unhashed log is older than --max-lag, 1 error.
This works even if the worker (and its own ALERT log line) is down, which is why monitoring should run it.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			name, _ := cmd.Flags().GetString("ledger")
			asJSON, _ := cmd.Flags().GetBool("json")
			maxLag, _ := cmd.Flags().GetDuration("max-lag")

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}
			db, err := bunconnect.OpenSQLDB(cmd.Context(), *connectionOptions)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			l, err := loghash.GetLedger(cmd.Context(), db, name)
			if err != nil {
				return err
			}
			st, err := loghash.GetStatus(cmd.Context(), db, *l)
			if err != nil {
				return err
			}
			if asJSON {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				if err := enc.Encode(st); err != nil {
					return err
				}
			} else {
				head := "none"
				if st.HeadID != nil {
					head = fmt.Sprintf("%d", *st.HeadID)
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger=%s HASH_LOGS=%s head_id=%s unhashed=%d lag=%.1fs\n", st.Ledger, st.HashMode, head, st.Unhashed, st.LagSeconds)
			}
			if maxLag > 0 && time.Duration(st.LagSeconds*float64(time.Second)) > maxLag {
				os.Exit(exitLagExceeded)
			}
			return nil
		},
	}
	cmd.Flags().Duration("max-lag", 0, "Exit with code 3 when the oldest unhashed log is older than this (0 disables)")
	addHashChainFlags(cmd)
	return cmd
}

func newHashChainSetModeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-mode",
		Short: "Switch an existing ledger between SYNC and ASYNC log hashing",
		Long: `SYNC -> ASYNC drops the hashing trigger and records the new mode. The worker then continues the chain from the
current head (same hash function, the chain stays continuous and verifiable end to end).
ASYNC -> SYNC refuses unless every log is already chained, then recreates the trigger.

Takes a short exclusive lock on the logs table: run it in a quiet period. The processes serving the ledger cache its
configuration: restart every API and worker process afterwards.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			name, _ := cmd.Flags().GetString("ledger")
			mode, _ := cmd.Flags().GetString("mode")

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}
			db, err := bunconnect.OpenSQLDB(cmd.Context(), *connectionOptions)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()

			previous, err := loghash.SetHashMode(cmd.Context(), db, name, mode)
			if err != nil {
				return err
			}
			if previous == mode {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger %s already uses HASH_LOGS=%s, nothing to do\n", name, mode)
				return nil
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "ledger %s: HASH_LOGS %s -> %s. Restart every API and worker process now.\n", name, previous, mode)
			return nil
		},
	}
	cmd.Flags().String("mode", "", "SYNC or ASYNC")
	_ = cmd.MarkFlagRequired("mode")
	addHashChainFlags(cmd)
	return cmd
}
