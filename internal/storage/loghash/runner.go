package loghash

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"

	ledger "github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
)

func (h *Hasher) Name() string { return "Async log hasher" }

// Run hashes the ledgers configured with HASH_LOGS=ASYNC until Stop is called.
func (h *Hasher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := h.RunOnce(ctx); err != nil {
				h.logger.Errorf("log hasher: %v", err)
			}
			timer.Reset(h.cfg.Interval)
		case ch := <-h.stop:
			close(ch)
			return nil
		}
	}
}

func (h *Hasher) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.stop <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
	return nil
}

// RunOnce lists the ASYNC ledgers and hashes the pending logs of each.
func (h *Hasher) RunOnce(ctx context.Context) error {
	ctx, span := h.tracer.Start(ctx, "RunOnce")
	defer span.End()

	var ledgers []ledger.Ledger
	err := storagecommon.Iterate(
		ctx,
		storagecommon.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
			Options: storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]{
				Builder: query.Match(fmt.Sprintf("features[%s]", features.FeatureHashLogs), "ASYNC"),
			},
		},
		systemstore.New(h.db).Ledgers().Paginate,
		func(cursor *bunpaginate.Cursor[ledger.Ledger]) error {
			ledgers = append(ledgers, cursor.Data...)
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("listing ledgers: %w", err)
	}

	var firstErr error
	for _, l := range ledgers {
		res, err := h.HashLedgerUntilCaughtUp(ctx, l)
		if err != nil {
			h.logger.Errorf("log hasher: ledger %s: %v", l.Name, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !res.Skipped && res.Unhashed >= 0 {
			h.report(l.Name, res)
		}
	}
	return firstErr
}

// report emits the operator facing log lines. The metric ledger.log_hasher.lag_seconds carries the same information.
func (h *Hasher) report(name string, res Result) {
	now := h.now()
	if res.Lag > h.cfg.AlertLag {
		if now.Sub(h.lastAlert[name]) >= 30*time.Second {
			h.lastAlert[name] = now
			h.logger.Errorf("ALERT log hash chain lag: ledger=%s lag=%s threshold=%s unhashed=%d head_id=%d blocked_on_id_hole=%t. "+
				"Logs newer than the chain head are not covered by the tamper evidence. See docs/design/async-log-hashing.md (runbook)",
				name, res.Lag.Round(time.Second), h.cfg.AlertLag, res.Unhashed, res.HeadID, res.Blocked)
		}
		return
	}
	if now.Sub(h.lastInfo[name]) >= time.Minute {
		h.lastInfo[name] = now
		h.logger.Infof("log hasher: ledger=%s head_id=%d unhashed=%d lag=%s", name, res.HeadID, res.Unhashed, res.Lag.Round(time.Millisecond))
	}
}
