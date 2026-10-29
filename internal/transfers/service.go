package transfers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/go-libs/v3/logging"
)

// ErrReferenceConflict is returned when a reference is reused with different
// request parameters. Nothing is posted.
var ErrReferenceConflict = errors.New("reference already used by a different request")

type Config struct {
	// Grace is the minimum age of a non-terminal transfer before the recovery
	// runner touches it. It also is the base of the exponential backoff.
	Grace time.Duration
	// MaxBackoff caps the backoff between recovery attempts of one transfer.
	MaxBackoff time.Duration
	// Lease is how long a driver owns a transfer. It must exceed the longest
	// expected drive; if it is exceeded anyway, ledger idempotency still
	// prevents double posting.
	Lease time.Duration
	// MaxAttempts is the number of drives (inline plus recovery) before a
	// transfer is moved to failed_needs_review.
	MaxAttempts int
	// BatchSize is the number of transfers one recovery pass claims.
	BatchSize int
	// ClientRetryCooldown rate limits client-driven re-drives of one transfer.
	ClientRetryCooldown time.Duration
}

func DefaultConfig() Config {
	return Config{
		Grace:               2 * time.Minute,
		MaxBackoff:          30 * time.Minute,
		Lease:               2 * time.Minute,
		MaxAttempts:         10,
		BatchSize:           100,
		ClientRetryCooldown: 5 * time.Second,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Grace <= 0 {
		c.Grace = d.Grace
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = d.MaxBackoff
	}
	if c.Lease <= 0 {
		c.Lease = d.Lease
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = d.MaxAttempts
	}
	if c.BatchSize <= 0 {
		c.BatchSize = d.BatchSize
	}
	if c.ClientRetryCooldown < 0 {
		c.ClientRetryCooldown = 0
	}
	return c
}

type Status int

const (
	// StatusCompleted: every money leg is done; the response body is available.
	// The fee record may still be pending (Outcome.Warning is then set).
	StatusCompleted Status = iota + 1
	// StatusPending: a leg failed; the work is durable and recovery will finish it.
	StatusPending
	// StatusInProgress: another driver currently owns the transfer.
	StatusInProgress
	// StatusNeedsReview: automation gave up; a human must act.
	StatusNeedsReview
)

type Outcome struct {
	Status   Status
	Transfer *Transfer
	// Replayed is true when the transfer pre-existed this call (a retry).
	Replayed bool
	// Cause is the error that stopped a pending drive.
	Cause   error
	Warning string
}

// RecoveryStats summarises one recovery pass.
type RecoveryStats struct {
	Claimed, Completed, Pending, Escalated, Failed int
}

type Service struct {
	store  Store
	exec   LegExecutor
	cfg    Config
	logger logging.Logger
	owner  string

	needsReview metric.Int64Counter
	recovered   metric.Int64Counter
	pending     metric.Int64Counter
}

func NewService(store Store, exec LegExecutor, cfg Config, logger logging.Logger) *Service {
	meter := otel.Meter("ledger/transfers")
	needsReview, _ := meter.Int64Counter("transfers.needs_review", metric.WithDescription("transfers moved to failed_needs_review"))
	recovered, _ := meter.Int64Counter("transfers.recovered", metric.WithDescription("transfers completed by a re-drive"))
	pending, _ := meter.Int64Counter("transfers.pending", metric.WithDescription("transfer drives that stopped before completion"))
	host, _ := os.Hostname()
	return &Service{
		store:       store,
		exec:        exec,
		cfg:         cfg.withDefaults(),
		logger:      logger,
		owner:       fmt.Sprintf("%s/%s", host, uuid.NewString()),
		needsReview: needsReview,
		recovered:   recovered,
		pending:     pending,
	}
}

// detach keeps a drive alive when the HTTP client goes away: abandoning a drive
// half way would only create more work for recovery.
func (s *Service) detach(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Lease)
}

// Execute records the transfer durably, then drives its legs. A non-nil error
// means nothing was posted and the request was rejected (the error is the
// ledger's own, so callers can map it exactly as before) or the transfer could
// not even be recorded. Anything that may have posted money is reported through
// Outcome instead.
func (s *Service) Execute(ctx context.Context, in Input) (*Outcome, error) {
	ctx, cancel := s.detach(ctx)
	defer cancel()

	t := NewTransfer(in)
	for i := 0; i < 3; i++ {
		got, created, err := s.store.Create(ctx, t, s.owner, s.cfg.Lease)
		if err != nil {
			return nil, err
		}
		if created {
			return s.drive(ctx, got, false)
		}
		if got.RequestHash != t.RequestHash && got.State != StateFailed {
			return nil, fmt.Errorf("%w: %q", ErrReferenceConflict, in.Reference)
		}
		if got.State == StateFailed {
			rearmed, ok, err := s.store.Rearm(ctx, got.ID, t, s.owner, s.cfg.Lease)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue // somebody else re-armed or changed it: look again
			}
			return s.drive(ctx, rearmed, false)
		}
		return s.resume(ctx, got)
	}
	return nil, fmt.Errorf("transfer %q is changing concurrently, retry", in.Reference)
}

// resume handles a retry that found an existing transfer.
func (s *Service) resume(ctx context.Context, got *Transfer) (*Outcome, error) {
	if got.State.Terminal() {
		return outcomeFor(got), nil
	}
	claimed, ok, err := s.store.Claim(ctx, got.ID, s.owner, ClaimOptions{
		Lease:   s.cfg.Lease,
		MinIdle: s.cfg.ClientRetryCooldown,
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		fresh, err := s.store.Get(ctx, got.ID)
		if err != nil {
			return nil, err
		}
		return outcomeFor(fresh), nil
	}
	return s.drive(ctx, claimed, true)
}

func outcomeFor(t *Transfer) *Outcome {
	switch t.State {
	case StateCompleted:
		return &Outcome{Status: StatusCompleted, Transfer: t, Replayed: true}
	case StateNeedsReview:
		return &Outcome{Status: StatusNeedsReview, Transfer: t, Replayed: true}
	default:
		return &Outcome{Status: StatusInProgress, Transfer: t, Replayed: true}
	}
}

// RecoverBatch claims stale non-terminal transfers and re-drives them.
func (s *Service) RecoverBatch(ctx context.Context) (RecoveryStats, error) {
	var stats RecoveryStats
	claimed, err := s.store.ClaimBatch(ctx, s.owner, RecoveryOptions{
		Grace:      s.cfg.Grace,
		MaxBackoff: s.cfg.MaxBackoff,
		Lease:      s.cfg.Lease,
		Limit:      s.cfg.BatchSize,
	})
	if err != nil {
		return stats, err
	}
	stats.Claimed = len(claimed)
	for _, t := range claimed {
		if ctx.Err() != nil {
			// Remaining rows keep their lease until it expires; nothing is lost.
			return stats, ctx.Err()
		}
		dctx, cancel := s.detach(ctx)
		out, err := s.drive(dctx, t, true)
		cancel()
		switch {
		case err != nil:
			stats.Failed++
		case out.Status == StatusCompleted && out.Transfer.State == StateCompleted:
			stats.Completed++
			s.recovered.Add(ctx, 1)
			s.log(t).Infof("transfer recovered")
		case out.Status == StatusNeedsReview:
			stats.Escalated++
		default:
			stats.Pending++
		}
	}
	return stats, nil
}

func (s *Service) log(t *Transfer) logging.Logger {
	return s.logger.WithFields(map[string]any{
		"transfer_id": t.ID.String(),
		"reference":   t.Reference,
		"state":       string(t.State),
		"attempts":    t.Attempts,
	})
}

type feeRecordError struct{ err error }

func (e *feeRecordError) Error() string { return "fee record write failed: " + e.err.Error() }
func (e *feeRecordError) Unwrap() error { return e.err }

// drive runs legs until the transfer completes or a leg fails. The caller must
// hold the lease.
func (s *Service) drive(ctx context.Context, t *Transfer, replayed bool) (*Outcome, error) {
	for t.State != StateCompleted {
		if err := s.step(ctx, t); err != nil {
			return s.legFailed(ctx, t, err, replayed)
		}
	}
	return &Outcome{Status: StatusCompleted, Transfer: t, Replayed: replayed}, nil
}

// step performs exactly the next leg and persists the new state.
func (s *Service) step(ctx context.Context, t *Transfer) error {
	switch t.State {
	case StateCreated:
		leg, err := s.exec.PostUser(ctx, t)
		if err != nil {
			return err
		}
		id := leg.TxID
		t.UserTxID = &id
		t.Result = leg.Result
		return s.advance(ctx, t, StateUserPosted)
	case StateUserPosted:
		if !t.NeedsChannelLeg() {
			return s.advance(ctx, t, StateCompleted)
		}
		leg, err := s.exec.PostChannel(ctx, t)
		if err != nil {
			return err
		}
		id := leg.TxID
		t.ChannelTxID = &id
		if leg.Warning != "" {
			t.Result.Warning = leg.Warning
		}
		return s.advance(ctx, t, StateChannelPosted)
	case StateChannelPosted:
		if !t.NeedsRevenueLeg() {
			return s.advance(ctx, t, StateCompleted)
		}
		leg, err := s.exec.PostRevenue(ctx, t)
		if err != nil {
			return err
		}
		t.RevenueTxID = leg.TxID
		return s.advance(ctx, t, StateRevenuePosted)
	case StateRevenuePosted:
		if err := s.exec.RecordFee(ctx, t); err != nil {
			return &feeRecordError{err: err}
		}
		return s.advance(ctx, t, StateCompleted)
	default:
		return fmt.Errorf("transfer %s in unexpected state %q", t.ID, t.State)
	}
}

func (s *Service) advance(ctx context.Context, t *Transfer, to State) error {
	from := t.State
	t.State = to
	t.LastError = ""
	if err := s.store.Save(ctx, t, from, s.owner); err != nil {
		t.State = from
		return err
	}
	return nil
}

func (s *Service) legFailed(ctx context.Context, t *Transfer, err error, replayed bool) (*Outcome, error) {
	var rejected *RejectedError
	switch {
	case errors.Is(err, ErrLeaseLost):
		// Another driver took over (our lease expired). It will finish the job.
		fresh, gerr := s.store.Get(ctx, t.ID)
		if gerr != nil {
			return nil, gerr
		}
		return outcomeFor(fresh), nil

	case t.State == StateCreated && (errors.As(err, &rejected) || errors.Is(err, ErrReferenceMismatch)):
		// The first leg was refused, so nothing is posted: close the transfer so
		// recovery can never turn a rejected request into a debit.
		if ferr := s.store.Finish(ctx, t.ID, s.owner, StateFailed, err.Error()); ferr != nil && !errors.Is(ferr, ErrLeaseLost) {
			s.log(t).Errorf("closing rejected transfer: %v", ferr)
		}
		return nil, err

	case errors.Is(err, ErrReferenceMismatch):
		return s.escalate(ctx, t, err, replayed)
	}

	var feeErr *feeRecordError
	t.LastError = truncate(err.Error(), 2000)
	if t.Attempts >= s.cfg.MaxAttempts {
		return s.escalate(ctx, t, err, replayed)
	}
	if rerr := s.store.Release(ctx, t.ID, s.owner, t.LastError); rerr != nil && !errors.Is(rerr, ErrLeaseLost) {
		s.log(t).Errorf("releasing transfer lease: %v", rerr)
	}
	s.pending.Add(ctx, 1)
	s.log(t).Errorf("transfer stopped before completion, recovery will retry: %v", err)
	if errors.As(err, &feeErr) {
		// All money legs are done: answer like before (success plus warning).
		return &Outcome{Status: StatusCompleted, Transfer: t, Replayed: replayed, Warning: err.Error()}, nil
	}
	return &Outcome{Status: StatusPending, Transfer: t, Replayed: replayed, Cause: err}, nil
}

func (s *Service) escalate(ctx context.Context, t *Transfer, cause error, replayed bool) (*Outcome, error) {
	msg := truncate(cause.Error(), 2000)
	if ferr := s.store.Finish(ctx, t.ID, s.owner, StateNeedsReview, msg); ferr != nil && !errors.Is(ferr, ErrLeaseLost) {
		s.log(t).Errorf("moving transfer to needs_review: %v", ferr)
	}
	t.State = StateNeedsReview
	t.LastError = msg
	s.needsReview.Add(ctx, 1)
	s.log(t).Errorf("ALERT transfer moved to failed_needs_review, manual action required (user_tx_id=%v channel_tx_id=%v revenue_tx_id=%v): %v",
		ptr(t.UserTxID), ptr(t.ChannelTxID), ptr(t.RevenueTxID), cause)
	return &Outcome{Status: StatusNeedsReview, Transfer: t, Replayed: replayed, Cause: cause}, nil
}

func ptr(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
