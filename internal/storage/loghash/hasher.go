package loghash

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/logging"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
)

const (
	DefaultInterval  = 500 * time.Millisecond
	DefaultBatchSize = 500
	DefaultGapGrace  = time.Second
	DefaultAlertLag  = 60 * time.Second

	maxBatchesPerCycle = 50
)

type Config struct {
	// Interval between two hashing cycles.
	Interval time.Duration
	// BatchSize is the max number of logs chained in one database transaction.
	BatchSize int
	// GapGrace is the extra time a hole in the log ids must have been observed before it is considered final.
	// It covers the (server side, microseconds) window between the allocation of an id and the
	// assignment of a transaction id to the inserting transaction.
	GapGrace time.Duration
	// AlertLag is the age of the oldest unchained log above which an error is logged.
	AlertLag time.Duration
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.GapGrace <= 0 {
		c.GapGrace = DefaultGapGrace
	}
	if c.AlertLag <= 0 {
		c.AlertLag = DefaultAlertLag
	}
	return c
}

// observation records what the hasher saw of the ids holes at one point in time.
// Once every transaction that was in flight at that point has ended (and GapGrace has elapsed),
// every hole at or below maxVisibleID is final.
type observation struct {
	// snapshotXmax is the first transaction id not yet assigned when the observation was made.
	// Every transaction that could still make a hole disappear had an id below it (or no id yet).
	snapshotXmax uint64
	at           time.Time
	maxVisibleID uint64
}

type ledgerState struct {
	obs        *observation
	resolvedUp uint64
	// headID is the head of the chain as of the last commit of this hasher (0: unknown, look it up).
	// Looking the head up is a scan from the end of the ledger over every unhashed log, so it is cached: after a long
	// outage the backlog is big and the lookup would be paid once per batch.
	headID uint64
}

// Result is the outcome of one HashLedger call.
type Result struct {
	// Skipped is true when another hasher holds the ledger.
	Skipped bool
	// Hashed is the number of logs chained by this call.
	Hashed int
	// HeadID is the id of the last chained log (0 when the chain is empty).
	HeadID uint64
	// Unhashed is the number of logs after the head still waiting to be chained.
	Unhashed int64
	// Lag is the age of the oldest log waiting to be chained.
	Lag time.Duration
	// Blocked is true when the chain is waiting on a hole of ids to become final.
	Blocked bool
}

type ledgerStats struct {
	Result
	at time.Time
}

// Hasher chains the logs of HASH_LOGS=ASYNC ledgers.
type Hasher struct {
	db     *bun.DB
	logger logging.Logger
	cfg    Config
	tracer trace.Tracer
	now    func() time.Time

	mu     sync.Mutex
	states map[int]*ledgerState
	stats  map[string]ledgerStats

	hashedCounter metric.Int64Counter
	lastAlert     map[string]time.Time
	lastInfo      map[string]time.Time

	stop chan chan struct{}
}

type Option func(*Hasher)

func WithTracer(t trace.Tracer) Option { return func(h *Hasher) { h.tracer = t } }

func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(h *Hasher) {
		meter := mp.Meter("github.com/formancehq/ledger/loghash")
		var err error
		h.hashedCounter, err = meter.Int64Counter("ledger.log_hasher.hashed_logs")
		if err != nil {
			panic(err)
		}
		unhashed, err := meter.Int64ObservableGauge("ledger.log_hasher.unhashed_logs",
			metric.WithDescription("Number of committed logs not yet part of the hash chain"))
		if err != nil {
			panic(err)
		}
		lag, err := meter.Float64ObservableGauge("ledger.log_hasher.lag_seconds",
			metric.WithUnit("s"),
			metric.WithDescription("Age of the oldest committed log not yet part of the hash chain. Alert above 60s"))
		if err != nil {
			panic(err)
		}
		head, err := meter.Int64ObservableGauge("ledger.log_hasher.head_id",
			metric.WithDescription("Id of the last log part of the hash chain"))
		if err != nil {
			panic(err)
		}
		_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			for name, s := range h.stats {
				attrs := metric.WithAttributes(attribute.String("ledger", name))
				o.ObserveInt64(unhashed, s.Unhashed, attrs)
				o.ObserveFloat64(lag, s.Lag.Seconds(), attrs)
				o.ObserveInt64(head, int64(s.HeadID), attrs)
			}
			return nil
		}, unhashed, lag, head)
		if err != nil {
			panic(err)
		}
	}
}

func WithClock(now func() time.Time) Option { return func(h *Hasher) { h.now = now } }

func NewHasher(logger logging.Logger, db *bun.DB, cfg Config, opts ...Option) *Hasher {
	h := &Hasher{
		db:        db,
		logger:    logger,
		cfg:       cfg.withDefaults(),
		tracer:    nooptrace.NewTracerProvider().Tracer("loghash"),
		now:       time.Now,
		states:    map[int]*ledgerState{},
		stats:     map[string]ledgerStats{},
		lastAlert: map[string]time.Time{},
		lastInfo:  map[string]time.Time{},
		stop:      make(chan chan struct{}),
	}
	h.hashedCounter, _ = noopmetric.NewMeterProvider().Meter("").Int64Counter("")
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Hasher) state(id int) *ledgerState {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.states[id]
	if !ok {
		s = &ledgerState{}
		h.states[id] = s
	}
	return s
}

// Stats returns the last known state of the chain of a ledger.
func (h *Hasher) Stats(name string) (Result, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.stats[name]
	return s.Result, ok
}

func parseXID(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

// HashLedger chains up to one batch of pending logs of the ledger, in a single database transaction.
func (h *Hasher) HashLedger(ctx context.Context, l ledger.Ledger) (Result, error) {
	ctx, span := h.tracer.Start(ctx, "HashLedger")
	defer span.End()
	span.SetAttributes(attribute.String("ledger", l.Name))

	if !l.HasFeature(features.FeatureHashLogs, "ASYNC") {
		return Result{}, fmt.Errorf("ledger %q is not configured with %s=ASYNC", l.Name, features.FeatureHashLogs)
	}
	bucket, err := quoteBucket(l.Bucket)
	if err != nil {
		return Result{}, err
	}
	st := h.state(l.ID)

	var res Result
	for attempt := 0; ; attempt++ {
		res, err = h.hashLedgerOnce(ctx, l, bucket, st)
		if errors.Is(err, errStaleHead) && attempt == 0 {
			continue
		}
		break
	}
	if err != nil {
		return Result{}, err
	}
	if res.Skipped {
		return res, nil
	}
	if res.HeadID != 0 {
		st.headID = res.HeadID
	}
	if res.Unhashed >= 0 {
		h.mu.Lock()
		h.stats[l.Name] = ledgerStats{Result: res, at: h.now()}
		h.mu.Unlock()
	}
	if res.Hashed > 0 {
		h.hashedCounter.Add(ctx, int64(res.Hashed), metric.WithAttributes(attribute.String("ledger", l.Name)))
	}
	return res, nil
}

var errStaleHead = errors.New("stale chain head")

func (h *Hasher) hashLedgerOnce(ctx context.Context, l ledger.Ledger, bucket string, st *ledgerState) (Result, error) {
	var (
		res Result
		err error
	)
	// REPEATABLE READ: the snapshot used to read the head, the ids and the transaction ids horizon is the same.
	err = h.db.RunInTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, func(ctx context.Context, tx bun.Tx) error {
		res = Result{}

		var locked bool
		// The lock is only used to make several hashers (several replicas) exclusive per ledger. Writers never take it.
		if err := tx.NewRaw(`select pg_try_advisory_xact_lock(hashtext(?))`, fmt.Sprintf("log-hasher:%d", l.ID)).Scan(ctx, &locked); err != nil {
			return fmt.Errorf("locking: %w", err)
		}
		if !locked {
			res.Skipped = true
			return nil
		}

		var xminS, xmaxS string
		if err := tx.NewRaw(`select pg_snapshot_xmin(pg_current_snapshot())::text, pg_snapshot_xmax(pg_current_snapshot())::text`).
			Scan(ctx, &xminS, &xmaxS); err != nil {
			return fmt.Errorf("reading snapshot: %w", err)
		}
		snapXmin, err := parseXID(xminS)
		if err != nil {
			return err
		}
		snapXmax, err := parseXID(xmaxS)
		if err != nil {
			return err
		}

		// Head of the chain
		var (
			headID   uint64
			headHash []byte
		)
		if st.headID != 0 {
			err = tx.NewRaw(`select id, hash from `+bucket+`.logs where ledger = ? and id = ? and hash is not null`, l.Name, st.headID).
				Scan(ctx, &headID, &headHash)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("reading cached head: %w", err)
			}
		}
		if headID == 0 {
			err = tx.NewRaw(`select id, hash from `+bucket+`.logs where ledger = ? and hash is not null order by id desc limit 1`, l.Name).
				Scan(ctx, &headID, &headHash)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("reading head: %w", err)
			}
		}

		var maxVisible *uint64
		if err := tx.NewRaw(`select max(id) from `+bucket+`.logs where ledger = ?`, l.Name).Scan(ctx, &maxVisible); err != nil {
			return fmt.Errorf("reading max id: %w", err)
		}

		now := h.now()
		// A hole observed earlier is final once every transaction in flight at that time has ended.
		if st.obs != nil && snapXmin >= st.obs.snapshotXmax && now.Sub(st.obs.at) >= h.cfg.GapGrace {
			if st.obs.maxVisibleID > st.resolvedUp {
				st.resolvedUp = st.obs.maxVisibleID
			}
			st.obs = nil
		}
		if st.obs == nil && maxVisible != nil && *maxVisible > st.resolvedUp {
			st.obs = &observation{snapshotXmax: snapXmax, at: now, maxVisibleID: *maxVisible}
		}

		var pending []struct {
			ID     uint64
			Hashed bool
		}
		if err := tx.NewRaw(`select id, hash is not null as hashed from `+bucket+`.logs where ledger = ? and id > ? order by id limit ?`,
			l.Name, headID, h.cfg.BatchSize).Scan(ctx, &pending); err != nil {
			return fmt.Errorf("reading pending ids: %w", err)
		}
		ids := make([]uint64, 0, len(pending))
		for _, p := range pending {
			if p.Hashed {
				// Somebody (another hasher) chained logs after the head we know: forget it and look the head up again.
				st.headID = 0
				return errStaleHead
			}
			ids = append(ids, p.ID)
		}

		n := chainablePrefix(headID, ids, st.resolvedUp)
		res.Blocked = n < len(ids)
		if n > 0 {
			first, last := ids[0], ids[n-1]
			r, err := tx.NewRaw(chainSQL(bucket), l.Name, first, last, headHash, l.Name).Exec(ctx)
			if err != nil {
				return fmt.Errorf("chaining logs: %w", err)
			}
			affected, err := r.RowsAffected()
			if err != nil {
				return err
			}
			if int(affected) != n {
				// Somebody else wrote hashes (or a log disappeared): do not commit a chain we cannot vouch for.
				return fmt.Errorf("chained %d logs, expected %d: concurrent modification of the hash chain", affected, n)
			}
			res.Hashed = n
			headID = last
		}
		res.HeadID = headID

		if res.Hashed >= h.cfg.BatchSize {
			// More to do right away: the backlog statistics are a scan of the backlog, they are computed when the batch is not full.
			res.Unhashed = -1
			return nil
		}
		var (
			lagSeconds *float64
			count      int64
		)
		if err := tx.NewRaw(`select count(*), extract(epoch from ((now() at time zone 'utc') - min(date))) from `+bucket+`.logs where ledger = ? and id > ?`, l.Name, headID).
			Scan(ctx, &count, &lagSeconds); err != nil {
			return fmt.Errorf("reading backlog: %w", err)
		}
		res.Unhashed = count
		if lagSeconds != nil && *lagSeconds > 0 {
			res.Lag = time.Duration(*lagSeconds * float64(time.Second))
		}

		return nil
	})
	if err != nil {
		// Another hasher committed between our snapshot and our lock (40001): it did our job, nothing to do.
		if strings.Contains(err.Error(), "SQLSTATE 40001") {
			return Result{Skipped: true}, nil
		}
		return Result{}, err
	}

	return res, nil
}

// chainSQL chains the logs of a ledger between two ids, using the same compute_hash function as the
// SYNC trigger, starting from the hash of the previous log.
// $2 and $3 are bounds, every visible log between them is part of the batch (holes are skipped).
func chainSQL(bucket string) string {
	return `
with recursive todo as (
	select l as r, l.id as id, row_number() over (order by l.id) as rn
	from ` + bucket + `.logs l
	where l.ledger = ? and l.id >= ? and l.id <= ?
), chain(rn, id, hash) as (
	select t.rn, t.id, ` + bucket + `.compute_hash(?::bytea, t.r) from todo t where t.rn = 1
	union all
	select t.rn, t.id, ` + bucket + `.compute_hash(c.hash, t.r) from chain c join todo t on t.rn = c.rn + 1
)
update ` + bucket + `.logs set hash = chain.hash
from chain
where logs.ledger = ? and logs.id = chain.id and logs.hash is null`
}

// HashLedgerUntilCaughtUp calls HashLedger until the backlog is empty, blocked, or maxBatchesPerCycle batches were processed.
func (h *Hasher) HashLedgerUntilCaughtUp(ctx context.Context, l ledger.Ledger) (Result, error) {
	var (
		last  Result
		total int
	)
	for i := 0; i < maxBatchesPerCycle; i++ {
		res, err := h.HashLedger(ctx, l)
		if err != nil {
			return last, err
		}
		total += res.Hashed
		last = res
		if res.Skipped || res.Hashed < h.cfg.BatchSize {
			break
		}
	}
	last.Hashed = total
	return last, nil
}

// HexHash is a helper to display hashes.
func HexHash(b []byte) string { return hex.EncodeToString(b) }
