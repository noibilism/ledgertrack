package loghash

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
)

// MismatchReason says why a log was reported.
type MismatchReason string

const (
	// ReasonHashMismatch: the stored hash is not the hash of the log content chained to the previous stored hash.
	// The row was modified, or a row before it was removed or replaced.
	ReasonHashMismatch MismatchReason = "HASH_MISMATCH"
	// ReasonUnchainedBelowHead: a log without hash sits before the chain head. It was inserted after the chain
	// went past its id (backdated insert), or its hash was erased.
	ReasonUnchainedBelowHead MismatchReason = "UNCHAINED_BELOW_HEAD"
)

type Mismatch struct {
	ID       uint64         `json:"id"`
	Reason   MismatchReason `json:"reason"`
	Stored   string         `json:"stored,omitempty"`
	Expected string         `json:"expected,omitempty"`
}

type Tail struct {
	Count   int64      `json:"count"`
	FirstID *uint64    `json:"firstId,omitempty"`
	LastID  *uint64    `json:"lastId,omitempty"`
	Oldest  *time.Time `json:"oldestDate,omitempty"`
	// LagSeconds is the age of the oldest log of the tail.
	LagSeconds float64 `json:"lagSeconds"`
}

type AnchorCheck struct {
	ID       uint64 `json:"id"`
	Expected string `json:"expected"`
	Actual   string `json:"actual,omitempty"`
	Match    bool   `json:"match"`
}

type Report struct {
	Ledger string `json:"ledger"`
	// HashMode is the HASH_LOGS configuration of the ledger.
	HashMode string `json:"hashMode"`
	// Verified is the number of hashed logs whose hash was recomputed and matched.
	Verified int64 `json:"verified"`
	// FirstMismatch is the first (lowest id) problem found, nil when the chain is intact.
	FirstMismatch *Mismatch `json:"firstMismatch,omitempty"`
	// Mismatches is the number of problems found (the verifier keeps going after the first one).
	Mismatches int64   `json:"mismatches"`
	HeadID     *uint64 `json:"headId,omitempty"`
	HeadHash   string  `json:"headHash,omitempty"`
	// IDHoles is the number of ids missing in the sequence. Holes are normal (rolled back transactions).
	IDHoles int64 `json:"idHoles"`
	// Tail is the set of logs after the chain head, not covered by the chain yet.
	Tail   Tail         `json:"unhashedTail"`
	Anchor *AnchorCheck `json:"anchor,omitempty"`
}

// OK is true when no problem was found. A non empty unhashed tail is not a problem by itself: look at Tail.LagSeconds.
func (r Report) OK() bool {
	return r.Mismatches == 0 && (r.Anchor == nil || r.Anchor.Match)
}

type VerifyOptions struct {
	// PageSize is the number of logs read per query.
	PageSize int
	// AnchorID/AnchorHash: when set, the verifier also checks that the hash of the log AnchorID is AnchorHash (hex).
	// This is what detects a tampering where the attacker recomputed the whole chain after the modification.
	AnchorID   *uint64
	AnchorHash string
}

type verifyRow struct {
	ID       uint64
	Hash     []byte
	Expected []byte
}

type unhashedRow struct {
	ID   uint64
	Date time.Time
}

// Verify walks the logs of a ledger by id, recomputes every link of the chain with the compute_hash function
// and reports the problems found, plus the unhashed tail.
// Read only. It reads the whole log table of the ledger: run it against a replica for big ledgers.
//
// What it checks, for every hashed log in id order: hash == compute_hash(stored hash of the previous hashed log, log content).
// Modifying any log (content, date, idempotency key, type), removing a log, or swapping two logs makes the first
// affected link fail. An attacker who recomputes every following hash up to the head is not detected by this
// alone: that is what the AnchorID/AnchorHash check, fed from an external immutable copy of an old head, is for.
func Verify(ctx context.Context, db bun.IDB, bucketName, ledgerName, hashMode string, opts VerifyOptions) (*Report, error) {
	bucket, err := quoteBucket(bucketName)
	if err != nil {
		return nil, err
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 5000
	}

	report := &Report{Ledger: ledgerName, HashMode: hashMode}
	note := func(m Mismatch) {
		report.Mismatches++
		if report.FirstMismatch == nil || m.ID < report.FirstMismatch.ID {
			mm := m
			report.FirstMismatch = &mm
		}
	}

	// 1. The chain: hashed logs only.
	var (
		started bool
		lastID  uint64
		anchor  bool
	)
	for {
		var rows []verifyRow
		// The previous hashed row is read again (id >= lastID) so that lag() sees its stored hash. It is skipped below.
		err := db.NewRaw(`
select id, hash,
	`+bucket+`.compute_hash(lag(hash) over (order by id), l) as expected
from `+bucket+`.logs l
where ledger = ? and hash is not null and id >= ?
order by id
limit ?`, ledgerName, lastID, pageSize+1).Scan(ctx, &rows)
		if err != nil {
			return nil, fmt.Errorf("reading logs: %w", err)
		}
		if started && len(rows) > 0 {
			rows = rows[1:]
		}
		if len(rows) == 0 {
			break
		}
		started = true
		for _, r := range rows {
			if bytes.Equal(r.Hash, r.Expected) {
				report.Verified++
			} else {
				note(Mismatch{ID: r.ID, Reason: ReasonHashMismatch, Stored: HexHash(r.Hash), Expected: HexHash(r.Expected)})
			}
			id := r.ID
			report.HeadID = &id
			report.HeadHash = HexHash(r.Hash)
			if opts.AnchorID != nil && id == *opts.AnchorID {
				anchor = true
				report.Anchor = &AnchorCheck{ID: id, Expected: opts.AnchorHash, Actual: HexHash(r.Hash), Match: HexHash(r.Hash) == opts.AnchorHash}
			}
			lastID = r.ID
		}
	}
	if opts.AnchorID != nil && !anchor {
		report.Anchor = &AnchorCheck{ID: *opts.AnchorID, Expected: opts.AnchorHash, Match: false}
	}

	// 2. Logs without hash: the tail (after the head) and, if any, the anomalies (before the head).
	var unhashed []unhashedRow
	if hashMode != "DISABLED" {
		if err := db.NewRaw(`select id, date from `+bucket+`.logs where ledger = ? and hash is null order by id`, ledgerName).
			Scan(ctx, &unhashed); err != nil {
			return nil, fmt.Errorf("reading unhashed logs: %w", err)
		}
	}
	for _, u := range unhashed {
		id := u.ID
		if report.HeadID != nil && id < *report.HeadID {
			note(Mismatch{ID: id, Reason: ReasonUnchainedBelowHead})
			continue
		}
		d := u.Date
		report.Tail.Count++
		if report.Tail.FirstID == nil {
			report.Tail.FirstID = &id
			report.Tail.Oldest = &d
		}
		report.Tail.LastID = &id
	}
	if report.Tail.Oldest != nil {
		var lag float64
		if err := db.NewRaw(`select extract(epoch from ((now() at time zone 'utc') - ?::timestamp))`,
			report.Tail.Oldest.Format("2006-01-02 15:04:05.999999")).Scan(ctx, &lag); err != nil {
			return nil, err
		}
		if lag > 0 {
			report.Tail.LagSeconds = lag
		}
	}

	// 3. Holes in the id sequence (informational).
	var span struct {
		Count int64
		Min   uint64
		Max   uint64
	}
	if err := db.NewRaw(`select count(*) as count, coalesce(min(id), 0) as min, coalesce(max(id), 0) as max from `+bucket+`.logs where ledger = ?`, ledgerName).
		Scan(ctx, &span.Count, &span.Min, &span.Max); err != nil {
		return nil, err
	}
	if span.Count > 0 {
		report.IDHoles = int64(span.Max-span.Min) + 1 - span.Count
	}

	return report, nil
}

// Status is the cheap view of a chain: no log is recomputed.
type Status struct {
	Ledger   string  `json:"ledger"`
	HashMode string  `json:"hashMode"`
	HeadID   *uint64 `json:"headId,omitempty"`
	HeadHash string  `json:"headHash,omitempty"`
	// Unhashed counts the logs after the head.
	Unhashed   int64   `json:"unhashed"`
	LagSeconds float64 `json:"lagSeconds"`
	// MaxID is the highest log id of the ledger.
	MaxID *uint64 `json:"maxId,omitempty"`
}

// GetStatus reads the head of the chain and the size/age of the backlog.
func GetStatus(ctx context.Context, db bun.IDB, l ledger.Ledger) (*Status, error) {
	bucket, err := quoteBucket(l.Bucket)
	if err != nil {
		return nil, err
	}
	st := &Status{Ledger: l.Name, HashMode: l.Features["HASH_LOGS"]}

	var (
		headID   *uint64
		headHash []byte
	)
	rows, err := db.QueryContext(ctx, `select id, hash from `+bucket+`.logs where ledger = ? and hash is not null order by id desc limit 1`, l.Name)
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		var id uint64
		if err := rows.Scan(&id, &headHash); err != nil {
			_ = rows.Close()
			return nil, err
		}
		headID = &id
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	st.HeadID = headID
	st.HeadHash = HexHash(headHash)

	var head uint64
	if headID != nil {
		head = *headID
	}
	var lag *float64
	if err := db.NewRaw(`select count(*), extract(epoch from ((now() at time zone 'utc') - min(date))), max(id) from `+bucket+`.logs where ledger = ? and id > ?`, l.Name, head).
		Scan(ctx, &st.Unhashed, &lag, &st.MaxID); err != nil {
		return nil, err
	}
	if lag != nil && *lag > 0 {
		st.LagSeconds = *lag
	}
	if st.MaxID == nil {
		st.MaxID = headID
	}
	return st, nil
}
