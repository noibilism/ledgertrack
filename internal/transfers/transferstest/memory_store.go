// Package transferstest holds in-memory fakes used to test the transfers
// orchestrator, the recovery runner and the HTTP handlers without Postgres or a
// real ledger.
package transferstest

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/formancehq/ledger/internal/transfers"
)

// MemoryStore is an in-memory transfers.Store with the same semantics as the
// SQL store (lease, grace/backoff, conditional updates) and a controllable
// clock. The store and the SQL store run the same contract test.
type MemoryStore struct {
	mu   sync.Mutex
	rows map[uuid.UUID]*transfers.Transfer
	now  time.Time
}

var _ transfers.Store = (*MemoryStore)(nil)

func NewMemoryStore(start time.Time) *MemoryStore {
	return &MemoryStore{rows: map[uuid.UUID]*transfers.Transfer{}, now: start}
}

// Advance moves the store clock forward.
func (m *MemoryStore) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

// Snapshot returns a copy of every row.
func (m *MemoryStore) Snapshot() []*transfers.Transfer {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*transfers.Transfer, 0, len(m.rows))
	for _, r := range m.rows {
		out = append(out, r.Copy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (m *MemoryStore) byReference(ref string) *transfers.Transfer {
	for _, r := range m.rows {
		if r.Reference == ref {
			return r
		}
	}
	return nil
}

func (m *MemoryStore) lease(r *transfers.Transfer, owner string, lease time.Duration) {
	until := m.now.Add(lease)
	r.LeaseOwner = &owner
	r.LeaseUntil = &until
	r.UpdatedAt = m.now
}

func (m *MemoryStore) leaseFree(r *transfers.Transfer) bool {
	return r.LeaseUntil == nil || r.LeaseUntil.Before(m.now)
}

func (m *MemoryStore) Create(_ context.Context, t *transfers.Transfer, owner string, lease time.Duration) (*transfers.Transfer, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing := m.byReference(t.Reference); existing != nil {
		return existing.Copy(), false, nil
	}
	r := t.Copy()
	r.State = transfers.StateCreated
	r.Attempts = 1
	r.CreatedAt = m.now
	m.lease(r, owner, lease)
	m.rows[r.ID] = r
	return r.Copy(), true, nil
}

func (m *MemoryStore) Get(_ context.Context, id uuid.UUID) (*transfers.Transfer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rows[id]
	if !ok {
		return nil, transfers.ErrNotFound
	}
	return r.Copy(), nil
}

func (m *MemoryStore) GetByReference(_ context.Context, reference string) (*transfers.Transfer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.byReference(reference)
	if r == nil {
		return nil, transfers.ErrNotFound
	}
	return r.Copy(), nil
}

func (m *MemoryStore) Rearm(_ context.Context, id uuid.UUID, t *transfers.Transfer, owner string, lease time.Duration) (*transfers.Transfer, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rows[id]
	if !ok || r.State != transfers.StateFailed {
		return nil, false, nil
	}
	n := t.Copy()
	n.ID = r.ID
	n.State = transfers.StateCreated
	n.Attempts = 1
	n.CreatedAt = m.now
	m.lease(n, owner, lease)
	m.rows[id] = n
	return n.Copy(), true, nil
}

func (m *MemoryStore) Claim(_ context.Context, id uuid.UUID, owner string, opts transfers.ClaimOptions) (*transfers.Transfer, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rows[id]
	if !ok || r.State.Terminal() || !m.leaseFree(r) || r.UpdatedAt.After(m.now.Add(-opts.MinIdle)) {
		return nil, false, nil
	}
	if opts.CountAttempt {
		r.Attempts++
	}
	m.lease(r, owner, opts.Lease)
	return r.Copy(), true, nil
}

func (m *MemoryStore) ClaimBatch(_ context.Context, owner string, opts transfers.RecoveryOptions) ([]*transfers.Transfer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var due []*transfers.Transfer
	for _, r := range m.rows {
		if r.State.Terminal() || !m.leaseFree(r) {
			continue
		}
		backoff := time.Duration(math.Min(
			float64(opts.Grace)*math.Pow(2, math.Max(float64(r.Attempts-1), 0)),
			float64(opts.MaxBackoff),
		))
		if r.UpdatedAt.Before(m.now.Add(-backoff)) {
			due = append(due, r)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].UpdatedAt.Before(due[j].UpdatedAt) })
	if opts.Limit > 0 && len(due) > opts.Limit {
		due = due[:opts.Limit]
	}
	out := make([]*transfers.Transfer, 0, len(due))
	for _, r := range due {
		r.Attempts++
		m.lease(r, owner, opts.Lease)
		out = append(out, r.Copy())
	}
	return out, nil
}

func (m *MemoryStore) owned(id uuid.UUID, from transfers.State, owner string) (*transfers.Transfer, error) {
	r, ok := m.rows[id]
	if !ok || r.LeaseOwner == nil || *r.LeaseOwner != owner {
		return nil, transfers.ErrLeaseLost
	}
	if from != "" && r.State != from {
		return nil, transfers.ErrLeaseLost
	}
	return r, nil
}

func (m *MemoryStore) Save(_ context.Context, t *transfers.Transfer, from transfers.State, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(t.ID, from, owner)
	if err != nil {
		return err
	}
	r.State = t.State
	r.UserTxID = cp(t.UserTxID)
	r.ChannelTxID = cp(t.ChannelTxID)
	r.RevenueTxID = cp(t.RevenueTxID)
	r.Result = t.Copy().Result
	r.LastError = t.LastError
	r.UpdatedAt = m.now
	if t.State == transfers.StateCompleted {
		at := m.now
		r.CompletedAt = &at
		r.LeaseOwner = nil
		r.LeaseUntil = nil
	}
	return nil
}

func (m *MemoryStore) Release(_ context.Context, id uuid.UUID, owner string, lastError string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(id, "", owner)
	if err != nil {
		return err
	}
	if r.State.Terminal() {
		return transfers.ErrLeaseLost
	}
	r.LeaseOwner, r.LeaseUntil = nil, nil
	r.LastError = lastError
	r.UpdatedAt = m.now
	return nil
}

func (m *MemoryStore) Finish(_ context.Context, id uuid.UUID, owner string, state transfers.State, lastError string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.owned(id, "", owner)
	if err != nil {
		return err
	}
	if r.State.Terminal() {
		return transfers.ErrLeaseLost
	}
	r.State = state
	r.LastError = lastError
	r.LeaseOwner, r.LeaseUntil = nil, nil
	r.UpdatedAt = m.now
	return nil
}

func cp(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
