package legacyimport

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Metadata keys written on every object created by the import.
const (
	metaLegacySourceID   = "legacy_source_id"
	metaLegacyImport     = "legacy_import"
	metaLegacyAccountNum = "legacy_account_number"
	metaLegacyClientSrc  = "legacy_client_source_id"
	metaLegacyAcctSrc    = "legacy_account_source_id"
	metaLegacyExternalID = "legacy_external_id"
	metaLegacyReference  = "legacy_reference"
	metaLegacyCreatedAt  = "legacy_created_at"
	metaLegacyOpenedAt   = "legacy_opened_at"
	metaLegacyAsOf       = "legacy_as_of"
	metaKYCSource        = "kyc_source"
	// Verifier recorded on the KYC verification.
	KYCVerifier = "legacy-import"
	// ProductMarker is appended to the description of imported products, which
	// have no metadata field.
	productMarkerPrefix = "[legacy_source_id="
)

// Item kinds tracked in the state file.
const (
	KindProduct = "product"
	KindClient  = "client"
	KindAccount = "account"
	KindBalance = "balance"
	KindLien    = "lien"
	// Final-status passes.
	KindAccountFinal = "account_final"
	KindClientFinal  = "client_final"
	KindProductFinal = "product_final"
)

// Item statuses.
const (
	StatusInflight = "inflight" // a non-idempotent create was started; outcome unknown
	StatusDone     = "done"
	StatusFailed   = "failed"
	StatusConflict = "conflict"
	StatusBlocked  = "blocked"
)

// ItemState is the checkpoint of one imported object.
type ItemState struct {
	Kind      string            `json:"kind"`
	SourceID  string            `json:"source_id"`
	Status    string            `json:"status"`
	RemoteID  string            `json:"remote_id,omitempty"`
	Detail    string            `json:"detail,omitempty"`
	Error     string            `json:"error,omitempty"`
	Extra     map[string]string `json:"extra,omitempty"`
	UpdatedAt time.Time         `json:"updated_at"`
	// CreateStarted is set before a non-idempotent create is sent and never
	// cleared: it is the only record that an unmarked orphan on the server
	// may be ours after a crash.
	CreateStarted bool `json:"create_started,omitempty"`
}

// State is the resume checkpoint. It is advisory: the server is the source of
// truth and every step re-checks it, so a lost or stale state file costs
// time, never correctness. The one thing it adds is the record that a
// non-idempotent create was started (StatusInflight), which allows adopting an
// orphan after a crash.
type State struct {
	Version int                   `json:"version"`
	Target  string                `json:"target"`
	Runs    int                   `json:"runs"`
	Started time.Time             `json:"started"`
	Updated time.Time             `json:"updated"`
	Items   map[string]*ItemState `json:"items"`

	mu    sync.Mutex
	path  string
	dirty bool
	lock  func()
}

func stateKey(kind, id string) string { return kind + ":" + id }

// LoadState reads path (creating an empty state if missing) and locks it for
// the lifetime of the process. The target must match the one stored.
func LoadState(path, target string) (*State, error) {
	unlock, err := lockFile(path + ".lock")
	if err != nil {
		return nil, err
	}
	st := &State{Version: 1, Target: target, Items: map[string]*ItemState{}, path: path, lock: unlock, Started: time.Now().UTC()}
	data, err := os.ReadFile(path) //nolint:gosec // path comes from the operator/CLI, not from request input
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		unlock()
		return nil, err
	default:
		if err := json.Unmarshal(data, st); err != nil {
			unlock()
			return nil, fmt.Errorf("state file %s is corrupt: %w", path, err)
		}
		if st.Target != target {
			unlock()
			return nil, fmt.Errorf("state file %s belongs to target %q, not %q: use a separate --state file per environment", path, st.Target, target)
		}
		if st.Items == nil {
			st.Items = map[string]*ItemState{}
		}
	}
	st.path, st.lock = path, unlock
	return st, nil
}

// ReadStateReadOnly loads a state file without locking (for the status command).
func ReadStateReadOnly(path string) (*State, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path comes from the operator/CLI, not from request input
	if err != nil {
		return nil, err
	}
	st := &State{}
	if err := json.Unmarshal(data, st); err != nil {
		return nil, fmt.Errorf("state file %s is corrupt: %w", path, err)
	}
	if st.Items == nil {
		st.Items = map[string]*ItemState{}
	}
	return st, nil
}

// Get returns a copy of the item state, or nil.
func (s *State) Get(kind, id string) *ItemState {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := s.Items[stateKey(kind, id)]
	if it == nil {
		return nil
	}
	cp := *it
	if it.Extra != nil {
		cp.Extra = map[string]string{}
		for k, v := range it.Extra {
			cp.Extra[k] = v
		}
	}
	return &cp
}

// Update mutates (creating if needed) an item under the state lock.
func (s *State) Update(kind, id string, fn func(*ItemState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := stateKey(kind, id)
	it := s.Items[k]
	if it == nil {
		it = &ItemState{Kind: kind, SourceID: id}
		s.Items[k] = it
	}
	fn(it)
	it.UpdatedAt = time.Now().UTC()
	s.dirty = true
}

// Set records a status with optional detail.
func (s *State) Set(kind, id, status, detail string) {
	s.Update(kind, id, func(it *ItemState) {
		it.Status = status
		it.Detail = detail
		if status == StatusInflight {
			it.CreateStarted = true
		}
		if status == StatusDone {
			it.Error = ""
		}
	})
}

// Fail records a failure.
func (s *State) Fail(kind, id, status string, err error) {
	s.Update(kind, id, func(it *ItemState) {
		it.Status = status
		if err != nil {
			it.Error = err.Error()
		}
	})
}

// Flush writes the state atomically if it changed.
func (s *State) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dirty {
		return nil
	}
	s.Updated = time.Now().UTC()
	data, err := json.MarshalIndent(s, "", " ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".import-state-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	s.dirty = false
	return nil
}

// Close flushes and releases the lock.
func (s *State) Close() error {
	err := s.Flush()
	if s.lock != nil {
		s.lock()
		s.lock = nil
	}
	return err
}

// BeginRun increments the run counter.
func (s *State) BeginRun() {
	s.mu.Lock()
	s.Runs++
	s.dirty = true
	s.mu.Unlock()
}

// Counts returns kind -> status -> count.
func (s *State) Counts() map[string]map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]int{}
	for _, it := range s.Items {
		if out[it.Kind] == nil {
			out[it.Kind] = map[string]int{}
		}
		out[it.Kind][it.Status]++
	}
	return out
}

// Problems returns the failed/conflict/blocked/inflight items sorted by key.
func (s *State) Problems() []ItemState {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ItemState
	for _, it := range s.Items {
		if it.Status != StatusDone {
			out = append(out, *it)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].SourceID < out[j].SourceID
	})
	return out
}
