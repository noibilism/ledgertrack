package transferstest

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"
	libtime "github.com/formancehq/go-libs/v3/time"

	ledgerinternal "github.com/formancehq/ledger/internal"
	channelmodels "github.com/formancehq/ledger/internal/channels/models"
	"github.com/formancehq/ledger/internal/machine"
	"github.com/formancehq/ledger/internal/transfers"
)

// FakeLedgers is a set of in-memory ledgers that enforce the two properties the
// orchestrator relies on: references are unique per ledger (a duplicate returns
// the existing transaction) and non-overdraft sources need funds.
type FakeLedgers struct {
	mu       sync.Mutex
	balances map[string]map[string]*big.Int // ledger -> account|asset -> balance
	byRef    map[string]map[string]*ledgerinternal.Transaction
	nextID   map[string]uint64
	applied  map[string]int // ledger|reference -> times actually committed
	posts    int

	// BeforeCommit runs before a new transaction is committed. Returning an
	// error simulates a failure where nothing was written.
	BeforeCommit func(m transfers.Movement) error
	// AfterCommit runs after a new transaction is committed and before Post
	// returns. Returning an error simulates "committed, response lost"; a panic
	// simulates the process dying at that point.
	AfterCommit func(m transfers.Movement) error
}

var _ transfers.Ledgers = (*FakeLedgers)(nil)

func NewFakeLedgers() *FakeLedgers {
	return &FakeLedgers{
		balances: map[string]map[string]*big.Int{},
		byRef:    map[string]map[string]*ledgerinternal.Transaction{},
		nextID:   map[string]uint64{},
		applied:  map[string]int{},
	}
}

func key(account, asset string) string { return account + "|" + asset }

// Fund credits an account from thin air (test setup).
func (f *FakeLedgers) Fund(ledger, account, asset string, amount int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.add(ledger, key(account, asset), amount)
}

func (f *FakeLedgers) add(ledger, k string, amount int64) {
	if f.balances[ledger] == nil {
		f.balances[ledger] = map[string]*big.Int{}
	}
	if f.balances[ledger][k] == nil {
		f.balances[ledger][k] = new(big.Int)
	}
	f.balances[ledger][k].Add(f.balances[ledger][k], big.NewInt(amount))
}

// Balance returns the balance of an account.
func (f *FakeLedgers) Balance(ledger, account, asset string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b := f.balances[ledger][key(account, asset)]; b != nil {
		return b.Int64()
	}
	return 0
}

// Applied returns how many times a reference was really committed on a ledger.
func (f *FakeLedgers) Applied(ledger, reference string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied[ledger+"|"+reference]
}

// PostCalls returns the number of Post calls received.
func (f *FakeLedgers) PostCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.posts
}

// Preexisting commits a transaction as if an earlier (legacy) request did it.
func (f *FakeLedgers) Preexisting(ctx context.Context, m transfers.Movement) {
	saved := f.BeforeCommit
	savedAfter := f.AfterCommit
	f.BeforeCommit, f.AfterCommit = nil, nil
	_, _ = f.Post(ctx, m)
	f.BeforeCommit, f.AfterCommit = saved, savedAfter
}

func (f *FakeLedgers) Post(_ context.Context, m transfers.Movement) (*transfers.Posted, error) {
	f.mu.Lock()
	f.posts++
	if existing := f.byRef[m.Ledger][m.Reference]; existing != nil {
		tx := *existing
		f.mu.Unlock()
		return &transfers.Posted{Tx: tx, Replayed: true}, nil
	}
	before := f.BeforeCommit
	f.mu.Unlock()

	if before != nil {
		if err := before(m); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	// Another goroutine may have committed the same reference meanwhile.
	if existing := f.byRef[m.Ledger][m.Reference]; existing != nil {
		tx := *existing
		f.mu.Unlock()
		return &transfers.Posted{Tx: tx, Replayed: true}, nil
	}
	if m.Source != "world" && !m.UnboundedOverdraft {
		bal := f.balances[m.Ledger][key(m.Source, m.Asset)]
		if bal == nil || bal.Cmp(big.NewInt(m.Amount)) < 0 {
			f.mu.Unlock()
			return nil, &transfers.RejectedError{Err: machine.NewErrInsufficientFund("insufficient funds in %s", m.Source)}
		}
	}
	f.add(m.Ledger, key(m.Source, m.Asset), -m.Amount)
	f.add(m.Ledger, key(m.Destination, m.Asset), m.Amount)

	f.nextID[m.Ledger]++
	id := f.nextID[m.Ledger]
	postings := ledgerinternal.Postings{{
		Source: m.Source, Destination: m.Destination, Asset: m.Asset, Amount: big.NewInt(m.Amount),
	}}
	vol := ledgerinternal.PostCommitVolumes{}
	for _, acc := range []string{m.Source, m.Destination} {
		b := f.balances[m.Ledger][key(acc, m.Asset)]
		in, out := new(big.Int), new(big.Int)
		if b.Sign() >= 0 {
			in.Set(b)
		} else {
			out.Neg(b)
		}
		if vol[acc] == nil {
			vol[acc] = ledgerinternal.VolumesByAssets{}
		}
		vol[acc][m.Asset] = ledgerinternal.Volumes{Input: in, Output: out}
	}
	md := metadata.Metadata{}
	for k, v := range m.Metadata {
		md[k] = v
	}
	tx := &ledgerinternal.Transaction{
		TransactionData: ledgerinternal.TransactionData{
			Postings:  postings,
			Metadata:  md,
			Timestamp: libtime.New(fixedTime),
			Reference: m.Reference,
		},
		ID:                &id,
		PostCommitVolumes: vol,
	}
	if f.byRef[m.Ledger] == nil {
		f.byRef[m.Ledger] = map[string]*ledgerinternal.Transaction{}
	}
	f.byRef[m.Ledger][m.Reference] = tx
	f.applied[m.Ledger+"|"+m.Reference]++
	after := f.AfterCommit
	out := *tx
	f.mu.Unlock()

	if after != nil {
		if err := after(m); err != nil {
			return nil, err
		}
	}
	return &transfers.Posted{Tx: out}, nil
}

// FakeFees is a transfers.FeeRecorder that stores one row per reference, like
// the SQL recorder, and counts calls.
type FakeFees struct {
	mu    sync.Mutex
	rows  map[string]*channelmodels.ChannelFeeRecord
	calls int
	// Fail makes Record return an error while it returns true.
	Fail func() bool
}

var _ transfers.FeeRecorder = (*FakeFees)(nil)

func NewFakeFees() *FakeFees {
	return &FakeFees{rows: map[string]*channelmodels.ChannelFeeRecord{}}
}

func (f *FakeFees) Record(_ context.Context, rec *channelmodels.ChannelFeeRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.Fail != nil && f.Fail() {
		return fmt.Errorf("fee store unavailable")
	}
	if _, ok := f.rows[rec.Reference]; !ok {
		c := *rec
		f.rows[rec.Reference] = &c
	}
	return nil
}

func (f *FakeFees) Rows(reference string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[reference]; ok {
		return 1
	}
	return 0
}

func (f *FakeFees) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *FakeFees) Row(reference string) *channelmodels.ChannelFeeRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rows[reference]
}

var fixedTime = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
