package transfers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/query"

	ledgerinternal "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

// ErrReferenceMismatch means a reference is already used on a ledger by a
// transaction that is not the one this leg would create. It is never retried:
// overwriting or ignoring it could hide a real conflict.
var ErrReferenceMismatch = errors.New("reference already used by a different transaction")

// RejectedError wraps an error for which the write is known not to have been
// committed (insufficient funds, validation, serialization, ...). On the first
// leg this lets the transfer fail cleanly instead of staying recoverable.
// Unwrap keeps errors.Is working so the HTTP layer maps it as before.
type RejectedError struct{ Err error }

func (e *RejectedError) Error() string { return e.Err.Error() }
func (e *RejectedError) Unwrap() error { return e.Err }

// Movement is one single-posting ledger transaction.
type Movement struct {
	Ledger         string
	Reference      string
	IdempotencyKey string
	Source         string
	Destination    string
	Asset          string
	Amount         int64
	// UnboundedOverdraft lets the source go negative (channel accounts).
	UnboundedOverdraft bool
	Metadata           map[string]string
}

// Posted is the outcome of Ledgers.Post. Replayed is true when the reference
// already existed and Tx is the existing transaction.
type Posted struct {
	Tx       ledgerinternal.Transaction
	Replayed bool
}

// Ledgers is the only thing the orchestrator needs from the ledger layer.
// Post must be idempotent on (Ledger, Reference): a duplicate reference returns
// the existing transaction with Replayed=true instead of an error.
type Ledgers interface {
	Post(ctx context.Context, m Movement) (*Posted, error)
}

// AssetName builds the ledger asset for a currency from the currency registry
// precision, e.g. NGN/2 or USDT/6. Unknown currencies default to precision 2,
// like the CBA scheduler does.
func AssetName(currency string) string {
	precision := 2
	if def, ok := currencyregistry.Lookup(currency); ok {
		precision = def.Precision
	}
	return fmt.Sprintf("%s/%d", currency, precision)
}

// SystemLedgers implements Ledgers on top of the system controller.
type SystemLedgers struct {
	system systemcontroller.Controller
}

func NewSystemLedgers(system systemcontroller.Controller) *SystemLedgers {
	return &SystemLedgers{system: system}
}

func (l *SystemLedgers) Post(ctx context.Context, m Movement) (*Posted, error) {
	ctrl, err := l.system.GetLedgerController(ctx, m.Ledger)
	if err != nil {
		return nil, classify(err)
	}

	source := "@" + m.Source
	if m.UnboundedOverdraft {
		source += " allowing unbounded overdraft"
	}
	script := fmt.Sprintf("send [%s %d] (\n\tsource = %s\n\tdestination = @%s\n)", m.Asset, m.Amount, source, m.Destination)

	md := map[string]string{}
	for k, v := range m.Metadata {
		md[k] = v
	}
	params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
		IdempotencyKey: m.IdempotencyKey,
		Input: ledgercontroller.CreateTransaction{
			RunScript: vm.RunScript{
				Script:    vm.Script{Plain: script},
				Reference: m.Reference,
				Metadata:  md,
			},
			Runtime: ledgerinternal.RuntimeMachine,
		},
	}
	_, created, _, err := ctrl.CreateTransaction(ctx, params)
	if err == nil {
		return &Posted{Tx: created.Transaction}, nil
	}
	if !isDuplicateReference(err) {
		return nil, classify(err)
	}

	existing, err := ctrl.GetTransaction(ctx, storagecommon.ResourceQuery[any]{
		Builder: query.Match("reference", m.Reference),
	})
	if err != nil {
		return nil, fmt.Errorf("reading existing transaction for reference %q on %s: %w", m.Reference, m.Ledger, err)
	}
	return &Posted{Tx: *existing, Replayed: true}, nil
}

func isDuplicateReference(err error) bool {
	if errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "duplicate reference")
}

// classify wraps errors that prove nothing was committed.
func classify(err error) error {
	switch {
	case errors.Is(err, &ledgercontroller.ErrInsufficientFunds{}),
		errors.Is(err, ledgercontroller.ErrCompilationFailed{}),
		errors.Is(err, &ledgercontroller.ErrMetadataOverride{}),
		errors.Is(err, ledgercontroller.ErrSchemaValidationError{}),
		errors.Is(err, ledgercontroller.ErrSchemaNotSpecified{}),
		errors.Is(err, ledgercontroller.ErrSchemaNotFound{}),
		errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}),
		errors.Is(err, ledgercontroller.ErrIdempotencyKeyConflict{}),
		errors.Is(err, ledgercontroller.ErrNotFound),
		errors.Is(err, postgres.ErrSerialization),
		errors.Is(err, postgres.ErrTooManyClient{}),
		strings.Contains(strings.ToLower(err.Error()), "insufficient fund"):
		return &RejectedError{Err: err}
	default:
		return err
	}
}
