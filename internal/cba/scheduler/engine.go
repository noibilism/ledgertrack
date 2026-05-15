package scheduler

import (
	"context"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/query"

	ledgerinternal "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/cba/models"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	currencyregistry "github.com/formancehq/ledger/internal/currency"
	"github.com/formancehq/ledger/internal/machine/vm"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type PostingEngine interface {
	AvailableBalance(context.Context, models.Account) (int64, error)
	Credit(context.Context, models.Account, int64, string, map[string]string) error
	Debit(context.Context, models.Account, int64, string, map[string]string) error
	RecordFeeIncome(context.Context, string, string, int64, map[string]string) error
	RecordInterestExpense(context.Context, string, string, int64, map[string]string) error
}

type ledgerPostingEngine struct {
	system systemcontroller.Controller
	cfg    LedgerPostingConfig
}

func NewLedgerPostingEngine(system systemcontroller.Controller, cfg LedgerPostingConfig) PostingEngine {
	return &ledgerPostingEngine{
		system: system,
		cfg:    cfg,
	}
}

func (e *ledgerPostingEngine) AvailableBalance(ctx context.Context, account models.Account) (int64, error) {
	l, err := e.system.GetLedgerController(ctx, e.cfg.LedgerName)
	if err != nil {
		return 0, err
	}

	balancesQ := storagecommon.ResourceQuery[ledgerstore.GetAggregatedVolumesOptions]{
		Opts:    ledgerstore.GetAggregatedVolumesOptions{},
		Builder: query.Match("address", walletAvailableAddress(account.WalletID, account.Currency)),
	}
	balancesMap, err := l.GetAggregatedBalances(ctx, balancesQ)
	if err != nil {
		return 0, err
	}

	asset := fmt.Sprintf("%s/2", account.Currency)
	if amount, ok := balancesMap[asset]; ok && amount != nil {
		return amount.Int64(), nil
	}
	return 0, nil
}

func (e *ledgerPostingEngine) Credit(ctx context.Context, account models.Account, amount int64, reference string, txnMetadata map[string]string) error {
	accountUser := walletAvailableAddress(account.WalletID, account.Currency)
	accountSystem := fmt.Sprintf("system:control:%s", account.Currency)
	script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s allowing unbounded overdraft
			destination = @%s
		)
	`, account.Currency, amount, accountSystem, accountUser)

	return e.createTransaction(ctx, e.cfg.LedgerName, script, reference, txnMetadata)
}

func (e *ledgerPostingEngine) Debit(ctx context.Context, account models.Account, amount int64, reference string, txnMetadata map[string]string) error {
	accountUser := walletAvailableAddress(account.WalletID, account.Currency)
	accountSystem := fmt.Sprintf("system:control:%s", account.Currency)
	script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s
			destination = @%s
		)
	`, account.Currency, amount, accountUser, accountSystem)

	return e.createTransaction(ctx, e.cfg.LedgerName, script, reference, txnMetadata)
}

func (e *ledgerPostingEngine) RecordFeeIncome(ctx context.Context, currency, reference string, amount int64, txnMetadata map[string]string) error {
	revenueLedgerName := fmt.Sprintf("revenue-%s", currency)
	script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @world
			destination = @%s
		)
	`, currency, amount, e.cfg.FeeIncomeAccount)

	return e.createTransaction(ctx, revenueLedgerName, script, reference, txnMetadata)
}

func (e *ledgerPostingEngine) RecordInterestExpense(ctx context.Context, currency, reference string, amount int64, txnMetadata map[string]string) error {
	revenueLedgerName := fmt.Sprintf("revenue-%s", currency)
	script := fmt.Sprintf(`
		send [%s/2 %d] (
			source = @%s allowing unbounded overdraft
			destination = @world
		)
	`, currency, amount, e.cfg.InterestExpenseAccount)

	return e.createTransaction(ctx, revenueLedgerName, script, reference, txnMetadata)
}

func (e *ledgerPostingEngine) createTransaction(ctx context.Context, ledgerName, script, reference string, txnMetadata map[string]string) error {
	l, err := e.system.GetLedgerController(ctx, ledgerName)
	if err != nil {
		return err
	}

	runMetadata := metadata.Metadata{}
	for key, value := range txnMetadata {
		runMetadata[key] = value
	}

	params := ledgercontroller.Parameters[ledgercontroller.CreateTransaction]{
		Input: ledgercontroller.CreateTransaction{
			RunScript: vm.RunScript{
				Script:    vm.Script{Plain: script},
				Reference: reference,
				Metadata:  runMetadata,
			},
			Runtime: ledgerinternal.RuntimeMachine,
		},
	}

	_, _, _, err = l.CreateTransaction(ctx, params)
	if isDuplicateReferenceError(err) {
		return nil
	}
	return err
}

func walletAvailableAddress(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:available", walletID, currency)
}

func isDuplicateReferenceError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "conflict") || strings.Contains(message, "duplicate reference")
}

func decimalToMinorHalfUp(amount decimal.Decimal, currency string) (int64, error) {
	definition, ok := currencyregistry.Lookup(currency)
	if !ok {
		definition = currencyregistry.Definition{Precision: 2, Enabled: true}
	}
	minor := amount.Shift(int32(definition.Precision)).Round(0)
	return minor.IntPart(), nil
}
