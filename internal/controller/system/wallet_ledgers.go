package system

import (
	"fmt"
	"strings"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
)

const (
	// WalletLedgerName is the ledger the wallet and CBA account routes write to (see ledgertrackOnly in internal/api/v2).
	WalletLedgerName = "ledgertrack"
	// ChannelsLedgerPrefix and RevenueLedgerPrefix are the per currency ledgers written by the wallet and CBA routes.
	ChannelsLedgerPrefix = "channels-"
	RevenueLedgerPrefix  = "revenue-"
)

// IsWalletLedger tells if a ledger name is one of the ledgers of the wallet/CBA write path.
func IsWalletLedger(name string) bool {
	return name == WalletLedgerName ||
		strings.HasPrefix(name, ChannelsLedgerPrefix) ||
		strings.HasPrefix(name, RevenueLedgerPrefix)
}

// ValidateWalletLedgerHashMode validates the value of --wallet-ledger-hash-mode.
// An empty value means "unchanged": ledgers keep the default configuration (HASH_LOGS=SYNC).
// DISABLED is deliberately not accepted: it removes the audit chain entirely.
func ValidateWalletLedgerHashMode(mode string) error {
	switch mode {
	case "", "SYNC", "ASYNC":
		return nil
	default:
		return fmt.Errorf("invalid wallet ledger hash mode %q: expected SYNC or ASYNC (or empty)", mode)
	}
}

// applyWalletLedgerHashMode sets HASH_LOGS on the creation of a wallet ledger.
//
// It only applies when the caller did not choose HASH_LOGS itself (explicitHashLogs false), which is the case for every
// creation done by the server (wallet, channels, revenue, CBA code paths create their ledgers with the default
// configuration). A caller that explicitly sets features still goes through the --experimental-features gate.
//
// It does not go through that gate because the operator opted in with the server flag, only a restricted set of
// ledgers is affected, and only between the two modes that both keep a hash chain.
// It never changes an existing ledger: HASH_LOGS is fixed at creation (use `hashchain set-mode` for that).
func applyWalletLedgerHashMode(mode, name string, explicitHashLogs bool, configuration *ledger.Configuration) {
	if mode == "" || explicitHashLogs || !IsWalletLedger(name) {
		return
	}
	// copy: the map may be the shared features.DefaultFeatures
	configuration.Features = configuration.Features.With(features.FeatureHashLogs, mode)
}
