// Package walletaddr builds the ledger addresses of a wallet.
//
// The wallet API (/wallets/{walletID}/...) reads the currency from the text after the last
// hyphen of the wallet id and keeps the balance under the rest. The account API
// (/accounts/{id}/...) and the importer must land on the same addresses, otherwise a migrated
// balance and a live wallet operation end up in two separate accounts.
package walletaddr

import (
	"fmt"
	"strings"
)

// User returns the user part of the address of a wallet: the wallet id without its
// "-{currency}" suffix. A wallet id that does not end with its currency is returned as is.
func User(walletID, currency string) string {
	if currency != "" {
		if trimmed, ok := strings.CutSuffix(walletID, "-"+currency); ok && trimmed != "" {
			return trimmed
		}
	}
	return walletID
}

// Available is the address holding the spendable balance of a wallet.
func Available(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:available", User(walletID, currency), currency)
}

// Lien is the address holding the amounts locked by liens on a wallet.
func Lien(walletID, currency string) string {
	return fmt.Sprintf("users:%s:wallets:%s:lien", User(walletID, currency), currency)
}
