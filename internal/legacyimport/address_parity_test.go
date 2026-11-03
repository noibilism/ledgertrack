package legacyimport

import (
	"testing"

	"github.com/formancehq/ledger/internal/walletaddr"
)

// The importer must post to the addresses the server uses for the same wallet.
func TestAddressesMatchServer(t *testing.T) {
	for _, tc := range [][2]string{
		{"client-CL-2026-000001-NGN", "NGN"},
		{"client-CL-2026-000001-RESERVE-USD", "USD"},
		{"client-CL-2026-000013-CUR-USD-001", "USD"},
		{"NGN", "NGN"},
	} {
		if got, want := availableAddress(tc[0], tc[1]), walletaddr.Available(tc[0], tc[1]); got != want {
			t.Errorf("available %v: importer %q, server %q", tc, got, want)
		}
		if got, want := lienAddress(tc[0], tc[1]), walletaddr.Lien(tc[0], tc[1]); got != want {
			t.Errorf("lien %v: importer %q, server %q", tc, got, want)
		}
	}
}
