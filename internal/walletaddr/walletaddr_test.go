package walletaddr

import "testing"

func TestAddresses(t *testing.T) {
	for _, tc := range []struct {
		wallet, ccy, available, lien string
	}{
		// Same addresses the wallet API derives from the wallet id.
		{"client-CL-2026-000001-NGN", "NGN", "users:client-CL-2026-000001:wallets:NGN:available", "users:client-CL-2026-000001:wallets:NGN:lien"},
		{"client-CL-2026-000001-RESERVE-USD", "USD", "users:client-CL-2026-000001-RESERVE:wallets:USD:available", "users:client-CL-2026-000001-RESERVE:wallets:USD:lien"},
		// A wallet id that does not end with its currency is kept whole.
		{"client-CL-2026-000013-CUR-USD-001", "USD", "users:client-CL-2026-000013-CUR-USD-001:wallets:USD:available", "users:client-CL-2026-000013-CUR-USD-001:wallets:USD:lien"},
		{"NGN", "NGN", "users:NGN:wallets:NGN:available", "users:NGN:wallets:NGN:lien"},
	} {
		if got := Available(tc.wallet, tc.ccy); got != tc.available {
			t.Errorf("Available(%q,%q) = %q, want %q", tc.wallet, tc.ccy, got, tc.available)
		}
		if got := Lien(tc.wallet, tc.ccy); got != tc.lien {
			t.Errorf("Lien(%q,%q) = %q, want %q", tc.wallet, tc.ccy, got, tc.lien)
		}
	}
}
