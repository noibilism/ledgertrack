package v2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/internal/cba/services"
	"github.com/formancehq/ledger/internal/machine/script/compiler"
)

func mustCompile(t *testing.T, script string) {
	t.Helper()
	_, err := compiler.Compile(script)
	require.NoError(t, err, "script:\n%s", script)
}

func twoLineCharge() *services.ChargePlan {
	return &services.ChargePlan{Lines: []services.ChargeLine{
		{Address: "gl:fees", AmountMinor: 100},
		{Address: "gl:tax", AmountMinor: 7},
	}}
}

func TestReserveCreditScriptCompiles(t *testing.T) {
	for name, plan := range map[string]*services.CreditPlan{
		"split":        {ReserveMinor: 15000, AvailableMinor: 85000},
		"full hold":    {ReserveMinor: 100000, AvailableMinor: 0},
		"no hold left": {ReserveMinor: 0, AvailableMinor: 100000},
	} {
		t.Run(name, func(t *testing.T) {
			script := reserveCreditScript("GHS/2", "system:control:GHS", "u:available", "u:lien", true, plan)
			mustCompile(t, script)
			mustCompile(t, appendSends(script, chargeSends("GHS/2", "system:control:GHS", twoLineCharge(), true)))
		})
	}
}

func TestReserveCreditSplitIsOneSend(t *testing.T) {
	script := reserveCreditScript("GHS/2", "s", "a", "l", true, &services.CreditPlan{ReserveMinor: 30, AvailableMinor: 70})
	require.Equal(t, 1, countSends(script))
}

func TestReserveDebitScriptCompiles(t *testing.T) {
	for name, plan := range map[string]*services.DrawPlan{
		"drawn and remainder": {DrawnMinor: 5000, RemainderMinor: 15000},
		"fully covered":       {DrawnMinor: 3000, RemainderMinor: 0},
		"nothing held":        {DrawnMinor: 0, RemainderMinor: 3000},
	} {
		t.Run(name, func(t *testing.T) {
			script := reserveDebitScript("KES/2", "u:available", "u:lien", "system:control:KES", plan)
			mustCompile(t, script)
			mustCompile(t, appendSends(script, chargeSends("KES/2", "u:available", twoLineCharge(), false)))
		})
	}
}

// A whitespace-only line between statements is a syntax error, so joining a plain debit and a charge
// has to normalise whitespace.
func TestAppendSendsToPlainScript(t *testing.T) {
	plain := fmt.Sprintf(`
		send [%s %d] (
			source = @%s
			destination = @%s
		)
	`, "KES/2", 10000, "u:available", "system:control:KES")
	mustCompile(t, plain)
	mustCompile(t, appendSends(plain, chargeSends("KES/2", "u:available", twoLineCharge(), false)))
	require.Equal(t, plain, appendSends(plain, ""))
}

func countSends(script string) int {
	n := 0
	for i := 0; i+4 <= len(script); i++ {
		if script[i:i+4] == "send" {
			n++
		}
	}
	return n
}
