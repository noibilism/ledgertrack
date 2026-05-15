package v2

import currencyregistry "github.com/formancehq/ledger/internal/currency"

func init() {
	currencyregistry.SetDefinitions(map[string]currencyregistry.Definition{
		"USD": {Precision: 2, Enabled: true},
		"EUR": {Precision: 2, Enabled: true},
		"BTC": {Precision: 8, Enabled: true},
		"NGN": {Precision: 2, Enabled: true},
		"GHS": {Precision: 2, Enabled: true},
		"KES": {Precision: 2, Enabled: true},
		"ZMW": {Precision: 2, Enabled: true},
	})
}
