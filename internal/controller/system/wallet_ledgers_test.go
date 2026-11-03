package system

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
)

type createLedgerRecorder struct {
	Driver
	created []*ledger.Ledger
}

func (r *createLedgerRecorder) CreateLedger(_ context.Context, l *ledger.Ledger) error {
	r.created = append(r.created, l)
	return nil
}

func TestIsWalletLedger(t *testing.T) {
	for name, want := range map[string]bool{
		"ledgertrack":  true,
		"channels-NGN": true,
		"revenue-USD":  true,
		"other":        false,
		"ledgertrack2": false,
		"my-channels-": false,
	} {
		require.Equal(t, want, IsWalletLedger(name), name)
	}
}

func TestValidateWalletLedgerHashMode(t *testing.T) {
	for _, ok := range []string{"", "SYNC", "ASYNC"} {
		require.NoError(t, ValidateWalletLedgerHashMode(ok))
	}
	for _, ko := range []string{"DISABLED", "async", "x"} {
		require.Error(t, ValidateWalletLedgerHashMode(ko))
	}
}

func TestCreateLedgerWalletHashMode(t *testing.T) {
	create := func(mode string, enableFeatures bool, name string, conf ledger.Configuration) (*ledger.Ledger, error) {
		rec := &createLedgerRecorder{}
		ctrl := NewDefaultController(rec, nil, nil,
			WithEnableFeatures(enableFeatures),
			WithWalletLedgerHashMode(mode),
		)
		err := ctrl.CreateLedger(context.Background(), name, conf)
		if err != nil {
			return nil, err
		}
		require.Len(t, rec.created, 1)
		return rec.created[0], nil
	}

	t.Run("default is unchanged", func(t *testing.T) {
		l, err := create("", false, "ledgertrack", ledger.Configuration{})
		require.NoError(t, err)
		require.Equal(t, "SYNC", l.Features[features.FeatureHashLogs])
	})

	t.Run("wallet ledgers get ASYNC without experimental features", func(t *testing.T) {
		for _, name := range []string{"ledgertrack", "channels-NGN", "revenue-USD"} {
			l, err := create("ASYNC", false, name, ledger.Configuration{})
			require.NoError(t, err, name)
			require.Equal(t, "ASYNC", l.Features[features.FeatureHashLogs], name)
			// everything else stays at its default
			require.Equal(t, features.DefaultFeatures.With(features.FeatureHashLogs, "ASYNC"), l.Features, name)
		}
	})

	t.Run("other ledgers are untouched", func(t *testing.T) {
		l, err := create("ASYNC", false, "customer-ledger", ledger.Configuration{})
		require.NoError(t, err)
		require.Equal(t, "SYNC", l.Features[features.FeatureHashLogs])
	})

	t.Run("the shared default feature set is not mutated", func(t *testing.T) {
		_, err := create("ASYNC", false, "ledgertrack", ledger.NewDefaultConfiguration())
		require.NoError(t, err)
		require.Equal(t, "SYNC", features.DefaultFeatures[features.FeatureHashLogs])
	})

	t.Run("an explicit choice is not overridden and still needs the experimental gate", func(t *testing.T) {
		_, err := create("ASYNC", false, "ledgertrack", ledger.Configuration{Features: features.FeatureSet{features.FeatureHashLogs: "DISABLED"}})
		require.ErrorIs(t, err, ErrExperimentalFeaturesDisabled)

		l, err := create("ASYNC", true, "ledgertrack", ledger.Configuration{Features: features.FeatureSet{features.FeatureHashLogs: "DISABLED"}})
		require.NoError(t, err)
		require.Equal(t, "DISABLED", l.Features[features.FeatureHashLogs])
	})

	t.Run("explicit non default features of other kinds still need the gate", func(t *testing.T) {
		_, err := create("ASYNC", false, "ledgertrack", ledger.Configuration{Features: features.FeatureSet{features.FeatureMovesHistory: "OFF"}})
		require.ErrorIs(t, err, ErrExperimentalFeaturesDisabled)
	})
}
