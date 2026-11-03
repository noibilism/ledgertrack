package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/transfers"
	"github.com/formancehq/ledger/internal/transfers/transferstest"
)

type recovererStub struct {
	passes []transfers.RecoveryStats
	err    error
	calls  int
}

func (s *recovererStub) RecoverBatch(context.Context) (transfers.RecoveryStats, error) {
	i := s.calls
	s.calls++
	if i < len(s.passes) {
		return s.passes[i], s.err
	}
	return transfers.RecoveryStats{}, s.err
}

func TestTransferRecoveryRunnerDrainsBacklogThenStops(t *testing.T) {
	t.Parallel()

	stub := &recovererStub{passes: []transfers.RecoveryStats{{Claimed: 10}, {Claimed: 10}, {Claimed: 3}}}
	runner := NewTransferRecoveryRunner(logging.Testing(), stub, TransferRecoveryRunnerConfig{
		Schedule: cron.Every(time.Minute), BatchSize: 10,
	})

	require.NoError(t, runner.run(context.Background()))
	require.Equal(t, 3, stub.calls, "keeps going while batches are full, stops on a partial one")
}

func TestTransferRecoveryRunnerBoundsAPassAndSurfacesErrors(t *testing.T) {
	t.Parallel()

	always := &recovererStub{}
	for i := 0; i < maxRecoveryPassesPerTick+10; i++ {
		always.passes = append(always.passes, transfers.RecoveryStats{Claimed: 5})
	}
	runner := NewTransferRecoveryRunner(logging.Testing(), always, TransferRecoveryRunnerConfig{
		Schedule: cron.Every(time.Minute), BatchSize: 5,
	})
	require.NoError(t, runner.run(context.Background()))
	require.Equal(t, maxRecoveryPassesPerTick, always.calls)

	failing := &recovererStub{err: errors.New("db down")}
	runner = NewTransferRecoveryRunner(logging.Testing(), failing, TransferRecoveryRunnerConfig{
		Schedule: cron.Every(time.Minute), BatchSize: 5,
	})
	require.ErrorContains(t, runner.run(context.Background()), "db down")
}

// The runner drives the real orchestrator: an interrupted transfer is finished
// by a tick once the grace period has elapsed.
func TestTransferRecoveryRunnerFinishesInterruptedTransfer(t *testing.T) {
	t.Parallel()

	store := transferstest.NewMemoryStore(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC))
	ledgers := transferstest.NewFakeLedgers()
	fees := transferstest.NewFakeFees()
	ledgers.Fund("ledgertrack", "users:u1:wallets:NGN:available", "NGN/2", 5_000)
	cfg := transfers.DefaultConfig()
	svc := transfers.NewService(store, transfers.NewLedgerExecutor(ledgers, fees), cfg, logging.Testing())

	// Simulate a crash after the principal: the channel ledger refuses and the
	// handler gives up, leaving the transfer pending.
	ledgers.BeforeCommit = func(m transfers.Movement) error {
		if m.Ledger == "channels-NGN" {
			return errors.New("channel ledger down")
		}
		return nil
	}
	out, err := svc.Execute(context.Background(), transfers.Input{
		Reference: "ref-runner", UserLedger: "ledgertrack", WalletID: "u1-NGN", Currency: "NGN",
		Kind: transfers.KindDebit, RequestedAmount: 1000, Amount: 1000,
		ChannelID: "ch1", ChannelAmount: 900, UserFee: 100, NetRevenue: 100,
	})
	require.NoError(t, err)
	require.Equal(t, transfers.StatusPending, out.Status)
	ledgers.BeforeCommit = nil

	runner := NewTransferRecoveryRunner(logging.Testing(), svc, TransferRecoveryRunnerConfig{
		Schedule: cron.Every(time.Minute), BatchSize: cfg.BatchSize,
	})

	require.NoError(t, runner.run(context.Background()))
	tr, _ := store.GetByReference(context.Background(), "ref-runner")
	require.Equal(t, transfers.StateUserPosted, tr.State, "inside the grace period nothing is touched")

	store.Advance(cfg.Grace + time.Second)
	require.NoError(t, runner.run(context.Background()))
	tr, _ = store.GetByReference(context.Background(), "ref-runner")
	require.Equal(t, transfers.StateCompleted, tr.State)
	require.Equal(t, 1, ledgers.Applied("ledgertrack", "ref-runner"))
	require.Equal(t, 1, ledgers.Applied("channels-NGN", "ref-runner"))
	require.Equal(t, 1, fees.Rows("ref-runner"))
}

// The recovery runner needs *transfers.Service from the graph; this guards the
// module wiring shared by `worker` and `serve` (embedded worker) without a DB.
func TestTransferRecoveryModuleWiring(t *testing.T) {
	t.Parallel()

	cfg := TransferRecoveryRunnerConfig{Schedule: cron.Every(time.Minute), BatchSize: 10}
	require.NoError(t, fx.ValidateApp(
		fx.Provide(
			func() *bun.DB { return nil },
			func() systemcontroller.Controller { return nil },
			func() logging.Logger { return logging.Testing() },
		),
		transfers.NewFXModule(transfers.DefaultConfig()),
		NewTransferRecoveryRunnerModule(cfg),
	))
}
