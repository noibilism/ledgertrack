package ledger

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v3/pointer"

	ledger "github.com/formancehq/ledger/internal"
)

// Regression test for the pool self-deadlock: the accounts upsert used to go through the
// controller's own (pooled, non transactional) store while the write transaction was open, so each
// write held one connection and then waited for a second one. With as many concurrent writes as pooled
// connections, every writer waited for a connection held by another writer.
//
// Here the controller store and the transactional store are two distinct mocks: any call on the
// controller store other than BeginTX fails the test.
func TestCreateTransactionUsesOnlyTheTransactionStore(t *testing.T) {
	t.Parallel()

	gc := gomock.NewController(t)

	poolStore := NewMockStore(gc) // must only be used to open the transaction
	txStore := NewMockStore(gc)
	numscriptRuntime := NewMockNumscriptRuntime(gc)
	parser := NewMockNumscriptParser(gc)

	l := NewDefaultController(ledger.Ledger{}, poolStore, parser, NewMockNumscriptParser(gc), NewMockNumscriptParser(gc))

	runScript := RunScript{}
	parser.EXPECT().Parse(runScript.Plain).Return(numscriptRuntime, nil)

	poolStore.EXPECT().BeginTX(gomock.Any(), nil).Return(txStore, &bun.Tx{}, nil)

	numscriptRuntime.EXPECT().
		Execute(gomock.Any(), txStore, runScript.Vars).
		Return(&NumscriptExecutionResult{
			Postings: ledger.Postings{ledger.NewPosting("world", "bank", "USD", big.NewInt(100))},
		}, nil)

	txStore.EXPECT().FindLatestSchemaVersion(gomock.Any()).Return(nil, nil)
	txStore.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil)
	txStore.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any()).Return(nil)
	txStore.EXPECT().InsertLog(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, log *ledger.Log) error {
		log.ID = pointer.For(uint64(0))
		return nil
	})
	txStore.EXPECT().Commit(gomock.Any()).Return(nil)

	_, _, _, err := l.CreateTransaction(context.Background(), Parameters[CreateTransaction]{
		Input: CreateTransaction{RunScript: runScript},
	})
	require.NoError(t, err)
}
