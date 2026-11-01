package loghash_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/loghash"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
)

// These tests need a Postgres server (no docker): LEDGERTRACK_TEST_PG_URI=postgres://user@host:port/postgres?sslmode=disable
// They are skipped when the variable is not set.
const pgEnv = "LEDGERTRACK_TEST_PG_URI"

var dbCounter atomic.Int64

type env struct {
	db     *bun.DB
	driver *driver.Driver
	ctx    context.Context
}

func newEnv(t *testing.T) *env {
	t.Helper()
	uri := os.Getenv(pgEnv)
	if uri == "" {
		t.Skipf("%s not set", pgEnv)
	}
	ctx := logging.TestingContext()

	admin, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{DatabaseSourceName: uri})
	require.NoError(t, err)
	name := fmt.Sprintf("lh_%d_%d", time.Now().UnixNano(), dbCounter.Add(1))
	_, err = admin.ExecContext(ctx, `create database `+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `drop database if exists `+name+` with (force)`)
		_ = admin.Close()
	})

	u, err := url.Parse(uri)
	require.NoError(t, err)
	u.Path = "/" + name
	db, err := bunconnect.OpenSQLDB(ctx, bunconnect.ConnectionOptions{DatabaseSourceName: u.String()})
	require.NoError(t, err)
	db.SetMaxOpenConns(20)
	t.Cleanup(func() { _ = db.Close() })
	db.Dialect().Tables().Register(&ledger.Log{}, &ledger.Ledger{})

	// pgcrypto provides public.digest
	_, err = db.ExecContext(ctx, `create extension if not exists pgcrypto`)
	require.NoError(t, err)

	d := driver.New(db, ledgerstore.NewFactory(db), bucket.NewDefaultFactory(), systemstore.NewStoreFactory())
	require.NoError(t, d.Initialize(ctx))

	return &env{db: db, driver: d, ctx: ctx}
}

func (e *env) createLedger(t *testing.T, name, hashMode string) ledger.Ledger {
	t.Helper()
	l, err := ledger.New(name, ledger.Configuration{
		Bucket:   "bkt",
		Features: features.DefaultFeatures.With(features.FeatureHashLogs, hashMode),
	})
	require.NoError(t, err)
	_, err = e.driver.CreateLedger(e.ctx, l)
	require.NoError(t, err)
	got, err := loghash.GetLedger(e.ctx, e.db, name)
	require.NoError(t, err)
	return *got
}

// insertLog inserts a log the way the ledger does, with an explicit id.
func insertLog(ctx context.Context, db bun.IDB, ledgerName string, id uint64, payload string) error {
	_, err := db.ExecContext(ctx, `
insert into bkt.logs (ledger, id, type, data, memento, date, idempotency_key)
values (?, ?, 'NEW_TRANSACTION', ?::jsonb, convert_to(?, 'UTF8'), ?::timestamp, ?)`,
		ledgerName, id, payload, payload, "2026-01-01 00:00:00."+fmt.Sprintf("%06d", id), fmt.Sprintf("ik-%d", id))
	return err
}

func payloadFor(id uint64) string {
	return fmt.Sprintf(`{"transaction":{"id":%d,"postings":[{"source":"world","destination":"a:%d","asset":"USD/2","amount":%d}]}}`, id, id, id*100)
}

func (e *env) insertLogs(t *testing.T, ledgerName string, from, to uint64) {
	t.Helper()
	for id := from; id <= to; id++ {
		require.NoError(t, insertLog(e.ctx, e.db, ledgerName, id, payloadFor(id)))
	}
}

func (e *env) hashes(t *testing.T, ledgerName string) map[uint64]string {
	t.Helper()
	rows, err := e.db.QueryContext(e.ctx, `select id, coalesce(encode(hash, 'hex'), '') from bkt.logs where ledger = ? order by id`, ledgerName)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	ret := map[uint64]string{}
	for rows.Next() {
		var (
			id uint64
			h  string
		)
		require.NoError(t, rows.Scan(&id, &h))
		ret[id] = h
	}
	require.NoError(t, rows.Err())
	return ret
}
