package loghash

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
)

// GetLedger reads a ledger by name.
func GetLedger(ctx context.Context, db bun.IDB, name string) (*ledger.Ledger, error) {
	l := &ledger.Ledger{}
	err := db.NewSelect().Model(l).Column("*").Where("name = ? and deleted_at is null", name).Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("ledger %q not found", name)
		}
		return nil, err
	}
	return l, nil
}

// SetHashMode switches an existing ledger between SYNC and ASYNC log hashing.
//
// SYNC -> ASYNC drops the BEFORE INSERT trigger that hashes logs (the Hasher takes over from the current head, the
// chain stays continuous and has the same format) and records the new mode.
// ASYNC -> SYNC refuses unless the chain is complete (nothing left to hash), then recreates the trigger.
// Both take a short ACCESS EXCLUSIVE lock on the logs table (trigger DDL): do it in a quiet period.
//
// The processes serving the ledger cache its configuration: restart them (all replicas) after the switch.
func SetHashMode(ctx context.Context, db *bun.DB, name, mode string) (previous string, err error) {
	if mode != "SYNC" && mode != "ASYNC" {
		return "", fmt.Errorf("unsupported mode %q: only SYNC and ASYNC can be set with this command", mode)
	}
	err = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		l := &ledger.Ledger{}
		if err := tx.NewSelect().Model(l).Column("*").Where("name = ? and deleted_at is null", name).For("update").Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("ledger %q not found", name)
			}
			return err
		}
		previous = l.Features[features.FeatureHashLogs]
		if previous == mode {
			return nil
		}
		if previous == "DISABLED" {
			return fmt.Errorf("ledger %q has HASH_LOGS=DISABLED: switching from DISABLED is not supported", name)
		}
		bucket, err := quoteBucket(l.Bucket)
		if err != nil {
			return err
		}
		trigger := fmt.Sprintf(`"set_log_hash_%d"`, l.ID)

		switch mode {
		case "ASYNC":
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`drop trigger if exists %s on %s.logs`, trigger, bucket)); err != nil {
				return fmt.Errorf("dropping trigger: %w", err)
			}
		case "SYNC":
			// Creating the trigger waits for in flight writers and blocks new ones until commit, so the count below is exact.
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(
				`create trigger %s before insert on %s.logs for each row when (new.ledger = '%s') execute procedure %s.set_log_hash()`,
				trigger, bucket, l.Name, bucket)); err != nil {
				return fmt.Errorf("creating trigger: %w", err)
			}
			var pending int64
			if err := tx.NewRaw(`select count(*) from `+bucket+`.logs where ledger = ? and hash is null`, l.Name).Scan(ctx, &pending); err != nil {
				return err
			}
			if pending > 0 {
				return fmt.Errorf("%d logs are not part of the hash chain yet: wait for the hasher to catch up (see `hashchain status`) and retry", pending)
			}
		}

		_, err = tx.NewUpdate().Model((*ledger.Ledger)(nil)).
			Set("features = features || jsonb_build_object(?::text, ?::text)", features.FeatureHashLogs, mode).
			Where("name = ?", name).
			Exec(ctx)
		return err
	})
	return previous, err
}
