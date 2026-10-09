package resources

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) reserveSnapshotBackup(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	ownerID string,
) (*SnapshotMeta, error) {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	res, err := tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $id as Utf8;
        select * from snapshots where id = $id;
    `, s.snapshotsPath), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	states, err := scanSnapshotStates(ctx, res)
	if err != nil {
		return nil, err
	}
	if res.Err() != nil {
		return nil, res.Err()
	}
	if len(states) == 0 {
		return nil, tx.Commit(ctx)
	}
	state := states[0]
	// An accepted copy survives Delete.
	if state.backupTaskID == ownerID {
		return state.toSnapshotMeta(), tx.Commit(ctx)
	}
	if state.status != snapshotStatusReady || state.backupCompleted {
		return nil, tx.Commit(ctx)
	}
	if state.backupTaskID != "" {
		return nil, errors.NewInterruptExecutionError()
	}
	state.backupTaskID = ownerID
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $states as List<%v>;
        upsert into snapshots select * from AS_TABLE($states);
    `, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())))
	if err != nil {
		return nil, err
	}
	return state.toSnapshotMeta(), tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) ReserveSnapshotBackup(
	ctx context.Context,
	snapshotID string,
	ownerID string,
) (meta *SnapshotMeta, err error) {
	if ownerID == "" {
		return nil, errors.NewNonRetriableErrorf("empty snapshot backup owner")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		meta, err = s.reserveSnapshotBackup(ctx, session, snapshotID, ownerID)
		return err
	})
	return meta, err
}

// Completion is recorded and the CP barrier is released after DP has stopped
// all local work and released its own hold. Stale owners cannot clear a retry.
func (s *storageYDB) ReleaseSnapshotBackup(ctx context.Context, snapshotID, ownerID string, completed bool) error {
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $id as Utf8;
        declare $owner as Utf8;
        declare $completed as Bool;
        $owned = select id from snapshots where id = $id and backup_task_id = $owner;
        update snapshots set backup_task_id = "",
            backup_completed = COALESCE(backup_completed, false) or $completed
            where id in (select id from $owned);
        delete from backup_queue where snapshot_id in (select id from $owned);
    `, s.snapshotsPath), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)), persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)), persistence.ValueParam("$completed", persistence.BoolValue(completed)))
	return err
}
