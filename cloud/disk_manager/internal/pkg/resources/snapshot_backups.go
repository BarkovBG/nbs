package resources

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// All manual requests share one durable queue slot. Source holds are acquired
// only after taking it, so intersecting graphs cannot deadlock each other.
func (s *storageYDB) snapshotBackupPlanTaskID(ctx context.Context, tx *persistence.Transaction) (string, error) {
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		select task_id from backup_plan where id = "snapshots";
 `, s.snapshotsPath))
	if err != nil {
		return "", err
	}
	defer res.Close()
	var taskID string
	if res.NextResultSet(ctx) && res.NextRow() {
		if err := res.ScanWithDefaults(&taskID); err != nil {
			return "", err
		}
	}
	return taskID, res.Err()
}

func (s *storageYDB) checkSnapshotBackupPlan(ctx context.Context, tx *persistence.Transaction, taskID string) error {
	actual, err := s.snapshotBackupPlanTaskID(ctx, tx)
	if err != nil {
		return err
	}
	if actual != taskID {
		return errors.NewInterruptExecutionError()
	}
	return nil
}

func (s *storageYDB) claimSnapshotBackupPlan(ctx context.Context, session *persistence.Session, taskID string) error {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	actual, err := s.snapshotBackupPlanTaskID(ctx, tx)
	if err != nil {
		return err
	}
	if actual != "" && actual != taskID {
		return errors.NewInterruptExecutionError()
	}
	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task as Utf8;
		upsert into backup_plan (id, task_id) values ("snapshots", $task);
 `, s.snapshotsPath), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *storageYDB) finishSnapshotBackupPlan(ctx context.Context, session *persistence.Session, taskID string) error {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	actual, err := s.snapshotBackupPlanTaskID(ctx, tx)
	if err != nil {
		return err
	}
	if actual != taskID {
		return tx.Commit(ctx)
	}
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task as Utf8;
		select id from snapshots view backup_plan_id_index where backup_plan_id = $task limit 1;
 `, s.snapshotsPath), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
	if err != nil {
		return err
	}
	defer res.Close()
	if res.NextResultSet(ctx) && res.NextRow() {
		return errors.NewInterruptExecutionError()
	}
	if err := res.Err(); err != nil {
		return err
	}
	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task as Utf8;
		delete from backup_plan where id = "snapshots" and task_id = $task;
 `, s.snapshotsPath), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Admission and Delete serialize on the source row. A held deleting ancestor
// may be admitted only while it is needed by a remaining descendant.
func (s *storageYDB) reserveSnapshotBackup(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	backupTaskID string,
	neededByDependents bool,
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
	if state.backupTaskID == backupTaskID {
		return state.toSnapshotMeta(), tx.Commit(ctx)
	}
	if state.backupCompleted || (state.status != snapshotStatusReady &&
		!(state.status == snapshotStatusDeleting && state.backupPlanID == backupTaskID && neededByDependents)) {
		return nil, tx.Commit(ctx)
	}
	// Only the plan that holds the source may accept its copy.
	if state.backupTaskID != "" || state.backupPlanID != backupTaskID {
		return nil, errors.NewInterruptExecutionError()
	}
	if err := s.checkSnapshotBackupPlan(ctx, tx, backupTaskID); err != nil {
		return nil, err
	}
	state.backupTaskID = backupTaskID
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

// A graph hold and Delete read/write the same source row. Once held, CP
// deletion cannot schedule DP deletion, so inherited chunk references remain.
func (s *storageYDB) holdSnapshotBackupPlan(
	ctx context.Context,
	session *persistence.Session,
	snapshotID, backupTaskID string,
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
	if state.backupPlanID == backupTaskID {
		return state.toSnapshotMeta(), tx.Commit(ctx)
	}
	if state.status != snapshotStatusReady || state.backupCompleted {
		return nil, tx.Commit(ctx)
	}
	if state.backupPlanID != "" || state.backupTaskID != "" {
		return nil, errors.NewInterruptExecutionError()
	}
	if err := s.checkSnapshotBackupPlan(ctx, tx, backupTaskID); err != nil {
		return nil, err
	}
	state.backupPlanID = backupTaskID
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

// backupTaskID is the durable CP task identity, not a user or a TTL lock.
// Repeating acquisition resumes its hold; stale cancellation cannot clear it.
func (s *storageYDB) ClaimSnapshotBackupPlan(ctx context.Context, backupTaskID string) error {
	if backupTaskID == "" {
		return errors.NewNonRetriableErrorf("empty snapshot backup task ID")
	}
	return s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		return s.claimSnapshotBackupPlan(ctx, session, backupTaskID)
	})
}

func (s *storageYDB) ReserveSnapshotBackup(
	ctx context.Context,
	snapshotID string,
	backupTaskID string,
	neededByDependents bool,
) (meta *SnapshotMeta, err error) {
	if backupTaskID == "" {
		return nil, errors.NewNonRetriableErrorf("empty snapshot backup task ID")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		meta, err = s.reserveSnapshotBackup(ctx, session, snapshotID, backupTaskID, neededByDependents)
		return err
	})
	return meta, err
}

func (s *storageYDB) HoldSnapshotBackupPlan(ctx context.Context, snapshotID, backupTaskID string) (meta *SnapshotMeta, err error) {
	if snapshotID == "" || backupTaskID == "" {
		return nil, errors.NewNonRetriableErrorf("empty snapshot backup plan task ID")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		meta, err = s.holdSnapshotBackupPlan(ctx, session, snapshotID, backupTaskID)
		return err
	})
	return meta, err
}

// Release the queue slot only after all saved graph holds have been released.
func (s *storageYDB) FinishSnapshotBackupPlan(ctx context.Context, backupTaskID string) error {
	if backupTaskID == "" {
		return errors.NewNonRetriableErrorf("empty snapshot backup task ID")
	}
	return s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		return s.finishSnapshotBackupPlan(ctx, session, backupTaskID)
	})
}

// A completed copy releases both holds after DP cleanup. A pruned plan node
// releases only its preparation hold. Stale owners cannot clear either hold.
func (s *storageYDB) ReleaseSnapshotBackup(ctx context.Context, snapshotID, backupTaskID string, completed bool) error {
	if snapshotID == "" || backupTaskID == "" {
		return errors.NewNonRetriableErrorf("empty snapshot backup task ID")
	}
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		declare $backup_task_id as Utf8;
		declare $completed as Bool;
		$owned = select id from snapshots where id = $id
		and (backup_task_id = $backup_task_id or backup_plan_id = $backup_task_id)
		and (COALESCE(backup_task_id, "") = "" or backup_task_id = $backup_task_id)
		and (COALESCE(backup_plan_id, "") = "" or backup_plan_id = $backup_task_id);
		update snapshots set backup_task_id = "", backup_plan_id = "",
		backup_completed = COALESCE(backup_completed, false) or ($completed and backup_task_id = $backup_task_id)
		where id in (select id from $owned);
		delete from backup_queue where snapshot_id in (select id from $owned);
    `, s.snapshotsPath), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)), persistence.ValueParam("$backup_task_id", persistence.UTF8Value(backupTaskID)), persistence.ValueParam("$completed", persistence.BoolValue(completed)))
	return err
}

////////////////////////////////////////////////////////////////////////////////

func createSnapshotBackupTables(ctx context.Context, folder string, db *persistence.YDBClient, dropUnusedColumns bool) error {
	return db.CreateOrAlterTable(ctx, folder, "backup_plan", persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithPrimaryKeyColumn("id"),
	), dropUnusedColumns)
}
