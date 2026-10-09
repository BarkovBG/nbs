package resources

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type SnapshotBackupFolder struct {
	FolderID   string
	Generation string
}

type snapshotBackupFolderState struct {
	generation  string
	enabled     bool
	coordinator string
	activeOwner string
	activePlan  string
}

func (s *storageYDB) getSnapshotBackupFolderTx(
	ctx context.Context,
	tx *persistence.Transaction,
	folderID string,
) (*snapshotBackupFolderState, error) {
	res, err := tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        select generation, enabled, coordinator_task_id, active_owner, active_plan
        from backup_folders where folder_id = $folder;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if !res.NextResultSet(ctx) || !res.NextRow() {
		return nil, res.Err()
	}
	var state snapshotBackupFolderState
	err = res.ScanNamed(
		persistence.OptionalWithDefault("generation", &state.generation),
		persistence.OptionalWithDefault("enabled", &state.enabled),
		persistence.OptionalWithDefault("coordinator_task_id", &state.coordinator),
		persistence.OptionalWithDefault("active_owner", &state.activeOwner),
		persistence.OptionalWithDefault("active_plan", &state.activePlan),
	)
	if err != nil {
		return nil, err
	}
	return &state, res.Err()
}

// Admission and Delete serialize on the source row. A held deleting ancestor
// may be admitted only while it is needed by a remaining descendant.
func (s *storageYDB) reserveSnapshotBackup(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	ownerID string,
	coordinatorID string,
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
	// An accepted copy survives Delete or disabling its folder.
	if state.backupTaskID == ownerID {
		return state.toSnapshotMeta(), tx.Commit(ctx)
	}
	if state.backupCompleted || (state.status != snapshotStatusReady &&
		!(state.status == snapshotStatusDeleting && state.backupPlanID == ownerID && neededByDependents)) {
		return nil, tx.Commit(ctx)
	}
	if state.backupTaskID != "" || (state.backupPlanID != "" && state.backupPlanID != ownerID) {
		return nil, errors.NewInterruptExecutionError()
	}
	folder, err := s.getSnapshotBackupFolderTx(ctx, tx, state.folderID)
	if err != nil {
		return nil, err
	}
	if folder == nil || !folder.enabled || folder.coordinator != coordinatorID {
		return nil, tx.Commit(ctx)
	}
	if (folder.activeOwner != "" && folder.activeOwner != ownerID) ||
		(folder.activePlan != "" && folder.activePlan != ownerID) {
		return nil, errors.NewInterruptExecutionError()
	}
	state.backupTaskID = ownerID
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $states as List<%v>;
        declare $folder as Utf8;
        declare $owner as Utf8;
        upsert into snapshots select * from AS_TABLE($states);
        update backup_folders set active_owner = $owner where folder_id = $folder;
    `, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
		persistence.ValueParam("$folder", persistence.UTF8Value(state.folderID)),
		persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)))
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
	snapshotID, ownerID, coordinatorID string,
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
	if state.backupPlanID == ownerID {
		return state.toSnapshotMeta(), tx.Commit(ctx)
	}
	if state.status != snapshotStatusReady || state.backupCompleted {
		return nil, tx.Commit(ctx)
	}
	if state.backupPlanID != "" || state.backupTaskID != "" {
		return nil, errors.NewInterruptExecutionError()
	}
	folder, err := s.getSnapshotBackupFolderTx(ctx, tx, state.folderID)
	if err != nil {
		return nil, err
	}
	if folder == nil || !folder.enabled || folder.coordinator != coordinatorID {
		return nil, tx.Commit(ctx)
	}
	if folder.activePlan != "" && folder.activePlan != ownerID {
		return nil, errors.NewInterruptExecutionError()
	}
	if folder.activeOwner != "" && folder.activeOwner != ownerID {
		return nil, errors.NewInterruptExecutionError()
	}
	state.backupPlanID = ownerID
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $states as List<%v>;
        declare $folder as Utf8;
        declare $owner as Utf8;
        upsert into snapshots select * from AS_TABLE($states);
        update backup_folders set active_plan = $owner where folder_id = $folder;
    `, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
		persistence.ValueParam("$folder", persistence.UTF8Value(state.folderID)),
		persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)))
	if err != nil {
		return nil, err
	}
	return state.toSnapshotMeta(), tx.Commit(ctx)
}

func (s *storageYDB) finishSnapshotBackupPlan(ctx context.Context, session *persistence.Session, folderID, ownerID string) error {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	folder, err := s.getSnapshotBackupFolderTx(ctx, tx, folderID)
	if err != nil {
		return err
	}
	if folder == nil || folder.activePlan != ownerID {
		return tx.Commit(ctx)
	}
	if folder.activeOwner != "" {
		return errors.NewInterruptExecutionError()
	}
	res, err := tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $owner as Utf8;
        select count(*) from snapshots view folder_id_index
            where folder_id = $folder and backup_plan_id = $owner;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)))
	if err != nil {
		return err
	}
	defer res.Close()
	if !res.NextResultSet(ctx) || !res.NextRow() {
		if res.Err() != nil {
			return res.Err()
		}
		return errors.NewInterruptExecutionError()
	}
	var count uint64
	if err := res.ScanWithDefaults(&count); err != nil {
		return err
	}
	if res.Err() != nil {
		return res.Err()
	}
	if count != 0 {
		return errors.NewInterruptExecutionError()
	}
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $owner as Utf8;
        update backup_folders set active_plan = ""
            where folder_id = $folder and active_plan = $owner;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *storageYDB) claimSnapshotBackupFolder(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	generation string,
	taskID string,
) (bool, error) {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	state, err := s.getSnapshotBackupFolderTx(ctx, tx, folderID)
	if err != nil {
		return false, err
	}
	if state == nil || !state.enabled || state.generation != generation {
		return false, tx.Commit(ctx)
	}
	if state.coordinator != "" && state.coordinator != taskID {
		return false, errors.NewInterruptExecutionError()
	}
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $task as Utf8;
        update backup_folders set coordinator_task_id = $task where folder_id = $folder;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *storageYDB) setSnapshotBackupFolder(
	ctx context.Context,
	session *persistence.Session,
	folderID string,
	generation string,
	enabled bool,
) error {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	state, err := s.getSnapshotBackupFolderTx(ctx, tx, folderID)
	if err != nil {
		return err
	}
	if !enabled {
		if state == nil || (generation != "" && generation != state.generation) {
			return tx.Commit(ctx)
		}
		// Retain ownership until the old coordinator has finished its child.
		_, err = tx.Execute(ctx, fmt.Sprintf(`
            --!syntax_v1
            pragma TablePathPrefix = "%v";
            declare $folder as Utf8;
            update backup_folders set enabled = false where folder_id = $folder;
        `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)))
	} else {
		if state != nil {
			if state.enabled {
				return tx.Commit(ctx)
			}
			if state.coordinator != "" || state.activeOwner != "" || state.activePlan != "" {
				return errors.NewInterruptExecutionError()
			}
			if state.generation == generation {
				return errors.NewNonRetriableErrorf("re-enabling snapshot backup requires a new generation")
			}
		}
		_, err = tx.Execute(ctx, fmt.Sprintf(`
            --!syntax_v1
            pragma TablePathPrefix = "%v";
            declare $folder as Utf8;
            declare $generation as Utf8;
            upsert into backup_folders (folder_id, generation, enabled)
            values ($folder, $generation, true);
        `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$generation", persistence.UTF8Value(generation)))
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) ListSnapshotBackupFolders(ctx context.Context) ([]SnapshotBackupFolder, error) {
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        select folder_id, generation from backup_folders
        where COALESCE(enabled, false) order by folder_id;
    `, s.snapshotsPath))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var folders []SnapshotBackupFolder
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var f SnapshotBackupFolder
			err = res.ScanNamed(persistence.OptionalWithDefault("folder_id", &f.FolderID), persistence.OptionalWithDefault("generation", &f.Generation))
			if err != nil {
				return nil, err
			}
			folders = append(folders, f)
		}
	}
	return folders, res.Err()
}

func (s *storageYDB) ListSnapshotBackupCandidates(
	ctx context.Context,
	folderID string,
	afterID string,
	limit int,
) ([]string, error) {
	if folderID == "" || limit <= 0 || limit > 1000 {
		return nil, errors.NewNonRetriableErrorf("invalid snapshot backfill page")
	}
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $after as Utf8;
        declare $ready as Int64;
        declare $limit as Uint64;
        select id from snapshots view folder_id_index
        where folder_id = $folder and id > $after
            and status = $ready and not COALESCE(backup_completed, false)
        order by id limit $limit;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
		persistence.ValueParam("$after", persistence.UTF8Value(afterID)),
		persistence.ValueParam("$ready", persistence.Int64Value(int64(snapshotStatusReady))), persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var ids []string
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			if err = res.ScanWithDefaults(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
	}
	return ids, res.Err()
}

func (s *storageYDB) ReserveSnapshotBackup(
	ctx context.Context,
	snapshotID string,
	ownerID string,
	coordinatorID string,
	neededByDependents bool,
) (meta *SnapshotMeta, err error) {
	if ownerID == "" || coordinatorID == "" {
		return nil, errors.NewNonRetriableErrorf("empty snapshot backup owner or coordinator")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		meta, err = s.reserveSnapshotBackup(ctx, session, snapshotID, ownerID, coordinatorID, neededByDependents)
		return err
	})
	return meta, err
}

func (s *storageYDB) HoldSnapshotBackupPlan(ctx context.Context, snapshotID, ownerID, coordinatorID string) (meta *SnapshotMeta, err error) {
	if snapshotID == "" || ownerID == "" || coordinatorID == "" {
		return nil, errors.NewNonRetriableErrorf("empty snapshot backup plan owner")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		meta, err = s.holdSnapshotBackupPlan(ctx, session, snapshotID, ownerID, coordinatorID)
		return err
	})
	return meta, err
}

// The coordinator cannot be replaced until every saved graph hold is released.
func (s *storageYDB) FinishSnapshotBackupPlan(ctx context.Context, folderID, ownerID string) error {
	if folderID == "" || ownerID == "" {
		return errors.NewNonRetriableErrorf("empty snapshot backup plan identity")
	}
	return s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		return s.finishSnapshotBackupPlan(ctx, session, folderID, ownerID)
	})
}

// A completed copy releases both holds after DP cleanup. A pruned plan node
// releases only its preparation hold. Stale owners cannot clear either hold.
func (s *storageYDB) ReleaseSnapshotBackup(ctx context.Context, snapshotID, ownerID string, completed bool) error {
	if snapshotID == "" || ownerID == "" {
		return errors.NewNonRetriableErrorf("empty snapshot backup owner")
	}
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $id as Utf8;
        declare $owner as Utf8;
        declare $completed as Bool;
        $copy_owned = select id, folder_id from snapshots where id = $id and backup_task_id = $owner;
        $owned = select id, folder_id from snapshots where id = $id
            and (backup_task_id = $owner or backup_plan_id = $owner)
            and (COALESCE(backup_task_id, "") = "" or backup_task_id = $owner)
            and (COALESCE(backup_plan_id, "") = "" or backup_plan_id = $owner);
        update snapshots set backup_task_id = "", backup_plan_id = "",
            backup_completed = COALESCE(backup_completed, false) or $completed
            where id in (select id from $owned);
        update backup_folders set active_owner = "" where folder_id in (select folder_id from $copy_owned) and active_owner = $owner;
        delete from backup_queue where snapshot_id in (select id from $owned);
    `, s.snapshotsPath), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)), persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)), persistence.ValueParam("$completed", persistence.BoolValue(completed)))
	return err
}

func (s *storageYDB) ClaimSnapshotBackupFolder(ctx context.Context, folderID, generation, taskID string) (claimed bool, err error) {
	if folderID == "" || generation == "" || taskID == "" {
		return false, errors.NewNonRetriableErrorf("invalid snapshot backup folder owner")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		claimed, err = s.claimSnapshotBackupFolder(ctx, session, folderID, generation, taskID)
		return err
	})
	return claimed, err
}

func (s *storageYDB) ReleaseSnapshotBackupFolder(ctx context.Context, folderID, taskID string) error {
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $task as Utf8;
        update backup_folders set coordinator_task_id = ""
        where folder_id = $folder and coordinator_task_id = $task
            and COALESCE(active_owner, "") = "" and COALESCE(active_plan, "") = "";
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
	return err
}

func (s *storageYDB) SetSnapshotBackupFolder(ctx context.Context, folderID, generation string, enabled bool) error {
	if folderID == "" || (enabled && generation == "") {
		return errors.NewNonRetriableErrorf("invalid snapshot backup folder selection")
	}
	return s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		return s.setSnapshotBackupFolder(ctx, session, folderID, generation, enabled)
	})
}

////////////////////////////////////////////////////////////////////////////////

func createSnapshotBackupTables(ctx context.Context, folder string, db *persistence.YDBClient, dropUnusedColumns bool) error {
	return db.CreateOrAlterTable(ctx, folder, "backup_folders", persistence.NewCreateTableDescription(
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("generation", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("enabled", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("active_owner", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("active_plan", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("coordinator_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithPrimaryKeyColumn("folder_id"),
	), dropUnusedColumns)
}
