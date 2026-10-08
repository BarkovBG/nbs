package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type SnapshotBackupFolder struct {
	FolderID   string
	Generation string
}

// State is the coordinator's serialized per-resource state, not a chunk list.
type SnapshotBackupPlan struct {
	FolderID       string
	SnapshotID     string
	OwnerID        string
	CoordinatorID  string
	Prepared       bool
	CreatingAt     time.Time
	BaseSnapshotID string
	State          []byte
}

func scanSnapshotBackupPlans(ctx context.Context, res persistence.Result) ([]SnapshotBackupPlan, error) {
	var plans []SnapshotBackupPlan
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var p SnapshotBackupPlan
			err := res.ScanNamed(
				persistence.OptionalWithDefault("folder_id", &p.FolderID),
				persistence.OptionalWithDefault("snapshot_id", &p.SnapshotID),
				persistence.OptionalWithDefault("owner_id", &p.OwnerID),
				persistence.OptionalWithDefault("coordinator_id", &p.CoordinatorID),
				persistence.OptionalWithDefault("prepared", &p.Prepared),
				persistence.OptionalWithDefault("creating_at", &p.CreatingAt),
				persistence.OptionalWithDefault("base_snapshot_id", &p.BaseSnapshotID),
				persistence.OptionalWithDefault("state", &p.State),
			)
			if err != nil {
				return nil, err
			}
			plans = append(plans, p)
		}
	}
	return plans, res.Err()
}

// Reserve and Delete serialize on the same CP snapshot row. In particular,
// deleting may be recorded, but cannot issue DP cleanup while OwnerID remains.
func (s *storageYDB) reserveSnapshotBackup(
	ctx context.Context, session *persistence.Session, snapshotID, ownerID string, start bool,
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
	if err = res.Err(); err != nil {
		return nil, err
	}
	if len(states) == 0 {
		return nil, tx.Commit(ctx)
	}
	state := states[0]
	if state.backupTaskID == ownerID && state.backupStarted {
		return state.toSnapshotMeta(), tx.Commit(ctx)
	}
	if state.status != snapshotStatusReady || state.backupCompleted {
		return nil, tx.Commit(ctx)
	}
	if state.backupTaskID != "" && state.backupTaskID != ownerID {
		return nil, errors.NewInterruptExecutionError()
	}
	res2, err := tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $id as Utf8;
        declare $owner as Utf8;
        select active_owner, prepared from backup_folders where folder_id = $folder;
        select owner_id, prepared from backup_plans where folder_id = $folder and snapshot_id = $id and owner_id = $owner;
    `, s.snapshotsPath),
		persistence.ValueParam("$folder", persistence.UTF8Value(state.folderID)),
		persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)))
	if err != nil {
		return nil, err
	}
	defer res2.Close()
	if !res2.NextResultSet(ctx) || !res2.NextRow() {
		if res2.Err() != nil {
			return nil, res2.Err()
		}
		return nil, tx.Commit(ctx)
	}
	var active string
	var prepared bool
	err = res2.ScanNamed(persistence.OptionalWithDefault("active_owner", &active), persistence.OptionalWithDefault("prepared", &prepared))
	if err != nil {
		return nil, err
	}
	if !res2.NextResultSet(ctx) || !res2.NextRow() {
		if res2.Err() != nil {
			return nil, res2.Err()
		}
		return nil, tx.Commit(ctx)
	}
	var sourcePrepared bool
	if err := res2.ScanNamed(persistence.OptionalWithDefault("prepared", &sourcePrepared)); err != nil {
		return nil, err
	}
	if res2.Err() != nil {
		return nil, res2.Err()
	}
	if start && (!prepared || !sourcePrepared || (active != "" && active != ownerID)) {
		return nil, errors.NewInterruptExecutionError()
	}
	state.backupTaskID = ownerID
	state.backupStarted = start
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $states as List<%v>;
        declare $folder as Utf8;
        declare $owner as Utf8;
        declare $start as Bool;
        upsert into snapshots select * from AS_TABLE($states);
        update backup_folders set active_owner = $owner where folder_id = $folder and $start;
    `, s.snapshotsPath, snapshotStateStructTypeString()),
		persistence.ValueParam("$states", persistence.ListValue(state.structValue())),
		persistence.ValueParam("$folder", persistence.UTF8Value(state.folderID)),
		persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)),
		persistence.ValueParam("$start", persistence.BoolValue(start)))
	if err != nil {
		return nil, err
	}
	return state.toSnapshotMeta(), tx.Commit(ctx)
}

func (s *storageYDB) saveSnapshotBackupPlan(ctx context.Context, session *persistence.Session, p SnapshotBackupPlan) error {
	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	res, err := tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $id as Utf8;
        select owner_id from backup_plans where folder_id = $folder and snapshot_id = $id;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(p.FolderID)), persistence.ValueParam("$id", persistence.UTF8Value(p.SnapshotID)))
	if err != nil {
		return err
	}
	defer res.Close()
	if res.NextResultSet(ctx) && res.NextRow() {
		var owner string
		if err := res.ScanWithDefaults(&owner); err != nil {
			return err
		}
		if owner != p.OwnerID {
			return errors.NewInterruptExecutionError()
		}
	}
	if res.Err() != nil {
		return res.Err()
	}
	_, err = tx.Execute(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $id as Utf8;
        declare $owner as Utf8;
        declare $coordinator as Utf8;
        declare $created as Timestamp;
        declare $base as Utf8;
        declare $state as String;
        declare $prepared as Bool;
        upsert into backup_plans (folder_id, snapshot_id, owner_id, coordinator_id, creating_at, base_snapshot_id, state, prepared)
            values ($folder, $id, $owner, $coordinator, $created, $base, $state, $prepared);
        $new = select $id as snapshot_id, $owner as backup_id;
        upsert into backup_queue select n.* from $new as n left only join backup_queue as q on n.snapshot_id = q.snapshot_id;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(p.FolderID)), persistence.ValueParam("$id", persistence.UTF8Value(p.SnapshotID)),
		persistence.ValueParam("$owner", persistence.UTF8Value(p.OwnerID)), persistence.ValueParam("$coordinator", persistence.UTF8Value(p.CoordinatorID)),
		persistence.ValueParam("$created", persistence.TimestampValue(p.CreatingAt)), persistence.ValueParam("$base", persistence.UTF8Value(p.BaseSnapshotID)), persistence.ValueParam("$state", persistence.StringValue(p.State)), persistence.ValueParam("$prepared", persistence.BoolValue(p.Prepared)))
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
        select folder_id, generation from backup_folders order by folder_id;
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

func (s *storageYDB) SetSnapshotBackupFolderPrepared(ctx context.Context, folderID, generation string, prepared bool) error {
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $generation as Utf8;
        declare $prepared as Bool;
        update backup_folders set prepared = $prepared where folder_id = $folder and COALESCE(generation, "") = $generation;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
		persistence.ValueParam("$generation", persistence.UTF8Value(generation)), persistence.ValueParam("$prepared", persistence.BoolValue(prepared)))
	return err
}

func (s *storageYDB) ListSnapshotBackupCandidates(ctx context.Context, folderID, afterID string, before time.Time, limit int) ([]string, error) {
	if folderID == "" || limit <= 0 || limit > 1000 {
		return nil, errors.NewNonRetriableErrorf("invalid snapshot backfill page")
	}
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $after as Utf8;
        declare $before as Timestamp;
        declare $ready as Int64;
        declare $limit as Uint64;
        select id from snapshots view folder_id_index where folder_id = $folder and id > $after and creating_at <= $before
            and status = $ready and not COALESCE(backup_completed, false)
        order by id limit $limit;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
		persistence.ValueParam("$after", persistence.UTF8Value(afterID)), persistence.ValueParam("$before", persistence.TimestampValue(before)),
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

func (s *storageYDB) GetSnapshotBackupPlan(ctx context.Context, folderID, snapshotID string) (*SnapshotBackupPlan, error) {
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $id as Utf8;
        select * from backup_plans where folder_id = $folder and snapshot_id = $id;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	plans, err := scanSnapshotBackupPlans(ctx, res)
	if err != nil || len(plans) == 0 {
		return nil, err
	}
	return &plans[0], nil
}

func (s *storageYDB) SaveSnapshotBackupPlan(ctx context.Context, p SnapshotBackupPlan) error {
	if p.FolderID == "" || p.SnapshotID == "" || p.OwnerID == "" || p.CoordinatorID == "" {
		return errors.NewNonRetriableErrorf("invalid snapshot backup plan")
	}
	return s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		return s.saveSnapshotBackupPlan(ctx, session, p)
	})
}

func (s *storageYDB) ListSnapshotBackupPlans(ctx context.Context, folderID string, limit int, eligible bool, afterID string) ([]SnapshotBackupPlan, error) {
	order := "p.snapshot_id"
	if eligible {
		order = "p.creating_at, p.snapshot_id"
	}
	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $after as Utf8;
        declare $limit as Uint64;
        declare $eligible as Bool;
        select p.* from backup_plans as p left join backup_plans as b
            on p.folder_id = b.folder_id and p.base_snapshot_id = b.snapshot_id
        where p.folder_id = $folder and p.snapshot_id > $after and (not $eligible or b.snapshot_id is null)
        order by %v limit $limit;
    `, s.snapshotsPath, order), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))), persistence.ValueParam("$eligible", persistence.BoolValue(eligible)), persistence.ValueParam("$after", persistence.UTF8Value(afterID)))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	return scanSnapshotBackupPlans(ctx, res)
}

func (s *storageYDB) RemoveSnapshotBackupPlan(ctx context.Context, folderID, snapshotID, ownerID string) error {
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $id as Utf8;
        declare $owner as Utf8;
        $owned = select snapshot_id from backup_plans where folder_id = $folder and snapshot_id = $id and owner_id = $owner;
        delete from backup_queue where snapshot_id in $owned and COALESCE(backup_id, "") = $owner;
        delete from backup_plans where folder_id = $folder and snapshot_id = $id and owner_id = $owner;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)), persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)))
	return err
}

func (s *storageYDB) ReserveSnapshotBackup(ctx context.Context, snapshotID, ownerID string, start bool) (meta *SnapshotMeta, err error) {
	if ownerID == "" {
		return nil, errors.NewNonRetriableErrorf("empty snapshot backup owner")
	}
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		var err error
		meta, err = s.reserveSnapshotBackup(ctx, session, snapshotID, ownerID, start)
		return err
	})
	return meta, err
}

// The DP hold is released before this CP reservation, after all child IO ends.
// The owner guard prevents a delayed cancellation from clearing another attempt.
func (s *storageYDB) ReleaseSnapshotBackup(ctx context.Context, snapshotID, ownerID string, completed bool) error {
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $id as Utf8;
        declare $owner as Utf8;
        declare $completed as Bool;
        $owned = select id, folder_id from snapshots where id = $id and backup_task_id = $owner
            and (not $completed or COALESCE(backup_started, false));
        update snapshots set backup_task_id = "", backup_started = false,
            backup_completed = COALESCE(backup_completed, false) or $completed
            where id in (select id from $owned);
        update backup_folders set active_owner = "" where folder_id in (select folder_id from $owned) and active_owner = $owner;
        delete from backup_queue where snapshot_id in (select id from $owned);
    `, s.snapshotsPath), persistence.ValueParam("$id", persistence.UTF8Value(snapshotID)), persistence.ValueParam("$owner", persistence.UTF8Value(ownerID)), persistence.ValueParam("$completed", persistence.BoolValue(completed)))
	return err
}

func createSnapshotBackupTables(ctx context.Context, folder string, db *persistence.YDBClient, dropUnusedColumns bool) error {
	err := db.CreateOrAlterTable(ctx, folder, "backup_folders", persistence.NewCreateTableDescription(
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("generation", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("prepared", persistence.Optional(persistence.TypeBool)),
		persistence.WithColumn("active_owner", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("coordinator_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithPrimaryKeyColumn("folder_id"),
	), dropUnusedColumns)
	if err != nil {
		return err
	}
	return db.CreateOrAlterTable(ctx, folder, "backup_plans", persistence.NewCreateTableDescription(
		persistence.WithColumn("folder_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("owner_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("coordinator_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("base_snapshot_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("state", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("prepared", persistence.Optional(persistence.TypeBool)),
		persistence.WithPrimaryKeyColumn("folder_id", "snapshot_id"),
	), dropUnusedColumns)
}

func (s *storageYDB) ClaimSnapshotBackupFolder(ctx context.Context, folderID, generation, taskID string) (claimed bool, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		res, err := tx.Execute(ctx, fmt.Sprintf(`
            --!syntax_v1
            pragma TablePathPrefix = "%v";
            declare $folder as Utf8;
            select coordinator_task_id, generation from backup_folders where folder_id = $folder;
        `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)))
		if err != nil {
			return err
		}
		defer res.Close()
		if !res.NextResultSet(ctx) || !res.NextRow() {
			if res.Err() != nil {
				return res.Err()
			}
			return tx.Commit(ctx)
		}
		var coordinator, currentGeneration string
		if err := res.ScanNamed(persistence.OptionalWithDefault("coordinator_task_id", &coordinator), persistence.OptionalWithDefault("generation", &currentGeneration)); err != nil {
			return err
		}
		if currentGeneration != generation {
			return tx.Commit(ctx)
		}
		foreign, err := tx.Execute(ctx, fmt.Sprintf(`
            --!syntax_v1
            pragma TablePathPrefix = "%v";
            declare $folder as Utf8;
            declare $task as Utf8;
            select snapshot_id from backup_plans where folder_id = $folder and COALESCE(coordinator_id, "") != $task limit 1;
        `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
		if err != nil {
			return err
		}
		defer foreign.Close()
		if foreign.NextResultSet(ctx) && foreign.NextRow() {
			return errors.NewInterruptExecutionError()
		}
		if foreign.Err() != nil {
			return foreign.Err()
		}
		if coordinator != "" && coordinator != taskID {
			return errors.NewInterruptExecutionError()
		}
		_, err = tx.Execute(ctx, fmt.Sprintf(`
            --!syntax_v1
            pragma TablePathPrefix = "%v";
            declare $folder as Utf8;
            declare $task as Utf8;
            update backup_folders set coordinator_task_id = $task where folder_id = $folder;
        `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
		if err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	return claimed, err
}

func (s *storageYDB) ReleaseSnapshotBackupFolder(ctx context.Context, folderID, taskID string) error {
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $task as Utf8;
        update backup_folders set coordinator_task_id = "", prepared = false
            where folder_id = $folder and coordinator_task_id = $task and COALESCE(active_owner, "") = "";
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$task", persistence.UTF8Value(taskID)))
	return err
}

// Enabling is idempotent; a fresh generation is used only when the folder was
// absent. Disabling removes selection, not accepted work or source references.
func (s *storageYDB) SetSnapshotBackupFolder(ctx context.Context, folderID, generation string, enabled bool) error {
	if folderID == "" || (enabled && generation == "") {
		return errors.NewNonRetriableErrorf("invalid snapshot backup folder")
	}
	_, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
        --!syntax_v1
        pragma TablePathPrefix = "%v";
        declare $folder as Utf8;
        declare $generation as Utf8;
        declare $enabled as Bool;
        $new = select $folder as folder_id, $generation as generation where $enabled;
        upsert into backup_folders select n.* from $new as n left only join backup_folders as f on n.folder_id = f.folder_id;
        delete from backup_folders where folder_id = $folder and not $enabled;
    `, s.snapshotsPath), persistence.ValueParam("$folder", persistence.UTF8Value(folderID)), persistence.ValueParam("$generation", persistence.UTF8Value(generation)), persistence.ValueParam("$enabled", persistence.BoolValue(enabled)))
	return err
}
