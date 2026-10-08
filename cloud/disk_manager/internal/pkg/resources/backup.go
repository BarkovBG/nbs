package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// EnqueueBackupBatch atomically checks readiness, preserves queued attempts and
// adds missing ones. Completed resources and resources created after the start
// of this backfill are excluded. The returned page is ordered by resource ID.
func (s *storageYDB) EnqueueBackupBatch(
	ctx context.Context,
	kind string,
	afterID string,
	createdBefore time.Time,
	limit int,
	backupID string,
) ([]BackupRequest, error) {

	if limit <= 0 || limit > 1000 || len(backupID) == 0 {
		return nil, errors.NewNonRetriableErrorf("invalid backup batch parameters")
	}
	var path, table, idColumn string
	var ready int64
	switch kind {
	case "snapshots":
		path, table, idColumn = s.snapshotsPath, "snapshots", "snapshot_id"
		ready = int64(snapshotStatusReady)
	case "images":
		path, table, idColumn = s.imagesPath, "images", "image_id"
		ready = int64(imageStatusReady)
	default:
		return nil, errors.NewNonRetriableErrorf("unknown backup resource kind %q", kind)
	}

	res, err := s.db.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $after_id as Utf8;
		declare $created_before as Timestamp;
		declare $limit as Uint64;
		declare $ready as Int64;
		declare $backup_id as Utf8;

		$candidates = (
			select id from %v
			where id > $after_id and creating_at <= $created_before
				and status = $ready and not COALESCE(backup_completed, false)
			order by id limit $limit
		);
		$page = (
			select c.id as resource_id,
				IF(q.%v is null, $backup_id, COALESCE(q.backup_id, "")) as backup_id
			from $candidates as c left join backup_queue as q on c.id = q.%v
		);
		$new = (
			select c.id as %v, $backup_id as backup_id
			from $candidates as c left only join backup_queue as q on c.id = q.%v
		);
		upsert into backup_queue select * from $new;
		select resource_id, backup_id from $page order by resource_id;
	`, path, table, idColumn, idColumn, idColumn, idColumn),
		persistence.ValueParam("$after_id", persistence.UTF8Value(afterID)),
		persistence.ValueParam("$created_before", persistence.TimestampValue(createdBefore)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
		persistence.ValueParam("$ready", persistence.Int64Value(ready)),
		persistence.ValueParam("$backup_id", persistence.UTF8Value(backupID)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var requests []BackupRequest
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var request BackupRequest
			err = res.ScanNamed(
				persistence.OptionalWithDefault("resource_id", &request.ResourceID),
				persistence.OptionalWithDefault("backup_id", &request.BackupID),
			)
			if err != nil {
				return nil, err
			}
			requests = append(requests, request)
		}
	}
	return requests, res.Err()
}
