package backups

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	registry *tasks.Registry,
	scheduler tasks.Scheduler,
	storage resources.Storage,
) error {

	return registry.RegisterForExecution("backups.Backfill", func() tasks.Task {
		return &backfillTask{scheduler: scheduler, storage: storage}
	})
}
