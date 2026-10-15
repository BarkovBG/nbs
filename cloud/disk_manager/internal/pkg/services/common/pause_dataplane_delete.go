package common

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

// PauseDataplaneDelete pauses dataplane.DeleteSnapshot when its source zone
// is lost. The caller then finishes the control-plane task successfully, so
// compute gets success. The paused task deletes the checkpoint after ResumeTask,
// once the disks are back.
//
// Already finished dataplane work counts as paused: the caller must not wait.
func PauseDataplaneDelete(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	scheduler tasks.Scheduler,
	zoneLost bool,
	taskID string,
) (bool, error) {

	if !zoneLost {
		return false, nil
	}

	err := scheduler.PauseTask(ctx, taskID)
	if err != nil {
		_, waitErr := scheduler.WaitTask(ctx, execCtx, taskID)
		if waitErr != nil {
			return false, err
		}

		return true, nil
	}

	logging.Info(
		ctx,
		"Paused dataplane.DeleteSnapshot %v because its zone is lost",
		taskID,
	)
	return true, nil
}
