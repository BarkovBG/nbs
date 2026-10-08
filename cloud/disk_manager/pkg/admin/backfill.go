package admin

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/backups/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type backfill struct {
	commandWithScheduler
	kind      string
	batchSize uint32
}

func (c *backfill) run() error {
	if c.serverConfig.GetSnapshotStorageBackupConfig() == nil {
		return fmt.Errorf("snapshot backup is not configured")
	}
	if c.batchSize == 0 || c.batchSize > 1000 {
		return fmt.Errorf("batch-size must be between 1 and 1000")
	}
	err := c.init()
	if err != nil {
		return err
	}
	defer c.close()

	id, err := c.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(c.ctx, "backfill_"+generateID()),
		"backups.Backfill",
		"",
		&protos.BackfillRequest{
			ResourceKind:        c.kind,
			BatchSize:           c.batchSize,
			CreatedBeforeMicros: time.Now().UnixMicro(),
		},
	)
	if err != nil {
		return err
	}
	fmt.Printf("Task: %v\n", id)
	return nil
}

func newBackfillCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
	kind string,
) *cobra.Command {

	c := &backfill{
		commandWithScheduler: newCommandWithScheduler(clientConfig, serverConfig),
		kind:                 kind,
	}
	cmd := &cobra.Command{
		Use:   "backfill",
		Short: "Back up existing ready " + kind,
		Long: "Start a resumable backfill of existing ready " + kind + ". " +
			"Each batch waits for completed backups before advancing. " +
			"Inspect the returned task with tasks get --id <task-id>. " +
			"Cancelling the backfill stops scanning; already queued backups continue.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}
	cmd.Flags().Uint32Var(
		&c.batchSize,
		"batch-size",
		32,
		"Maximum number of resource backups in a batch",
	)
	return cmd
}
