package tasks

import (
	"context"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	empty_metrics "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

type blankTask struct {
}

func (t *blankTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *blankTask) Load(_, _ []byte) error {
	return nil
}

func (t *blankTask) Run(ctx context.Context, execCtx ExecutionContext) error {
	logging.Error(ctx, "running task with id %v", execCtx.GetTaskID())

	hostname := os.Getenv("HOST")

	logging.Info(ctx, "hostname is %s", hostname)

	banInterval := 30 * time.Second
	maxBanHostsCount := uint64(1)

	endpoint := "ydb.serverless.yandexcloud.net:2135"
	database := "ru-central1/b1g06bpp2fj1ve048589/etn1ak8gsoc82h42o5nv"
	secure := true
	disableAuthentication := true

	db, err := persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:              &endpoint,
			Database:              &database,
			Secure:                &secure,
			DisableAuthentication: &disableAuthentication,
		},
		empty_metrics.NewRegistry(),
	)
	if err != nil {
		logging.Error(ctx, "Failed to initialize YDB client: %v", err)
		return err
	}
	defer db.Close(ctx)

	regularSystemTasksEnabled := false
	availabilityMonitoringStorage := persistence.NewAvailabilityMonitoringStorage(
		&tasks_config.TasksConfig{
			RegularSystemTasksEnabled: &regularSystemTasksEnabled,
		},
		db,
		banInterval,
		maxBanHostsCount,
	)

	successRateReportingInterval := 20 * time.Second
	successRateAvailabilityTrasehold := 0.5

	availabilityMonitoring, err := persistence.NewAvailabilityMonitoring(
		ctx,
		"s3",
		hostname,
		successRateReportingInterval,
		successRateAvailabilityTrasehold,
		availabilityMonitoringStorage,
		empty_metrics.NewRegistry(),
	)
	if err != nil {
		return err
	}

	s3, err := persistence.NewS3Client(
		"storage.yandexcloud.net",
		"ru-central1",
		persistence.S3Credentials{},
		10*time.Second,
		empty_metrics.NewRegistry(),
		10,
		availabilityMonitoring,
	)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	tickCount := 0
	for range ticker.C {
		_, err = s3.GetObject(ctx, "barkovbg-s3", "BarkovBorisPP.pdf")
		tickCount++

		if tickCount == 45 {
			break
		}
	}

	logging.Error(ctx, "completed task with id %v", execCtx.GetTaskID())

	return nil
}

func (t *blankTask) Cancel(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	logging.Info(ctx, "cancelling blank task with id %v", execCtx.GetTaskID())
	return nil
}

func (t *blankTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *blankTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
