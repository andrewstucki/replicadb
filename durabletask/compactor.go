package durabletask

import (
	"context"
	"errors"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

const (
	compactionActivityName      = "__compaction_activity"
	compactionOrchestrationName = "__compaction"
	compactionID                = api.InstanceID("__compaction")
)

func registerCompaction(e *Executor) error {
	if err := e.RegisterOrchestration(compactionOrchestrationName, compactOrchestration); err != nil {
		return err
	}
	return e.RegisterActivity(compactionActivityName, compactionActivity(e.backend))
}

type compactionInput struct {
	WorkerName string
	Interval   time.Duration
}

// compactBackend schedules the hub's compaction orchestration. One compactor serves
// every executor on the hub: a finished one is replaced, and one that is running,
// started by this executor or another, is left to run.
func compactBackend(ctx context.Context, e *Executor) error {
	_, err := e.ScheduleOrchestration(ctx, compactionOrchestrationName,
		api.WithInstanceID(compactionID),
		api.WithOrchestrationIdReusePolicy(&api.OrchestrationIdReusePolicy{
			OperationStatus: []api.OrchestrationStatus{
				api.RUNTIME_STATUS_COMPLETED,
				api.RUNTIME_STATUS_FAILED,
				api.RUNTIME_STATUS_TERMINATED,
				api.RUNTIME_STATUS_CANCELED,
			},
			Action: api.REUSE_ID_ACTION_TERMINATE,
		}),
		api.WithInput(compactionInput{
			WorkerName: e.backend.workerName,
			Interval:   e.compactionInterval,
		}))
	if errors.Is(err, api.ErrDuplicateInstance) {
		return nil
	}
	return err
}

func stopCompaction(ctx context.Context, e *Executor) error {
	running, err := isRunning(ctx, e.backend, compactionID)
	if err != nil {
		return err
	}

	if running {
		return e.TerminateOrchestration(ctx, compactionID)
	}

	return nil
}

func compactionActivity(db *ReplicaDBBackend) func(ctx task.ActivityContext) (any, error) {
	return func(ctx task.ActivityContext) (any, error) {
		var input compactionInput

		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}

		return nil, db.PurgeCompletedOrchestrationStateOlderThan(ctx.Context(), input.Interval)
	}
}

func compactOrchestration(ctx *task.OrchestrationContext) (any, error) {
	var input compactionInput

	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	if err := ctx.CallActivity(compactionActivityName, task.WithActivityInput(input)).Await(nil); err != nil {
		return nil, err
	}

	if err := ctx.CreateTimer(input.Interval).Await(nil); err != nil {
		return nil, err
	}

	ctx.ContinueAsNew(input)

	return nil, nil
}
