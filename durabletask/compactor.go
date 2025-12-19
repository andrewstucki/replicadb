package durabletask

import (
	"context"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

const (
	compactionActivityName      = "__compaction_activity"
	compactionOrchestrationName = "__compaction"
	compactionID                = api.InstanceID("__compaction")
)

type backendRegistry struct {
	backends map[string]*ReplicaDBBackend
	mutex    sync.RWMutex
}

func (b *backendRegistry) register(be *ReplicaDBBackend) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.backends[be.workerName] = be
}

func (b *backendRegistry) get(workerName string) *ReplicaDBBackend {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.backends[workerName]
}

var globalCompactedBackends *backendRegistry

func init() {
	globalCompactedBackends = &backendRegistry{
		backends: map[string]*ReplicaDBBackend{},
	}
}

func registerCompaction(e *Executor) error {
	if err := e.RegisterOrchestration(compactionOrchestrationName, compactOrchestration); err != nil {
		return err
	}
	return e.RegisterActivity(compactionActivityName, compactionActivity)
}

type compactionInput struct {
	WorkerName string
	Interval   time.Duration
}

func isTerminal(status api.OrchestrationStatus) bool {
	switch status {
	case api.RUNTIME_STATUS_COMPLETED,
		api.RUNTIME_STATUS_FAILED,
		api.RUNTIME_STATUS_TERMINATED,
		api.RUNTIME_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

func compactBackend(ctx context.Context, e *Executor) error {
	globalCompactedBackends.register(e.backend)

	// stop and restart with our interval
	if err := stopCompaction(ctx, e); err != nil {
		return err
	}

	_, err := e.ScheduleOrchestration(ctx, compactionOrchestrationName, api.WithInstanceID(compactionID), api.WithInput(compactionInput{
		WorkerName: e.backend.workerName,
		Interval:   e.compactionInterval,
	}))
	return err
}

func stopCompaction(ctx context.Context, e *Executor) error {
	metadata, err := e.backend.GetOrchestrationMetadata(ctx, compactionID)
	if (err != nil && err == api.ErrInstanceNotFound) || (metadata != nil && isTerminal(metadata.RuntimeStatus)) {
		// nothing to do since we're already all terminated
		return nil
	}

	if err != nil {
		return err
	}

	// terminate
	return e.TerminateOrchestration(ctx, compactionID)
}

func compactionActivity(ctx task.ActivityContext) (any, error) {
	var input compactionInput

	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	db := globalCompactedBackends.get(input.WorkerName)
	if db == nil {
		return nil, nil
	}

	return nil, db.PurgeCompletedOrchestrationStateOlderThan(ctx.Context(), input.Interval)
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
