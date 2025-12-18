package durabletask

import (
	"context"
	"sync"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

const (
	compactionTaskName     = "__compaction_task"
	compactionWorkflowName = "__compaction"
	compactionID           = api.InstanceID("__compaction")
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
	return e.RegisterWorkflow(&compactor{})
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

	_, err := e.ScheduleWorkflow(ctx, compactionWorkflowName, api.WithInstanceID(compactionID), api.WithInput(compactionInput{
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
	return e.TerminateWorkflow(ctx, compactionID)
}

type compactionTask struct{}

func (t *compactionTask) Name() string {
	return compactionTaskName
}

func (t *compactionTask) Execute(ctx task.ActivityContext) (any, error) {
	var input compactionInput

	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	db := globalCompactedBackends.get(input.WorkerName)
	if db == nil {
		return nil, nil
	}

	return nil, db.PurgeCompletedOrchestrationStateBefore(ctx.Context(), time.Now().Add(-input.Interval))
}

type compactor struct{}

func (t *compactor) Name() string {
	return compactionWorkflowName
}

func (c *compactor) Execute(ctx *task.OrchestrationContext) (any, error) {
	var input compactionInput

	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	if err := ctx.CallActivity(compactionTaskName, task.WithActivityInput(input)).Await(nil); err != nil {
		return nil, err
	}

	if err := ctx.CreateTimer(input.Interval).Await(nil); err != nil {
		return nil, err
	}

	ctx.ContinueAsNew(input)

	return nil, nil
}

func (c *compactor) Tasks() []Task {
	return []Task{
		&compactionTask{},
	}
}
