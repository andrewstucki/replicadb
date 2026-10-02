package durabletask

import (
	"context"
	"time"

	"github.com/andrewstucki/replicadb"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
)

type Executor struct {
	backend *ReplicaDBBackend

	registry *task.TaskRegistry
	executor backend.Executor
	client   backend.TaskHubClient
	worker   backend.TaskHubWorker

	compactorEnabled   bool
	compactionInterval time.Duration

	// pollers hears the workers' pollers stop, and stopPolling stops them.
	pollers     *pollerLog
	stopPolling context.CancelFunc
}

func NewExecutor(db *replicadb.DB, options ...Option) *Executor {
	replicadb := NewReplicaDBBackend(db, options...)
	registry := task.NewTaskRegistry()
	executor := task.NewTaskExecutor(registry)
	parallelism := backend.WithMaxParallelism(replicadb.maxParallelism)
	pollers := &pollerLog{Logger: replicadb.logger, stopped: make(chan struct{}, workers)}
	orchestrationWorker := backend.NewOrchestrationWorker(replicadb, executor, pollers, parallelism)
	activityWorker := backend.NewActivityTaskWorker(replicadb, executor, pollers, parallelism)
	worker := backend.NewTaskHubWorker(replicadb, orchestrationWorker, activityWorker, replicadb.logger)
	client := backend.NewTaskHubClient(replicadb)

	return &Executor{
		backend:  replicadb,
		registry: registry,
		executor: executor,
		client:   client,
		worker:   worker,
		pollers:  pollers,
	}
}

func (e *Executor) EnableCompactor(interval time.Duration) *Executor {
	e.compactionInterval = interval
	e.compactorEnabled = true
	return e
}

func (e *Executor) RegisterActivity(name string, activity task.Activity) error {
	return e.registry.AddActivityN(name, activity)
}

func (e *Executor) RegisterOrchestration(name string, orchestrator task.Orchestrator) error {
	return e.registry.AddOrchestratorN(name, orchestrator)
}

func (e *Executor) Start(ctx context.Context) error {
	if err := registerCompaction(e); err != nil {
		return err
	}

	polling, stop := context.WithCancel(ctx)
	e.stopPolling = stop
	if err := e.worker.Start(polling); err != nil {
		return err
	}

	if e.compactorEnabled {
		return compactBackend(ctx, e)
	}
	return stopCompaction(ctx, e)
}

// Shutdown stops the workers' pollers, and once each has stopped, drains the workers.
//
// NB: durabletask-go's worker adds to its pending WaitGroup each time it polls, and
// draining waits on that group, so draining while a poller may poll again races. Stopping
// the pollers first, and waiting for each to say it has, keeps the two apart.
func (e *Executor) Shutdown(ctx context.Context) error {
	if e.stopPolling != nil {
		e.stopPolling()
		e.stopPolling = nil
		e.pollers.wait(ctx, workers, pollersStopMost)
	}
	return e.worker.Shutdown(ctx)
}

func (e *Executor) ScheduleOrchestration(ctx context.Context, name string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	return e.client.ScheduleNewOrchestration(ctx, name, opts...)
}

func (e *Executor) OrchestrationMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return e.client.FetchOrchestrationMetadata(ctx, id)
}

func (e *Executor) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return e.client.WaitForOrchestrationStart(ctx, id)
}

func (e *Executor) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return e.client.WaitForOrchestrationCompletion(ctx, id)
}

func (e *Executor) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	return e.client.TerminateOrchestration(ctx, id, opts...)
}

func (e *Executor) ExternalEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	return e.client.RaiseEvent(ctx, id, eventName, opts...)
}

func (e *Executor) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return e.client.SuspendOrchestration(ctx, id, reason)
}

func (e *Executor) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return e.client.ResumeOrchestration(ctx, id, reason)
}

func (e *Executor) OrchestrationsWithName(ctx context.Context, name string) ([]*api.OrchestrationMetadata, error) {
	return e.backend.GetOrchestrationsWithName(ctx, name)
}

func (e *Executor) PurgeOrchestration(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	return e.client.PurgeOrchestrationState(ctx, id, opts...)
}

func (e *Executor) PurgeCompletedOrchestrationsOlderThan(ctx context.Context, timeframe time.Duration, opts ...api.PurgeOptions) error {
	return e.backend.PurgeCompletedOrchestrationStateOlderThan(ctx, timeframe, opts...)
}
