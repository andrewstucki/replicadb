package durabletask

import (
	"context"

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
}

func NewExecutor(db *replicadb.DB, options ...Option) *Executor {
	replicadb := NewReplicaDBBackend(db, options...)
	registry := task.NewTaskRegistry()
	executor := task.NewTaskExecutor(registry)
	orchestrationWorker := backend.NewOrchestrationWorker(replicadb, executor, replicadb.logger)
	activityWorker := backend.NewActivityTaskWorker(replicadb, executor, replicadb.logger)
	worker := backend.NewTaskHubWorker(replicadb, orchestrationWorker, activityWorker, replicadb.logger)
	client := backend.NewTaskHubClient(replicadb)

	return &Executor{
		backend:  replicadb,
		registry: registry,
		executor: executor,
		client:   client,
		worker:   worker,
	}
}

type Task interface {
	Name() string
	Execute(ctx task.ActivityContext) (any, error)
}

type Workflow interface {
	Name() string
	Execute(ctx *task.OrchestrationContext) (any, error)
	Tasks() []Task
}

func (e *Executor) RegisterWorkflow(workflow Workflow) error {
	if err := e.registry.AddOrchestratorN(workflow.Name(), workflow.Execute); err != nil {
		return err
	}

	for _, task := range workflow.Tasks() {
		if err := e.registry.AddActivityN(task.Name(), task.Execute); err != nil {
			return err
		}
	}

	return nil
}

func (e *Executor) Start(ctx context.Context) error {
	return e.worker.Start(ctx)
}

func (e *Executor) Shutdown(ctx context.Context) error {
	return e.worker.Shutdown(ctx)
}

func (e *Executor) ScheduleWorkflow(ctx context.Context, orchestrator string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	return e.client.ScheduleNewOrchestration(ctx, orchestrator, opts...)
}

func (e *Executor) WorkflowMetadata(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return e.client.FetchOrchestrationMetadata(ctx, id)
}

func (e *Executor) WaitForWorkflowStart(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return e.client.WaitForOrchestrationStart(ctx, id)
}

func (e *Executor) WaitForWorkflowCompletion(ctx context.Context, id api.InstanceID) (*api.OrchestrationMetadata, error) {
	return e.client.WaitForOrchestrationCompletion(ctx, id)
}

func (e *Executor) TerminateWorkflow(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	return e.client.TerminateOrchestration(ctx, id, opts...)
}

func (e *Executor) ExternalEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	return e.client.RaiseEvent(ctx, id, eventName, opts...)
}

func (e *Executor) SuspendWorkflow(ctx context.Context, id api.InstanceID, reason string) error {
	return e.client.SuspendOrchestration(ctx, id, reason)
}

func (e *Executor) ResumeWorkflow(ctx context.Context, id api.InstanceID, reason string) error {
	return e.client.ResumeOrchestration(ctx, id, reason)
}

func (e *Executor) WorkflowsWithName(ctx context.Context, name string) ([]*api.OrchestrationMetadata, error) {
	return e.backend.GetOrchestrationsWithName(ctx, name)
}

func (e *Executor) PurgeWorkflow(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	return e.client.PurgeOrchestrationState(ctx, id, opts...)
}

func (e *Executor) PurgeCompletedWorkflows(ctx context.Context, opts ...api.PurgeOptions) error {
	return e.backend.PurgeCompletedOrchestrationState(ctx, opts...)
}
