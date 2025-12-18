package durabletask

import (
	"encoding/json"
	"testing"

	"github.com/andrewstucki/replicadb"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

const (
	TestTaskName     = "TestTask"
	TestWorkflowName = "TestWorkflow"
)

type tracker struct {
	CalledTask     bool
	CalledWorkflow bool
}

type TestTask struct{}

func (t *TestTask) Name() string {
	return TestTaskName
}
func (t *TestTask) Execute(ctx task.ActivityContext) (any, error) {
	var input tracker
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	input.CalledTask = true
	return input, nil
}

type TestWorkflow struct{}

func (t *TestWorkflow) Name() string {
	return TestWorkflowName
}

func (t *TestWorkflow) Execute(ctx *task.OrchestrationContext) (any, error) {
	var output tracker
	if err := ctx.CallActivity(TestTaskName, task.WithActivityInput(tracker{
		CalledWorkflow: true,
	})).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func (t *TestWorkflow) Tasks() []Task {
	return []Task{
		&TestTask{},
	}
}

func TestExecutor(t *testing.T) {
	db, err := replicadb.Memory()
	require.NoError(t, err)

	executor := NewExecutor(db)
	executor.RegisterWorkflow(&TestWorkflow{})

	require.NoError(t, executor.Start(t.Context()))
	defer func() {
		require.NoError(t, executor.Shutdown(t.Context()))
	}()

	id, err := executor.ScheduleWorkflow(t.Context(), TestWorkflowName)
	require.NoError(t, err)

	metadata, err := executor.WaitForWorkflowCompletion(t.Context(), id)
	require.NoError(t, err)

	var output tracker
	require.NoError(t, json.Unmarshal([]byte(metadata.SerializedOutput), &output))
	require.True(t, output.CalledTask)
	require.True(t, output.CalledWorkflow)

	_, err = executor.WorkflowMetadata(t.Context(), id)
	require.NoError(t, err)

	require.NoError(t, executor.PurgeCompletedWorkflows(t.Context()))

	_, err = executor.WorkflowMetadata(t.Context(), id)
	require.Error(t, err)
}
