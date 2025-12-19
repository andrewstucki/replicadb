package durabletask

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/andrewstucki/replicadb"
	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
)

const (
	TestActivityName      = "TestActivity"
	TestOrchestrationName = "TestOrchestration"
)

type tracker struct {
	CalledActivity      bool
	CalledOrchestration bool
}

func TestingActivity(ctx task.ActivityContext) (any, error) {
	var input tracker
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	input.CalledActivity = true
	return input, nil
}

func TestingOrchestration(ctx *task.OrchestrationContext) (any, error) {
	var output tracker
	if err := ctx.CallActivity(TestActivityName, task.WithActivityInput(tracker{
		CalledOrchestration: true,
	})).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func TestExecutor(t *testing.T) {
	db, err := replicadb.Memory()
	require.NoError(t, err)

	executor := NewExecutor(db)
	require.NoError(t, executor.RegisterOrchestration(TestOrchestrationName, TestingOrchestration))
	require.NoError(t, executor.RegisterActivity(TestActivityName, TestingActivity))

	require.NoError(t, executor.EnableCompactor(time.Second))

	require.NoError(t, executor.Start(t.Context()))
	defer func() {
		require.NoError(t, executor.Shutdown(t.Context()))
	}()

	id, err := executor.ScheduleOrchestration(t.Context(), TestOrchestrationName)
	require.NoError(t, err)

	metadata, err := executor.WaitForOrchestrationCompletion(t.Context(), id)
	require.NoError(t, err)

	var output tracker
	require.NoError(t, json.Unmarshal([]byte(metadata.SerializedOutput), &output))
	require.True(t, output.CalledActivity)
	require.True(t, output.CalledOrchestration)

	_, err = executor.OrchestrationMetadata(t.Context(), id)
	require.NoError(t, err)

	// wait for two seconds and see if we've compacted stuff
	time.Sleep(2 * time.Second)

	_, err = executor.OrchestrationMetadata(t.Context(), id)
	require.Error(t, err)
}
