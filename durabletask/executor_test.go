package durabletask

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
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

	executor := NewExecutor(db).EnableCompactor(time.Second)
	require.NoError(t, executor.RegisterOrchestration(TestOrchestrationName, TestingOrchestration))
	require.NoError(t, executor.RegisterActivity(TestActivityName, TestingActivity))

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

func TestExecutorParallelism(t *testing.T) {
	db, err := replicadb.Memory()
	require.NoError(t, err)

	const n = 4
	executor := NewExecutor(db, WithMaxParallelism(n))
	var (
		mu      sync.Mutex
		running int
		most    int
	)
	require.NoError(t, executor.RegisterActivity(TestActivityName, func(ctx task.ActivityContext) (any, error) {
		mu.Lock()
		running++
		most = max(most, running)
		mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		return nil, nil
	}))
	require.NoError(t, executor.RegisterOrchestration(TestOrchestrationName, func(ctx *task.OrchestrationContext) (any, error) {
		var calls []task.Task
		for range n {
			calls = append(calls, ctx.CallActivity(TestActivityName))
		}
		for _, c := range calls {
			if err := c.Await(nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}))

	require.NoError(t, executor.Start(t.Context()))
	defer func() {
		require.NoError(t, executor.Shutdown(t.Context()))
	}()

	id, err := executor.ScheduleOrchestration(t.Context(), TestOrchestrationName)
	require.NoError(t, err)
	_, err = executor.WaitForOrchestrationCompletion(t.Context(), id)
	require.NoError(t, err)
	require.Greater(t, most, 1, "activities ran side by side")
}

func TestExecutorsShareACompactor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	var executors []*Executor
	for range 2 {
		db, err := replicadb.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		executor := NewExecutor(db).EnableCompactor(time.Hour)
		require.NoError(t, executor.RegisterOrchestration(TestOrchestrationName, TestingOrchestration))
		require.NoError(t, executor.RegisterActivity(TestActivityName, TestingActivity))
		require.NoError(t, executor.Start(t.Context()), "a second executor starts beside the first's compactor")
		t.Cleanup(func() { require.NoError(t, executor.Shutdown(context.Background())) })
		executors = append(executors, executor)
	}

	id, err := executors[1].ScheduleOrchestration(t.Context(), TestOrchestrationName)
	require.NoError(t, err)
	_, err = executors[0].WaitForOrchestrationCompletion(t.Context(), id)
	require.NoError(t, err)

	running, err := isRunning(t.Context(), executors[0].backend, compactionID)
	require.NoError(t, err)
	require.True(t, running, "the first executor's compactor still runs")
}
