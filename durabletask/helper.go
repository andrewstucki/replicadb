package durabletask

import (
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
)

var orchestrationStatus = map[string]api.OrchestrationStatus{
	"RUNNING":          api.RUNTIME_STATUS_RUNNING,
	"COMPLETED":        api.RUNTIME_STATUS_COMPLETED,
	"CONTINUED_AS_NEW": api.RUNTIME_STATUS_CONTINUED_AS_NEW,
	"FAILED":           api.RUNTIME_STATUS_FAILED,
	"CANCELED":         api.RUNTIME_STATUS_CANCELED,
	"TERMINATED":       api.RUNTIME_STATUS_TERMINATED,
	"PENDING":          api.RUNTIME_STATUS_PENDING,
	"SUSPENDED":        api.RUNTIME_STATUS_SUSPENDED,
}

func toRuntimeStatusString(status api.OrchestrationStatus) string {
	return status.String()[len("ORCHESTRATION_STATUS_"):]
}

func fromRuntimeStatusString(status string) api.OrchestrationStatus {
	return orchestrationStatus[status]
}

func toExecutionStartedEvent(historyEvent *backend.HistoryEvent) *executionStartedEvent {
	startEvent := historyEvent.GetExecutionStarted()
	return &executionStartedEvent{
		name:        startEvent.Name,
		version:     startEvent.Version.GetValue(),
		instanceId:  startEvent.OrchestrationInstance.InstanceId,
		executionId: startEvent.OrchestrationInstance.ExecutionId.GetValue(),
		input:       startEvent.Input.GetValue(),
	}
}

type executionStartedEvent struct {
	name        string
	version     string
	instanceId  string
	executionId string
	input       string
}
