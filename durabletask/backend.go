package durabletask

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/andrewstucki/replicadb"
	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"google.golang.org/protobuf/proto"
)

var (
	// emptyString is used to avoid nil string pointers when constructing
	// orchestration metadata responses.
	emptyString string = ""

	//go:embed schema.sql
	schema string
	//go:embed drop.sql
	dropSchema string
)

// Option represents a functional option for configuring a ReplicaDBBackend.
type Option func(b *ReplicaDBBackend)

// ReplicaDBBackend implements the durabletask-go backend.Backend interface
// using replicadb as the persistence layer.
type ReplicaDBBackend struct {
	db *replicadb.DB

	orchestrationLockTimeout time.Duration
	activityLockTimeout      time.Duration
	workerName               string
	logger                   backend.Logger
}

var _ backend.Backend = (*ReplicaDBBackend)(nil)

// WithOrchestrationLockTimeout configures the orchestration lock timeout.
func WithOrchestrationLockTimeout(timeout time.Duration) func(b *ReplicaDBBackend) {
	return func(b *ReplicaDBBackend) {
		b.orchestrationLockTimeout = timeout
	}
}

// WithActivityLockTimeout configures the activity lock timeout.
func WithActivityLockTimeout(timeout time.Duration) func(b *ReplicaDBBackend) {
	return func(b *ReplicaDBBackend) {
		b.activityLockTimeout = timeout
	}
}

// WithLogger configures the logger used by the backend.
func WithLogger(logger backend.Logger) func(b *ReplicaDBBackend) {
	return func(b *ReplicaDBBackend) {
		b.logger = logger
	}
}

// generateWorkerId returns a globally unique worker identifier composed of
// hostname, process ID, and a random UUID.
func generateWorkerId() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()
	uuidStr := uuid.NewString()
	return fmt.Sprintf("%s,%d,%s", hostname, pid, uuidStr)
}

// NewReplicaDBBackend creates a new replicadb-backed durable task backend.
//
// The returned value implements backend.Backend and is ready to be started
// after applying any provided configuration options.
func NewReplicaDBBackend(db *replicadb.DB, options ...Option) *ReplicaDBBackend {
	workerId := generateWorkerId()

	b := &ReplicaDBBackend{
		db:                       db,
		workerName:               workerId,
		orchestrationLockTimeout: time.Duration(2 * time.Minute),
		activityLockTimeout:      time.Duration(2 * time.Minute),
		logger:                   NoopLogger(),
	}

	for _, opt := range options {
		opt(b)
	}

	return b
}

// CreateTaskHub creates the task hub schema in the underlying database.
func (b *ReplicaDBBackend) CreateTaskHub(context.Context) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	// Initialize database
	if _, err := db.Exec(schema); err != nil {
		panic(fmt.Errorf("failed to initialize the database: %w", err))
	}

	return nil
}

// DeleteTaskHub deletes all task hub tables from the database.
func (b *ReplicaDBBackend) DeleteTaskHub(ctx context.Context) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	// Destroy the database
	if _, err := db.Exec(dropSchema); err != nil {
		return fmt.Errorf("failed to destroy the database: %w", err)
	}
	return nil
}

// AbandonOrchestrationWorkItem releases locks held on an orchestration
// and optionally delays re-visibility of its events.
func (b *ReplicaDBBackend) AbandonOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var visibleTime *time.Time = nil
	if delay := wi.GetAbandonDelay(); delay > 0 {
		t := time.Now().UTC().Add(delay)
		visibleTime = &t
	}

	dbResult, err := tx.ExecContext(
		ctx,
		"UPDATE NewEvents SET [LockedBy] = NULL, [VisibleTime] = ? WHERE [InstanceID] = ? AND [LockedBy] = ?",
		visibleTime,
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update NewEvents table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE NewEvents statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	dbResult, err = tx.ExecContext(
		ctx,
		"UPDATE Instances SET [LockedBy] = NULL, [LockExpiration] = NULL WHERE [InstanceID] = ? AND [LockedBy] = ?",
		string(wi.InstanceID),
		wi.LockedBy,
	)

	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	rowsAffected, err = dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by UPDATE Instances statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CompleteOrchestrationWorkItem finalizes processing of an orchestration work item.
//
// This method:
//   - Updates orchestration instance metadata (status, timestamps, output)
//   - Appends new history events
//   - Enqueues outbound activity tasks and orchestration messages
//   - Deletes processed inbound events
//
// All operations are performed atomically within a transaction. If the
// orchestration lock is lost, ErrWorkItemLockLost is returned.
func (b *ReplicaDBBackend) CompleteOrchestrationWorkItem(ctx context.Context, wi *backend.OrchestrationWorkItem) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()

	// Dynamically generate the UPDATE statement for the Instances table
	var sqlSB strings.Builder
	sqlSB.WriteString("UPDATE Instances SET ")

	sqlUpdateArgs := make([]any, 0, 10)
	isCreated := false
	isCompleted := false
	isTerminated := false

	for _, e := range wi.State.NewEvents() {
		if es := e.GetExecutionStarted(); es != nil {
			if isCreated {
				// TODO: Log warning about duplicate start event
				continue
			}
			isCreated = true
			sqlSB.WriteString("[CreatedTime] = ?, [Input] = ?, ")
			sqlUpdateArgs = append(sqlUpdateArgs, e.Timestamp.AsTime())
			sqlUpdateArgs = append(sqlUpdateArgs, es.Input.GetValue())
		} else if ec := e.GetExecutionCompleted(); ec != nil {
			if isCompleted {
				// TODO: Log warning about duplicate completion event
				continue
			}
			isCompleted = true
			sqlSB.WriteString("[CompletedTime] = ?, [Output] = ?, [FailureDetails] = ?, ")
			sqlUpdateArgs = append(sqlUpdateArgs, now)
			sqlUpdateArgs = append(sqlUpdateArgs, ec.Result.GetValue())
			if ec.FailureDetails != nil {
				bytes, err := proto.Marshal(ec.FailureDetails)
				if err != nil {
					return fmt.Errorf("failed to marshal FailureDetails: %w", err)
				}
				sqlUpdateArgs = append(sqlUpdateArgs, &bytes)
			} else {
				sqlUpdateArgs = append(sqlUpdateArgs, nil)
			}
		} else if et := e.GetExecutionTerminated(); et != nil {
			if isTerminated {
				// TODO: Log warning about duplicate termination event
				continue
			}
			isTerminated = true
			sqlSB.WriteString("[CompletedTime] = ?, ")
			sqlUpdateArgs = append(sqlUpdateArgs, now)
		}
		// TODO: Execution suspended & resumed
	}

	if wi.State.CustomStatus != nil {
		sqlSB.WriteString("[CustomStatus] = ?, ")
		sqlUpdateArgs = append(sqlUpdateArgs, wi.State.CustomStatus.Value)
	}

	// TODO: Support for stickiness, which would extend the LockExpiration
	sqlSB.WriteString("[RuntimeStatus] = ?, [LastUpdatedTime] = ?, [LockExpiration] = NULL WHERE [InstanceID] = ? AND [LockedBy] = ?")
	sqlUpdateArgs = append(sqlUpdateArgs, toRuntimeStatusString(wi.State.RuntimeStatus()), now, string(wi.InstanceID), wi.LockedBy)

	result, err := tx.ExecContext(ctx, sqlSB.String(), sqlUpdateArgs...)
	if err != nil {
		return fmt.Errorf("failed to update Instances table: %w", err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get the number of rows affected by the Instance table update: %w", err)
	} else if count == 0 {
		return fmt.Errorf("instance '%s' no longer exists or was locked by a different worker", string(wi.InstanceID))
	}

	// If continue-as-new, delete all existing history
	if wi.State.ContinuedAsNew() {
		if _, err := tx.ExecContext(ctx, "DELETE FROM History WHERE InstanceID = ?", string(wi.InstanceID)); err != nil {
			return fmt.Errorf("failed to delete from History table: %w", err)
		}
	}

	// Save new history events
	newHistoryCount := len(wi.State.NewEvents())
	if newHistoryCount > 0 {
		query := "INSERT INTO History ([InstanceID], [SequenceNumber], [EventPayload]) VALUES (?, ?, ?)" +
			strings.Repeat(", (?, ?, ?)", newHistoryCount-1)

		args := make([]any, 0, newHistoryCount*3)
		nextSequenceNumber := len(wi.State.OldEvents())
		for _, e := range wi.State.NewEvents() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			args = append(args, string(wi.InstanceID), nextSequenceNumber, eventPayload)
			nextSequenceNumber++
		}

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to insert into the History table: %w", err)
		}
	}

	// Save outbound activity tasks
	newActivityCount := len(wi.State.PendingTasks())
	if newActivityCount > 0 {
		insertSql := "INSERT INTO NewTasks ([InstanceID], [EventPayload]) VALUES (?, ?)" +
			strings.Repeat(", (?, ?)", newActivityCount-1)

		sqlInsertArgs := make([]any, 0, newActivityCount*2)
		for _, e := range wi.State.PendingTasks() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload)
		}

		_, err = tx.ExecContext(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewTasks table: %w", err)
		}
	}

	// Save outbound orchestrator events
	newEventCount := len(wi.State.PendingTimers()) + len(wi.State.PendingMessages())
	if newEventCount > 0 {
		insertSql := "INSERT INTO NewEvents ([InstanceID], [EventPayload], [VisibleTime]) VALUES (?, ?, ?)" +
			strings.Repeat(", (?, ?, ?)", newEventCount-1)

		sqlInsertArgs := make([]any, 0, newEventCount*3)
		for _, e := range wi.State.PendingTimers() {
			eventPayload, err := backend.MarshalHistoryEvent(e)
			if err != nil {
				return err
			}

			visibleTime := e.GetTimerFired().GetFireAt().AsTime()
			sqlInsertArgs = append(sqlInsertArgs, string(wi.InstanceID), eventPayload, visibleTime)
		}

		for _, msg := range wi.State.PendingMessages() {
			if es := msg.HistoryEvent.GetExecutionStarted(); es != nil {
				// Need to insert a new row into the DB
				if _, err := b.createOrchestrationInstanceInternal(ctx, msg.HistoryEvent, tx); err != nil {
					if err == backend.ErrDuplicateEvent {
						b.logger.Warnf(
							"%v: dropping sub-orchestration creation event because an instance with the target ID (%v) already exists.",
							wi.InstanceID,
							es.OrchestrationInstance.InstanceId)
					} else {
						return err
					}
				}
			}

			eventPayload, err := backend.MarshalHistoryEvent(msg.HistoryEvent)
			if err != nil {
				return err
			}

			sqlInsertArgs = append(sqlInsertArgs, msg.TargetInstanceID, eventPayload, nil)
		}

		_, err = tx.ExecContext(ctx, insertSql, sqlInsertArgs...)
		if err != nil {
			return fmt.Errorf("failed to insert into the NewEvents table: %w", err)
		}
	}

	// Delete inbound events
	dbResult, err := tx.ExecContext(
		ctx,
		"DELETE FROM NewEvents WHERE [InstanceID] = ? AND [LockedBy] = ?",
		string(wi.InstanceID),
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// CreateOrchestrationInstance creates a new orchestration instance and enqueues
// its ExecutionStarted event for processing.
//
// If an instance with the same ID already exists, the configured reuse policy
// determines whether the request is ignored, rejected, or replaces the
// existing instance.
func (b *ReplicaDBBackend) CreateOrchestrationInstance(ctx context.Context, e *backend.HistoryEvent, opts ...backend.OrchestrationIdReusePolicyOptions) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	var instanceID string
	if instanceID, err = b.createOrchestrationInstanceInternal(ctx, e, tx, opts...); errors.Is(err, api.ErrIgnoreInstance) {
		// choose to ignore, do nothing
		return nil
	} else if err != nil {
		return err
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)`,
		instanceID,
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to create orchestration: %w", err)
	}

	return nil
}

func (b *ReplicaDBBackend) createOrchestrationInstanceInternal(ctx context.Context, e *backend.HistoryEvent, tx *sql.Tx, opts ...backend.OrchestrationIdReusePolicyOptions) (string, error) {
	if e == nil {
		return "", errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return "", errors.New("HistoryEvent must have a non-nil timestamp")
	}

	startEvent := toExecutionStartedEvent(e)
	if startEvent == nil {
		return "", errors.New("HistoryEvent must be an ExecutionStartedEvent")
	}
	instanceID := startEvent.instanceId

	policy := &api.OrchestrationIdReusePolicy{}

	for _, opt := range opts {
		opt(policy)
	}

	rows, err := insertOrIgnoreInstanceTableInternal(ctx, tx, e, startEvent)
	if err != nil {
		return "", err
	}

	// instance with same ID already exists
	if rows <= 0 {
		return instanceID, b.handleInstanceExists(ctx, tx, startEvent, policy, e)
	}
	return instanceID, nil
}

func insertOrIgnoreInstanceTableInternal(ctx context.Context, tx *sql.Tx, e *backend.HistoryEvent, startEvent *executionStartedEvent) (int64, error) {
	res, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO [Instances] (
			[Name],
			[Version],
			[InstanceID],
			[ExecutionID],
			[Input],
			[RuntimeStatus],
			[CreatedTime]
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		startEvent.name,
		startEvent.version,
		startEvent.instanceId,
		startEvent.executionId,
		startEvent.input,
		"PENDING",
		e.Timestamp.AsTime(),
	)
	if err != nil {
		return -1, fmt.Errorf("failed to insert into [Instances] table: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return -1, fmt.Errorf("failed to count the rows affected: %w", err)
	}
	return rows, nil
}

func (b *ReplicaDBBackend) handleInstanceExists(ctx context.Context, tx *sql.Tx, startEvent *executionStartedEvent, policy *api.OrchestrationIdReusePolicy, e *backend.HistoryEvent) error {
	// query RuntimeStatus for the existing instance
	queryRow := tx.QueryRowContext(
		ctx,
		`SELECT [RuntimeStatus] FROM Instances WHERE [InstanceID] = ?`,
		startEvent.instanceId,
	)
	var runtimeStatus *string
	err := queryRow.Scan(&runtimeStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan the Instances table result: %w", err)
	}

	// status not match, return instance duplicate error
	if !isStatusMatch(policy.OperationStatus, fromRuntimeStatusString(*runtimeStatus)) {
		return api.ErrDuplicateInstance
	}

	// status match
	switch policy.Action {
	case api.REUSE_ID_ACTION_IGNORE:
		// Log an warning message and ignore creating new instance
		b.logger.Warnf("An instance with ID '%s' already exists; dropping duplicate create request", startEvent.instanceId)
		return api.ErrIgnoreInstance
	case api.REUSE_ID_ACTION_TERMINATE:
		// terminate existing instance
		if err := b.cleanupOrchestrationStateInternal(ctx, tx, api.InstanceID(startEvent.instanceId), false); err != nil {
			return fmt.Errorf("failed to cleanup orchestration status: %w", err)
		}
		// create a new instance
		var rows int64
		if rows, err = insertOrIgnoreInstanceTableInternal(ctx, tx, e, startEvent); err != nil {
			return err
		}

		// should never happen, because we clean up instance before create new one
		if rows <= 0 {
			return fmt.Errorf("failed to insert into [Instances] table because entry already exists")
		}
		return nil
	}
	// default behavior
	return api.ErrDuplicateInstance
}

func isStatusMatch(statuses []api.OrchestrationStatus, runtimeStatus api.OrchestrationStatus) bool {
	return slices.Contains(statuses, runtimeStatus)
}

func (b *ReplicaDBBackend) cleanupOrchestrationStateInternal(ctx context.Context, tx *sql.Tx, id api.InstanceID, requireCompleted bool) error {
	row := tx.QueryRowContext(ctx, "SELECT 1 FROM Instances WHERE [InstanceID] = ?", string(id))
	if err := row.Err(); err != nil {
		return fmt.Errorf("failed to query for instance existence: %w", err)
	}

	var unused int
	if err := row.Scan(&unused); errors.Is(err, sql.ErrNoRows) {
		return api.ErrInstanceNotFound
	} else if err != nil {
		return fmt.Errorf("failed to scan instance existence: %w", err)
	}

	if requireCompleted {
		// purge orchestration in ['COMPLETED', 'FAILED', 'TERMINATED']
		dbResult, err := tx.ExecContext(ctx, "DELETE FROM Instances WHERE [InstanceID] = ? AND [RuntimeStatus] IN ('COMPLETED', 'FAILED', 'TERMINATED')", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}

		rowsAffected, err := dbResult.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected in Instances delete operation: %w", err)
		}
		if rowsAffected == 0 {
			return api.ErrNotCompleted
		}
	} else {
		// clean up orchestration in all [RuntimeStatus]
		_, err := tx.ExecContext(ctx, "DELETE FROM Instances WHERE [InstanceID] = ?", string(id))
		if err != nil {
			return fmt.Errorf("failed to delete from the Instances table: %w", err)
		}
	}

	_, err := tx.ExecContext(ctx, "DELETE FROM History WHERE [InstanceID] = ?", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from History table: %w", err)
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM NewEvents WHERE [InstanceID] = ?", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewEvents table: %w", err)
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE [InstanceID] = ?", string(id))
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}
	return nil
}

// AddNewOrchestrationEvent enqueues a new external event for an existing
// orchestration instance.
func (b *ReplicaDBBackend) AddNewOrchestrationEvent(ctx context.Context, iid api.InstanceID, e *backend.HistoryEvent) error {
	if e == nil {
		return errors.New("HistoryEvent must be non-nil")
	} else if e.Timestamp == nil {
		return errors.New("HistoryEvent must have a non-nil timestamp")
	}

	eventPayload, err := backend.MarshalHistoryEvent(e)
	if err != nil {
		return err
	}

	db, err := b.db.Write()
	if err != nil {
		return err
	}

	_, err = db.ExecContext(
		ctx,
		`INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)`,
		string(iid),
		eventPayload,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row into [NewEvents] table: %w", err)
	}

	return nil
}

// GetOrchestrationMetadata retrieves metadata for a specific orchestration
// instance, including runtime status, timestamps, input/output, and failure details.
func (b *ReplicaDBBackend) GetOrchestrationMetadata(ctx context.Context, iid api.InstanceID) (*api.OrchestrationMetadata, error) {
	return deserializeMetadataRecord(b.db.QueryRowContext(
		ctx,
		`SELECT [InstanceID], [Name], [RuntimeStatus], [CreatedTime], [LastUpdatedTime], [Input], [Output], [CustomStatus], [FailureDetails]
		FROM Instances WHERE [InstanceID] = ?`,
		string(iid),
	))
}

// GetOrchestrationRuntimeState loads the full orchestration runtime state,
// including all historical events, for replay by the orchestrator.
func (b *ReplicaDBBackend) GetOrchestrationRuntimeState(ctx context.Context, wi *backend.OrchestrationWorkItem) (*backend.OrchestrationRuntimeState, error) {
	rows, err := b.db.QueryContext(
		ctx,
		"SELECT [EventPayload] FROM History WHERE [InstanceID] = ? ORDER BY [SequenceNumber] ASC",
		string(wi.InstanceID),
	)
	if err != nil {
		return nil, err
	}

	existingEvents := make([]*backend.HistoryEvent, 0, 50)
	for rows.Next() {
		var eventPayload []byte
		if err := rows.Scan(&eventPayload); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}

		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}

		existingEvents = append(existingEvents, e)
	}

	state := backend.NewOrchestrationRuntimeState(wi.InstanceID, existingEvents)
	return state, nil
}

// GetOrchestrationWorkItem acquires a lock on an orchestration instance that
// has visible, unprocessed events and returns it for execution.
//
// If no work is available, ErrNoWorkItems is returned.
func (b *ReplicaDBBackend) GetOrchestrationWorkItem(ctx context.Context) (*backend.OrchestrationWorkItem, error) {
	db, err := b.db.Write()
	if err != nil {
		return nil, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	newLockExpiration := now.Add(b.orchestrationLockTimeout)

	// Place a lock on an orchestration instance that has new events that are ready to be executed.
	row := tx.QueryRowContext(
		ctx,
		`UPDATE Instances SET [LockedBy] = ?, [LockExpiration] = ?
		WHERE [rowid] = (
			SELECT [rowid] FROM Instances I
			WHERE (I.[LockExpiration] IS NULL OR I.[LockExpiration] < ?) AND EXISTS (
				SELECT 1 FROM NewEvents E
				WHERE E.[InstanceID] = I.[InstanceID] AND (E.[VisibleTime] IS NULL OR E.[VisibleTime] < ?)
			)
			LIMIT 1
		) RETURNING [InstanceID]`,
		b.workerName,      // LockedBy for Instances table
		newLockExpiration, // Updated LockExpiration for Instances table
		now,               // LockExpiration for Instances table
		now,               // VisibleTime for NewEvents table
	)

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}

	var instanceID string
	if err := row.Scan(&instanceID); err != nil {
		if err == sql.ErrNoRows {
			// No new events to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the orchestration work-item: %w", err)
	}

	// TODO: Get all the unprocessed events associated with the locked instance
	events, err := tx.QueryContext(
		ctx,
		`UPDATE NewEvents SET [DequeueCount] = [DequeueCount] + 1, [LockedBy] = ? WHERE rowid IN (
			SELECT rowid FROM NewEvents
			WHERE [InstanceID] = ? AND ([VisibleTime] IS NULL OR [VisibleTime] <= ?)
			LIMIT 1000
		)
		RETURNING [EventPayload], [DequeueCount]`,
		b.workerName,
		instanceID,
		now,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query for orchestration work-items: %w", err)
	}

	maxDequeueCount := int32(0)

	newEvents := make([]*backend.HistoryEvent, 0, 10)
	for events.Next() {
		var eventPayload []byte
		var dequeueCount int32
		if err := events.Scan(&eventPayload, &dequeueCount); err != nil {
			return nil, fmt.Errorf("failed to read history event: %w", err)
		}

		if dequeueCount > maxDequeueCount {
			maxDequeueCount = dequeueCount
		}

		e, err := backend.UnmarshalHistoryEvent(eventPayload)
		if err != nil {
			return nil, err
		}

		newEvents = append(newEvents, e)
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to update orchestration work-item: %w", err)
	}

	wi := &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(instanceID),
		NewEvents:  newEvents,
		LockedBy:   b.workerName,
		RetryCount: maxDequeueCount - 1,
	}

	return wi, nil
}

// GetActivityWorkItem acquires and locks the next available activity task
// for execution.
//
// If no activity work is available, ErrNoWorkItems is returned.
func (b *ReplicaDBBackend) GetActivityWorkItem(ctx context.Context) (*backend.ActivityWorkItem, error) {
	db, err := b.db.Write()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newLockExpiration := now.Add(b.orchestrationLockTimeout)

	row := db.QueryRowContext(
		ctx,
		`UPDATE NewTasks SET [LockedBy] = ?, [LockExpiration] = ?, [DequeueCount] = [DequeueCount] + 1
		WHERE [SequenceNumber] = (
			SELECT [SequenceNumber] FROM NewTasks T
			WHERE T.[LockExpiration] IS NULL OR T.[LockExpiration] < ?
			LIMIT 1
		) RETURNING [SequenceNumber], [InstanceID], [EventPayload]`,
		b.workerName,
		newLockExpiration,
		now,
	)

	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("failed to query for activity work-items: %w", err)
	}

	var sequenceNumber int64
	var instanceID string
	var eventPayload []byte

	if err := row.Scan(&sequenceNumber, &instanceID, &eventPayload); err != nil {
		if err == sql.ErrNoRows {
			// No new activity tasks to process
			return nil, backend.ErrNoWorkItems
		}

		return nil, fmt.Errorf("failed to scan the activity work-item: %w", err)
	}

	e, err := backend.UnmarshalHistoryEvent(eventPayload)
	if err != nil {
		return nil, err
	}

	wi := &backend.ActivityWorkItem{
		SequenceNumber: sequenceNumber,
		InstanceID:     api.InstanceID(instanceID),
		NewEvent:       e,
		LockedBy:       b.workerName,
	}
	return wi, nil
}

// CompleteActivityWorkItem completes an activity task by:
//   - Enqueuing its result as a new orchestration event
//   - Deleting the activity task if the lock is still valid
func (b *ReplicaDBBackend) CompleteActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bytes, err := backend.MarshalHistoryEvent(wi.Result)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO NewEvents ([InstanceID], [EventPayload]) VALUES (?, ?)", string(wi.InstanceID), bytes)
	if err != nil {
		return fmt.Errorf("failed to insert into NewEvents table: %w", err)
	}

	dbResult, err := tx.ExecContext(ctx, "DELETE FROM NewTasks WHERE [SequenceNumber] = ? AND [LockedBy] = ?", wi.SequenceNumber, wi.LockedBy)
	if err != nil {
		return fmt.Errorf("failed to delete from NewTasks table: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by delete statement: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// AbandonActivityWorkItem releases the lock on an activity task, making it
// available for retry by another worker.
func (b *ReplicaDBBackend) AbandonActivityWorkItem(ctx context.Context, wi *backend.ActivityWorkItem) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	dbResult, err := db.ExecContext(
		ctx,
		"UPDATE NewTasks SET [LockedBy] = NULL, [LockExpiration] = NULL WHERE [SequenceNumber] = ? AND [LockedBy] = ?",
		wi.SequenceNumber,
		wi.LockedBy,
	)
	if err != nil {
		return fmt.Errorf("failed to update the NewTasks table for abandon: %w", err)
	}

	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed get rows affected by update statement for abandon: %w", err)
	} else if rowsAffected == 0 {
		return backend.ErrWorkItemLockLost
	}

	return nil
}

// GetOrchestrationsWithStatus returns orchestration metadata of orchestrations with the given status.
func (b *ReplicaDBBackend) GetOrchestrationsWithStatus(ctx context.Context, status api.OrchestrationStatus) ([]*api.OrchestrationMetadata, error) {
	rows, err := b.db.QueryContext(
		ctx,
		`SELECT [InstanceID], [Name], [RuntimeStatus], [CreatedTime], [LastUpdatedTime], [Input], [Output], [CustomStatus], [FailureDetails]
		FROM Instances WHERE [RuntimeStatus] = ?`,
		toRuntimeStatusString(status),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metadata := []*api.OrchestrationMetadata{}

	for rows.Next() {
		meta, err := deserializeMetadataRecord(rows)
		if err != nil {
			return nil, err
		}
		metadata = append(metadata, meta)
	}

	return metadata, nil
}

// GetOrchestrationsWithName returns orchestration metadata of orchestrations with the given status.
func (b *ReplicaDBBackend) GetOrchestrationsWithName(ctx context.Context, name string) ([]*api.OrchestrationMetadata, error) {
	rows, err := b.db.QueryContext(
		ctx,
		`SELECT [InstanceID], [Name], [RuntimeStatus], [CreatedTime], [LastUpdatedTime], [Input], [Output], [CustomStatus], [FailureDetails]
		FROM Instances WHERE [Name] = ?`,
		name,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metadata := []*api.OrchestrationMetadata{}

	for rows.Next() {
		meta, err := deserializeMetadataRecord(rows)
		if err != nil {
			return nil, err
		}
		metadata = append(metadata, meta)
	}

	return metadata, nil
}

type rowScanner interface {
	Err() error
	Scan(dest ...any) error
}

func deserializeMetadataRecord(row rowScanner) (*api.OrchestrationMetadata, error) {
	err := row.Err()
	if err == sql.ErrNoRows {
		return nil, api.ErrInstanceNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to query the Instances table: %w", row.Err())
	}

	var instanceID *string
	var name *string
	var runtimeStatus *string
	var createdAt *time.Time
	var lastUpdatedAt *time.Time
	var input *string
	var output *string
	var customStatus *string
	var failureDetails *backend.TaskFailureDetails

	var failureDetailsPayload []byte
	err = row.Scan(&instanceID, &name, &runtimeStatus, &createdAt, &lastUpdatedAt, &input, &output, &customStatus, &failureDetailsPayload)
	if err == sql.ErrNoRows {
		return nil, api.ErrInstanceNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to scan the Instances table result: %w", err)
	}

	if input == nil {
		input = &emptyString
	}

	if output == nil {
		output = &emptyString
	}

	if customStatus == nil {
		customStatus = &emptyString
	}

	if len(failureDetailsPayload) > 0 {
		failureDetails = new(backend.TaskFailureDetails)
		if err := proto.Unmarshal(failureDetailsPayload, failureDetails); err != nil {
			return nil, fmt.Errorf("failed to unmarshal failure details: %w", err)
		}
	}

	metadata := api.NewOrchestrationMetadata(
		api.InstanceID(*instanceID),
		*name,
		fromRuntimeStatusString(*runtimeStatus),
		*createdAt,
		*lastUpdatedAt,
		*input,
		*output,
		*customStatus,
		failureDetails,
	)
	return metadata, nil
}

// PurgeCompletedOrchestrationStateBefore permanently deletes all persisted state for all
// orchestration instances, provided they have completed prior to the given time.
func (b *ReplicaDBBackend) PurgeCompletedOrchestrationStateOlderThan(ctx context.Context, timeframe time.Duration, opts ...api.PurgeOptions) error {
	before := time.Now().Add(-timeframe).UTC()

	db, err := b.db.Write()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, "SELECT [InstanceID] FROM Instances WHERE [RuntimeStatus] IN ('COMPLETED', 'FAILED', 'TERMINATED') AND [CompletedTime] < ?", before)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if err := b.cleanupOrchestrationStateInternal(ctx, tx, api.InstanceID(id), true); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// PurgeOrchestrationState permanently deletes all persisted state for an
// orchestration instance, provided it has completed.
func (b *ReplicaDBBackend) PurgeOrchestrationState(ctx context.Context, id api.InstanceID) error {
	db, err := b.db.Write()
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := b.cleanupOrchestrationStateInternal(ctx, tx, id, true); err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Start initializes the backend.
//
// ReplicaDBBackend does not require explicit startup logic.
func (*ReplicaDBBackend) Start(context.Context) error {
	return nil
}

// Stop shuts down the backend.
//
// ReplicaDBBackend does not require explicit shutdown logic.
func (*ReplicaDBBackend) Stop(context.Context) error {
	return nil
}
