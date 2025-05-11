package rdq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// --- Start of Queue Implementation ---

const (
	taskStatusPending    = "pending"
	taskStatusProcessing = "processing"
	taskStatusCompleted  = "completed"
	taskStatusFailed     = "failed"

	// Keys for task-specific metadata and results
	keyStatus             = "task:{%s}:status"         // Stores the processing status of a specific taskID
	keyResult             = "task:{%s}:result"         // Stores the execution result of a specific taskID
	keyStreamResultFormat = "stream:task:{%s}:results" // Stream for publishing results of a specific taskID

	// Suffix for the main task stream (one stream per queue instance)
	keyTaskStreamSuffix = "tasks_stream"
	// Suffix for the consumer group name associated with the task stream
	defaultConsumerGroupNameSuffix = "group"

	// Default prefix for all internal Redis keys
	keyPrefix = "_rdq:"

	// Fields within a task stream message
	taskStreamMessageFieldTaskID  = "taskID"
	taskStreamMessageFieldPayload = "payload"
)

// QueueConfig holds configuration options for the Queue.
type QueueConfig struct {
	// TaskExpiry is the time after which task status, and result keys/streams expire.
	TaskExpiry time.Duration

	// ProducerResultWaitTimeout is the maximum time a producer will wait for a task result via the result Stream.
	// This is used to create a context.WithTimeout for the producer's wait.
	ProducerResultWaitTimeout time.Duration

	// Name is the base name for this queue, used to prefix Redis keys.
	Name string
	// Logger is the slog.Logger instance for this queue.
	Logger *slog.Logger

	// ConsumerGroupName is the name of the consumer group for the task stream.
	// All consumers for this queue should share the same group name.
	ConsumerGroupName string

	// ConsumerIdentifier is a unique identifier for this specific consumer instance.
	// Crucial for XREADGROUP's Pending Entries List (PEL) management.
	ConsumerIdentifier string
}

// DefaultQueueConfig returns a QueueConfig with default values.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		TaskExpiry:                5 * time.Minute,
		ProducerResultWaitTimeout: 60 * time.Second,
		Name:                      "", // Will be defaulted to a UUID if empty
		Logger:                    slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		ConsumerGroupName:         "", // Will be defaulted based on queue name
		ConsumerIdentifier:        "", // Will be defaulted to a UUID if empty
	}
}

// WithTaskExpiry sets the TaskExpiry duration.
func (qc QueueConfig) WithTaskExpiry(d time.Duration) QueueConfig {
	qc.TaskExpiry = d
	return qc
}

// WithProducerResultWaitTimeout sets the ProducerResultWaitTimeout duration.
func (qc QueueConfig) WithProducerResultWaitTimeout(d time.Duration) QueueConfig {
	qc.ProducerResultWaitTimeout = d
	return qc
}

// WithName sets the queue name.
func (qc QueueConfig) WithName(name string) QueueConfig {
	qc.Name = name
	return qc
}

// WithLogger sets a custom slog.Logger.
func (qc QueueConfig) WithLogger(l *slog.Logger) QueueConfig {
	qc.Logger = l
	return qc
}

// WithConsumerGroupName sets the consumer group name.
func (qc QueueConfig) WithConsumerGroupName(groupName string) QueueConfig {
	qc.ConsumerGroupName = groupName
	return qc
}

// WithConsumerIdentifier sets the unique consumer identifier.
func (qc QueueConfig) WithConsumerIdentifier(id string) QueueConfig {
	qc.ConsumerIdentifier = id
	return qc
}

// Task represents a task payload (used by producer).
type Task struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// Result represents a task result.
type Result struct {
	TaskID string          `json:"task_id"`
	Data   json.RawMessage `json:"data,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// Queue represents the message queue.
type Queue struct {
	redisClient   redis.UniversalClient
	config        QueueConfig
	taskStreamKey string // Cache the computed task stream key
}

// NewQueue creates a new Queue instance.
func NewQueue(client redis.UniversalClient, config QueueConfig) *Queue {
	if config.Name == "" {
		config.Name = uuid.NewString()
		if config.Logger != nil {
			config.Logger.Info("Queue name not provided, defaulted to UUID", "queueName", config.Name)
		}
	}

	if config.Logger == nil {
		config.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	if config.ConsumerGroupName == "" {
		config.ConsumerGroupName = config.Name + ":" + defaultConsumerGroupNameSuffix
		config.Logger.Info("ConsumerGroupName not provided, defaulted", "consumerGroupName", config.ConsumerGroupName)
	}
	if config.ConsumerIdentifier == "" {
		config.ConsumerIdentifier = uuid.NewString()
		config.Logger.Info("ConsumerIdentifier not provided, defaulted to UUID", "consumerIdentifier", config.ConsumerIdentifier)
	}

	q := &Queue{
		redisClient: client,
		config:      config,
	}
	q.taskStreamKey = q.getKey(keyTaskStreamSuffix)

	err := q.redisClient.XGroupCreateMkStream(context.Background(), q.taskStreamKey, q.config.ConsumerGroupName, "0").Err()
	if err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			q.config.Logger.Error("Failed to create consumer group for task stream (and error is not BUSYGROUP)",
				"stream", q.taskStreamKey, "group", q.config.ConsumerGroupName, "err", err)
		} else {
			q.config.Logger.Info("Consumer group already exists or was successfully created",
				"stream", q.taskStreamKey, "group", q.config.ConsumerGroupName)
		}
	} else {
		q.config.Logger.Info("Successfully created consumer group for task stream",
			"stream", q.taskStreamKey, "group", q.config.ConsumerGroupName)
	}
	return q
}

// Config returns the queue's configuration.
func (q *Queue) Config() QueueConfig {
	return q.config
}

// Name returns the base name of the queue.
func (q *Queue) Name() string {
	return q.config.Name
}

// getKey constructs a Redis key.
func (q *Queue) getKey(format string, args ...interface{}) string {
	return fmt.Sprintf(keyPrefix+q.config.Name+":"+format, args...)
}

// consumeScript is a Lua script to atomically check and update task status.
var consumeScript = redis.NewScript(`
local status_key = KEYS[1]
local old_status = ARGV[1] -- Note: ARGV[1] in script corresponds to taskID in Go, but script uses it as old_status for clarity if taskID was ARGV[0]
-- Corrected ARGV mapping based on Go code:
-- ARGV[1] = taskID (for logging/internal use, not key construction)
-- ARGV[2] = old_status (e.g. "pending")
-- ARGV[3] = new_status (e.g. "processing")
-- ARGV[4] = expiry_seconds
-- ARGV[5] = processing_status_check (e.g. "processing", for "already_done_or_processing_by_other")
-- ARGV[6] = completed_status_check (e.g. "completed", for "already_done_or_processing_by_other")

local expected_old_status = ARGV[2]
local new_processing_status = ARGV[3]
local expiry = tonumber(ARGV[4])
local check_processing_status = ARGV[5]
local check_completed_status = ARGV[6]
-- local task_id_arg = ARGV[1] -- available if needed

local current_status = redis.call('GET', status_key)

if current_status == expected_old_status then
    local set_ok = redis.call('SET', status_key, new_processing_status, 'EX', expiry, 'XX')
    if set_ok then
        return 'processing'
    else
        return 'race_or_expired'
    end
elseif current_status == new_processing_status then
    return 'already_processing_by_self_or_claimed'
elseif current_status == check_processing_status or current_status == check_completed_status or current_status == taskStatusFailed then -- taskStatusFailed is a global constant
    return 'already_done_or_processing_by_other'
else
    if current_status == false then
        return 'expired_or_missing'
    end
    return 'invalid_status_unexpected'
end
`)

// Produce adds a task to the queue.
func (q *Queue) Produce(ctx context.Context, taskID string, payload []byte) (*Result, <-chan *Result, error) {
	logger := q.config.Logger
	payloadJSON := json.RawMessage(payload)

	statusKey := q.getKey(keyStatus, taskID)
	resultKey := q.getKey(keyResult, taskID)

	setOk, err := q.redisClient.SetNX(ctx, statusKey, taskStatusPending, q.config.TaskExpiry).Result()
	if err != nil {
		logger.Error("Redis SETNX error for task status", "taskID", taskID, "statusKey", statusKey, "err", err)
		return nil, nil, fmt.Errorf("redis SETNX error for task %s: %w", taskID, err)
	}

	var action string
	if setOk {
		action = "queued_to_stream"
		logger.Info("Task status set to pending via SETNX. Adding task to stream.", "taskID", taskID)
		_, xaddErr := q.redisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: q.taskStreamKey,
			Values: map[string]interface{}{
				taskStreamMessageFieldTaskID:  taskID,
				taskStreamMessageFieldPayload: string(payloadJSON),
			},
		}).Result()
		if xaddErr != nil {
			logger.Error("CRITICAL - Failed to XADD task to stream after setting status to pending", "taskID", taskID, "stream", q.taskStreamKey, "err", xaddErr)
			q.redisClient.Set(ctx, statusKey, taskStatusFailed, q.config.TaskExpiry)
			return nil, nil, fmt.Errorf("failed to XADD task %s to stream: %w", taskID, xaddErr)
		}
		logger.Info("Task added to stream.", "taskID", taskID, "stream", q.taskStreamKey)
	} else {
		currentStatus, getErr := q.redisClient.Get(ctx, statusKey).Result()
		if getErr != nil && !errors.Is(getErr, redis.Nil) {
			logger.Error("Failed to GET status for existing task after SETNX failed", "taskID", taskID, "statusKey", statusKey, "err", getErr)
			return nil, nil, fmt.Errorf("failed to get status for existing task %s: %w", taskID, getErr)
		}
		if errors.Is(getErr, redis.Nil) {
			logger.Warn("Task status key expired between SETNX and GET. Treating as if task needs queuing for result listening.", "taskID", taskID)
			action = "waiting_for_result_stream"
		} else {
			switch currentStatus {
			case taskStatusCompleted, taskStatusFailed:
				action = "completed_or_failed_direct_result"
				logger.Info("Task already completed or failed, fetching result directly.", "taskID", taskID, "currentStatus", currentStatus)
				resultData, getResErr := q.redisClient.Get(ctx, resultKey).Bytes()
				if errors.Is(getResErr, redis.Nil) {
					logger.Warn("Task status is completed/failed, but result data not found (likely expired).", "taskID", taskID, "currentStatus", currentStatus, "resultKey", resultKey)
					return &Result{TaskID: taskID, Error: fmt.Sprintf("task %s is %s, but result data expired", taskID, currentStatus)}, nil, nil
				} else if getResErr != nil {
					logger.Error("Failed to get result data for task", "taskID", taskID, "currentStatus", currentStatus, "resultKey", resultKey, "err", getResErr)
					return nil, nil, fmt.Errorf("failed to get result for task %s (%s): %w", taskID, currentStatus, getResErr)
				}
				var res Result
				if umErr := json.Unmarshal(resultData, &res); umErr != nil {
					logger.Warn("Failed to unmarshal direct result data for task", "taskID", taskID, "err", umErr)
					res.TaskID = taskID
					res.Error = fmt.Sprintf("failed to unmarshal direct result: %v", umErr)
				}
				return &res, nil, nil
			case taskStatusPending, taskStatusProcessing:
				action = "waiting_for_result_stream"
				logger.Info("Task already pending or processing, producer will wait for result via result stream.", "taskID", taskID, "currentStatus", currentStatus)
			default:
				logger.Warn("Unexpected task status found. Producer will wait for result via result stream.", "taskID", taskID, "currentStatus", currentStatus)
				action = "waiting_for_result_stream"
			}
		}
	}

	if action == "queued_to_stream" || action == "waiting_for_result_stream" {
		resultCh := make(chan *Result, 1)
		resultStreamKey := q.getKey(keyStreamResultFormat, taskID)

		go func() {
			goroutineLogger := q.config.Logger.With("goroutine", "producerResultListener", "taskID", taskID)
			defer close(resultCh)

			waitResultCtx, cancelWaitResult := context.WithTimeout(ctx, q.config.ProducerResultWaitTimeout)
			defer cancelWaitResult()

			goroutineLogger.Info("Attempting to fetch existing result or wait on result stream", "resultStreamKey", resultStreamKey)

			existingMsgs, revRangeErr := q.redisClient.XRevRangeN(waitResultCtx, resultStreamKey, "+", "-", 1).Result()
			if revRangeErr == nil && len(existingMsgs) == 1 {
				msg := existingMsgs[0]
				var res Result
				if val, ok := msg.Values["result"].(string); ok {
					if umErr := json.Unmarshal([]byte(val), &res); umErr == nil {
						goroutineLogger.Info("Fetched existing result from result stream via XRevRange", "messageID", msg.ID)
						resultCh <- &res
						return
					} else {
						goroutineLogger.Warn("Failed to unmarshal existing result stream message. Will proceed to XREAD.", "messageID", msg.ID, "err", umErr)
					}
				} else {
					goroutineLogger.Warn("Existing result stream message 'result' field not a string. Will proceed to XREAD.", "messageID", msg.ID)
				}
			} else if revRangeErr != nil && !errors.Is(revRangeErr, redis.Nil) {
				goroutineLogger.Error("Error checking existing result stream (XRevRange)", "err", revRangeErr)
				resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("error checking existing result stream: %v", revRangeErr)}
				return
			}

			goroutineLogger.Info("No valid immediate result from result stream XRevRange, proceeding to XREAD.", "timeout", q.config.ProducerResultWaitTimeout)
			xreadStartID := "0-0"
			xreadArgs := &redis.XReadArgs{
				Streams: []string{resultStreamKey, xreadStartID},
				Count:   1,
				Block:   0, // Rely on waitResultCtx for timeout
			}

			cmdResult, readErr := q.redisClient.XRead(waitResultCtx, xreadArgs).Result()
			if readErr != nil {
				errMsg := fmt.Sprintf("wait for result stream message timed out after %v", q.config.ProducerResultWaitTimeout)
				if errors.Is(readErr, context.DeadlineExceeded) || errors.Is(waitResultCtx.Err(), context.DeadlineExceeded) {
					goroutineLogger.Warn("Wait for result stream message timed out (context deadline)", "err", readErr)
				} else if errors.Is(readErr, redis.Nil) { // Should be less common with Block:0 if ctx timeout is shorter
					goroutineLogger.Warn("Wait for result stream message timed out (XRead redis.Nil)")
				} else if errors.Is(readErr, context.Canceled) {
					goroutineLogger.Info("Context cancelled while waiting for result stream message.", "err", readErr)
					errMsg = readErr.Error()
				} else {
					goroutineLogger.Error("Failed to read from result stream", "err", readErr)
					errMsg = fmt.Sprintf("failed to read from result stream: %v", readErr)
				}
				resultCh <- &Result{TaskID: taskID, Error: errMsg}
				return
			}

			if len(cmdResult) > 0 && len(cmdResult[0].Messages) > 0 {
				msg := cmdResult[0].Messages[0]
				goroutineLogger.Info("Received message from result stream", "messageID", msg.ID)
				var res Result
				if val, ok := msg.Values["result"].(string); ok {
					if umErr := json.Unmarshal([]byte(val), &res); umErr != nil {
						goroutineLogger.Error("Failed to unmarshal received result stream message", "messageID", msg.ID, "err", umErr)
						resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("failed to unmarshal result stream message: %v", umErr)}
					} else {
						resultCh <- &res
					}
				} else {
					goroutineLogger.Error("Received result stream message 'result' field not a string", "messageID", msg.ID)
					resultCh <- &Result{TaskID: taskID, Error: "received result stream message 'result' field not a string"}
				}
			} else {
				errMsg := "received no message from result stream unexpectedly"
				if errors.Is(waitResultCtx.Err(), context.DeadlineExceeded) {
					goroutineLogger.Warn("XRead on result stream returned no message and no error, context deadline exceeded")
					errMsg = fmt.Sprintf("wait for result stream message timed out after %v (context check)", q.config.ProducerResultWaitTimeout)
				} else {
					goroutineLogger.Error("XRead on result stream returned no message and no error (unexpected)")
				}
				resultCh <- &Result{TaskID: taskID, Error: errMsg}
			}
		}()
		return nil, resultCh, nil
	}

	logger.Error("Unexpected state after producer logic, no action taken to wait for result.", "taskID", taskID, "action", action)
	return nil, nil, fmt.Errorf("unexpected producer state for task %s, action: %s", taskID, action)
}

// ProduceBlock calls Produce and blocks until the result is available or an error occurs.
func (q *Queue) ProduceBlock(ctx context.Context, taskID string, payload []byte) (*Result, error) {
	logger := q.config.Logger
	immediateResult, resultCh, err := q.Produce(ctx, taskID, payload)
	if err != nil {
		return nil, fmt.Errorf("produce failed: %w", err)
	}
	if immediateResult != nil {
		if immediateResult.Error != "" {
			return immediateResult, errors.New(immediateResult.Error)
		}
		return immediateResult, nil
	}
	if resultCh != nil {
		select {
		case finalResult, ok := <-resultCh:
			if !ok {
				logger.Error("Result channel closed unexpectedly in ProduceBlock", "taskID", taskID)
				return nil, errors.New("result channel closed unexpectedly")
			}
			if finalResult == nil {
				logger.Error("Result channel delivered nil result in ProduceBlock", "taskID", taskID)
				return nil, errors.New("result channel delivered nil result")
			}
			if finalResult.Error != "" {
				return finalResult, errors.New(finalResult.Error)
			}
			return finalResult, nil
		case <-ctx.Done():
			logger.Warn("ProduceBlock cancelled by context while waiting for result channel", "taskID", taskID, "err", ctx.Err())
			return nil, fmt.Errorf("produce block cancelled by context: %w", ctx.Err())
		}
	}
	logger.Error("Produce returned neither immediate result nor channel in ProduceBlock", "taskID", taskID)
	return nil, errors.New("produce returned neither immediate result nor result channel")
}

// ProcessTaskFunc is a function type that handles task processing.
type ProcessTaskFunc func(ctx context.Context, taskID string, payload []byte) ([]byte, error)

// Consume processes tasks from the queue using XREADGROUP.
func (q *Queue) Consume(ctx context.Context, processFunc ProcessTaskFunc) {
	logger := q.config.Logger.With("consumerIdentifier", q.config.ConsumerIdentifier, "group", q.config.ConsumerGroupName, "taskStream", q.taskStreamKey)
	logger.Info("Consumer starting")

	logger.Info("Starting phase 1: processing own pending messages (from ID '0')")
	for {
		select {
		case <-ctx.Done():
			logger.Info("Context cancelled during pending message processing phase.", "err", ctx.Err())
			return
		default:
		}
		messages, err := q.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    q.config.ConsumerGroupName,
			Consumer: q.config.ConsumerIdentifier,
			Streams:  []string{q.taskStreamKey, "0"},
			Count:    1,
			Block:    0, // Non-blocking check for PEL
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				logger.Info("No more pending messages in PEL for this consumer.")
				break
			}
			logger.Error("Error reading pending messages from task stream", "err", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if len(messages) == 0 || len(messages[0].Messages) == 0 {
			logger.Info("No more pending messages in PEL for this consumer (empty batch).")
			break
		}
		q.processStreamMessages(ctx, messages[0].Messages, processFunc, logger)
	}
	logger.Info("Finished phase 1. Starting phase 2: processing new messages (from ID '>')")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context cancelled during new message processing phase.", "err", ctx.Err())
			return
		default:
		}

		readGroupArgs := &redis.XReadGroupArgs{
			Group:    q.config.ConsumerGroupName,
			Consumer: q.config.ConsumerIdentifier,
			Streams:  []string{q.taskStreamKey, ">"},
			Count:    1,
			Block:    5 * time.Second, // Redis server-side block. Go ctx will interrupt if it expires sooner.
		}

		streamMessages, err := q.redisClient.XReadGroup(ctx, readGroupArgs).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logger.Info("Context cancelled or deadline exceeded while reading new messages.", "err", err)
				return
			}
			logger.Error("Error reading new messages from task stream", "err", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(streamMessages) > 0 && len(streamMessages[0].Messages) > 0 {
			q.processStreamMessages(ctx, streamMessages[0].Messages, processFunc, logger)
		}
	}
}

// processStreamMessages helper to process a batch of messages and ACK them.
func (q *Queue) processStreamMessages(ctx context.Context, msgs []redis.XMessage, processFunc ProcessTaskFunc, logger *slog.Logger) {
	for _, msg := range msgs {
		taskID, taskIDOk := msg.Values[taskStreamMessageFieldTaskID].(string)
		payloadStr, payloadOk := msg.Values[taskStreamMessageFieldPayload].(string)

		if !taskIDOk || !payloadOk {
			logger.Error("Invalid message format in task stream: missing taskID or payload", "messageID", msg.ID, "values", msg.Values)
			if ackErr := q.redisClient.XAck(ctx, q.taskStreamKey, q.config.ConsumerGroupName, msg.ID).Err(); ackErr != nil {
				logger.Error("Failed to ACK malformed message", "messageID", msg.ID, "err", ackErr)
			}
			continue
		}
		payloadData := []byte(payloadStr)
		msgLogger := logger.With("taskID", taskID, "messageID", msg.ID)
		msgLogger.Info("Consumer picked up task from stream")

		statusKey := q.getKey(keyStatus, taskID)

		// ARGV for Lua: 1:taskID, 2:old_status, 3:new_status, 4:expiry, 5:processing_check, 6:completed_check
		scriptArgs := []interface{}{
			taskID,                             // ARGV[1]
			taskStatusPending,                  // ARGV[2] expected_old_status
			taskStatusProcessing,               // ARGV[3] new_processing_status
			int(q.config.TaskExpiry.Seconds()), // ARGV[4] expiry
			taskStatusProcessing,               // ARGV[5] check_processing_status
			taskStatusCompleted,                // ARGV[6] check_completed_status
		}
		actionCmd := consumeScript.Run(ctx, q.redisClient, []string{statusKey}, scriptArgs...)
		consumeAction, err := actionCmd.Result()
		if err != nil {
			msgLogger.Error("Consume script execution error. Task will not be processed by this attempt, and NOT ACKed. Will be retried or claimed later.", "err", err)
			continue
		}
		actionStr, ok := consumeAction.(string)
		if !ok {
			logger.Error("Consume script returned non-string action, skipping task.", "taskID", taskID, "actionType", fmt.Sprintf("%T", consumeAction))
			continue
		}
		msgLogger.Info("Consume script returned action", "action", actionStr)

		shouldProcess := false
		switch actionStr {
		case "processing":
			shouldProcess = true
		case "already_processing_by_self_or_claimed":
			msgLogger.Info("Task status indicates already claimed or processing by self. Will ACK.")
		case "already_done_or_processing_by_other":
			msgLogger.Info("Task already completed, failed, or being processed by another. Will ACK.")
		case "expired_or_missing":
			msgLogger.Warn("Task status key was missing or expired when consumeScript ran. Will ACK.")
		case "race_or_expired":
			msgLogger.Warn("Consume script indicated a race or status key expiry during SET. Will ACK.")
		case "invalid_status_unexpected":
			fallthrough
		default:
			msgLogger.Error("Consume script returned unexpected action. Will ACK.", "action", actionStr)
		}

		if shouldProcess {
			msgLogger.Info("Executing processing logic for task...")
			var resultData []byte
			var processErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						msgLogger.Error("Panic occurred during processFunc, marking task failed", "panic", r)
						processErr = fmt.Errorf("processFunc panic: %v", r)
					}
				}()
				resultData, processErr = processFunc(ctx, taskID, payloadData)
			}()

			var finalStatus string
			var taskResult *Result
			if processErr != nil {
				msgLogger.Error("Task processing failed", "err", processErr)
				finalStatus = taskStatusFailed
				taskResult = &Result{TaskID: taskID, Error: processErr.Error()}
			} else {
				msgLogger.Info("Task processing successful")
				finalStatus = taskStatusCompleted
				taskResult = &Result{TaskID: taskID, Data: json.RawMessage(resultData)}
			}
			q.updateStatusAndStreamResult(ctx, taskID, finalStatus, taskResult)
		}

		if ackErr := q.redisClient.XAck(ctx, q.taskStreamKey, q.config.ConsumerGroupName, msg.ID).Err(); ackErr != nil {
			msgLogger.Error("Failed to ACK message from task stream. This may lead to reprocessing.", "err", ackErr)
		} else {
			msgLogger.Info("Successfully ACKed message from task stream.")
		}
	}
}

// updateStatusAndStreamResult updates the task's final status, stores the result,
// and publishes the result to the task-specific result stream for producers.
func (q *Queue) updateStatusAndStreamResult(ctx context.Context, taskID string, finalStatus string, result *Result) {
	logger := q.config.Logger.With("taskID", taskID)
	resultKey := q.getKey(keyResult, taskID)
	statusKey := q.getKey(keyStatus, taskID)
	resultStreamKey := q.getKey(keyStreamResultFormat, taskID)

	pipe := q.redisClient.Pipeline()

	resultJSON, err := json.Marshal(result)
	if err != nil {
		logger.Error("Failed to marshal final result for storage/streaming", "err", err)
		errorResult := &Result{TaskID: taskID, Error: fmt.Sprintf("internal: failed to marshal final result: %v", err)}
		resultJSON, _ = json.Marshal(errorResult)
	}

	if resultJSON != nil {
		pipe.Set(ctx, resultKey, resultJSON, q.config.TaskExpiry)
	}
	pipe.Set(ctx, statusKey, finalStatus, q.config.TaskExpiry)

	if resultJSON != nil {
		xaddArgs := &redis.XAddArgs{
			Stream: resultStreamKey,
			ID:     "*",
			Values: map[string]interface{}{"result": string(resultJSON)},
		}
		pipe.XAdd(ctx, xaddArgs)
		pipe.Expire(ctx, resultStreamKey, q.config.TaskExpiry)
	} else {
		logger.Error("resultJSON is nil, not adding to result stream or setting keys.")
	}

	cmders, execErr := pipe.Exec(ctx)
	if execErr != nil {
		logger.Error("Pipeline failed for final status update, result storage, or result streaming", "err", execErr)
		for i, cmder := range cmders {
			if cmder.Err() != nil {
				logger.Error("Pipeline command failed", "commandIndex", i, "commandName", cmder.Name(), "commandErr", cmder.Err())
			}
		}
	} else {
		logger.Info("Task final status updated, result stored, and result published to result stream.", "finalStatus", finalStatus, "resultStreamKey", resultStreamKey)
	}
}

// --- End of Queue Implementation ---
