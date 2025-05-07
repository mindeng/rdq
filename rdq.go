package rdq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- Start of Queue Implementation ---

const (
	taskStatusPending    = "pending"
	taskStatusProcessing = "processing"
	taskStatusCompleted  = "completed"
	taskStatusFailed     = "failed"

	keyStatus             = "task:{%s}:status"
	keyPayload            = "task:{%s}:payload"
	keyResult             = "task:{%s}:result"
	keyStreamResultFormat = "stream:task:{%s}:results"

	keyQueue = "queue:tasks"

	// Default prefix for internal Redis keys
	defaultKeyPrefix = "_rdq:"
)

// QueueConfig holds configuration options for the Queue.
type QueueConfig struct {
	// TaskExpiry is the time after which task status, payload, result, and stream keys expire.
	// Should be long enough to cover max processing time + waiting time.
	TaskExpiry time.Duration

	// ProducerWaitTimeout is the maximum time a producer will wait for a task result via Stream.
	ProducerWaitTimeout time.Duration

	// ConsumerBRPOPTimeout is the timeout for the blocking pop operation.
	// A small non-zero timeout allows the consumer to check context cancellation periodically.
	ConsumerBRPOPTimeout time.Duration

	KeyPrefix string
	Logger    *slog.Logger // Logger instance for this queue
}

// DefaultQueueConfig returns a QueueConfig with default values.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		TaskExpiry:           5 * time.Minute,
		ProducerWaitTimeout:  60 * time.Second,
		ConsumerBRPOPTimeout: 1 * time.Second,
		KeyPrefix:            defaultKeyPrefix,
		Logger:               slog.New(slog.NewJSONHandler(os.Stdout, nil)), // Default logger
	}
}

// WithTaskExpiry sets the TaskExpiry duration.
func (qc QueueConfig) WithTaskExpiry(d time.Duration) QueueConfig {
	qc.TaskExpiry = d
	return qc
}

// WithProducerWaitTimeout sets the ProducerWaitTimeout duration.
func (qc QueueConfig) WithProducerWaitTimeout(d time.Duration) QueueConfig {
	qc.ProducerWaitTimeout = d
	return qc
}

// WithConsumerBRPOPTimeout sets the ConsumerBRPOPTimeout duration.
func (qc QueueConfig) WithConsumerBRPOPTimeout(d time.Duration) QueueConfig {
	qc.ConsumerBRPOPTimeout = d
	return qc
}

// WithKeyPrefix sets the KeyPrefix string.
func (qc QueueConfig) WithKeyPrefix(p string) QueueConfig {
	qc.KeyPrefix = p
	return qc
}

// WithLogger sets a custom slog.Logger.
func (qc QueueConfig) WithLogger(l *slog.Logger) QueueConfig {
	qc.Logger = l
	return qc
}

// Task represents a task payload.
type Task struct {
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

// Result represents a task result.
type Result struct {
	TaskID string          `json:"task_id"`
	Data   json.RawMessage `json:"data,omitempty"`
	Error  string          `json:"error,omitempty"` // Use Error field to signal processing/fetch error
}

// Queue represents the message queue.
type Queue struct {
	redisClient redis.UniversalClient
	config      QueueConfig // Holds configuration including the logger
}

// NewQueue creates a new Queue instance.
// If config.Logger is nil, a default slog.Logger will be used.
func NewQueue(client redis.UniversalClient, config QueueConfig) *Queue {
	if config.KeyPrefix != "" && config.KeyPrefix[len(config.KeyPrefix)-1] != ':' {
		config.KeyPrefix += ":"
	} else if config.KeyPrefix == "" {
		config.KeyPrefix = defaultKeyPrefix
	}

	// Ensure logger is initialized
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil)) // Default if not provided
	}

	return &Queue{
		redisClient: client,
		config:      config,
	}
}

func (q *Queue) getKey(format string, args ...interface{}) string {
	prefixedFormat := q.config.KeyPrefix + format
	return fmt.Sprintf(prefixedFormat, args...)
}

// KEYS: [status_key]
// ARGV: [task_id, old_status, new_status, expiry_seconds, processing_status, completed_status]
var consumeScript = redis.NewScript(`
local status_key = KEYS[1]
local task_id = ARGV[1]
local old_status = ARGV[2]
local new_status = ARGV[3]
local expiry = tonumber(ARGV[4])
local processing_status = ARGV[5]
local completed_status = ARGV[6]

local current_status = redis.call('GET', status_key)

if current_status == old_status then
    -- Status is as expected, transition to new status
    -- Use XX to ensure the key exists (prevents setting if it expired between GET and SET)
    local set_ok = redis.call('SET', status_key, new_status, 'EX', expiry, 'XX')
    if set_ok then
        return 'processing'
    else
        -- Should not happen if GET returned old_status and key didn't expire immediately after
        -- but indicates a race condition or quick expiry.
        return 'race'
    end
elseif current_status == processing_status or current_status == completed_status then
    -- Check if already processing or completed by another consumer/retry
    return 'already_done'
else
    -- Status is not the expected old_status (e.g., failed, expired, not found)
    return 'invalid_status'
end
`)

// Produce adds a task to the queue.
// If the result is immediately available (task already completed/failed), it returns the result directly.
// Otherwise, it returns a channel that will deliver the result or an error later.
// Returns (immediateResult *Result, resultChannel <-chan *Result, err error).
func (q *Queue) Produce(ctx context.Context, taskID string, payload []byte) (*Result, <-chan *Result, error) {
	logger := q.config.Logger // Use logger from config
	payloadJSON, err := json.Marshal(json.RawMessage(payload))
	if err != nil {
		logger.Error("Failed to marshal payload", "taskID", taskID, "err", err)
		return nil, nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	statusKey := q.getKey(keyStatus, taskID)
	payloadKey := q.getKey(keyPayload, taskID)
	resultKey := q.getKey(keyResult, taskID)
	queueKey := q.getKey(keyQueue)

	setOk, err := q.redisClient.SetNX(ctx, statusKey, taskStatusPending, q.config.TaskExpiry).Result()
	if err != nil {
		logger.Error("Redis SETNX error for task", "taskID", taskID, "key", statusKey, "err", err)
		return nil, nil, fmt.Errorf("redis SETNX error for task %s: %w", taskID, err)
	}

	var action string
	if setOk {
		action = "queued"
		logger.Info("Task status set to pending. Storing payload and queueing.", "taskID", taskID)

		pipe := q.redisClient.Pipeline()
		pipe.Set(ctx, payloadKey, payloadJSON, q.config.TaskExpiry)
		pipe.LPush(ctx, queueKey, taskID)
		_, execErr := pipe.Exec(ctx)
		if execErr != nil {
			logger.Error("CRITICAL - Failed to store payload or queue after setting pending", "taskID", taskID, "err", execErr)
			q.redisClient.Set(ctx, statusKey, taskStatusFailed, q.config.TaskExpiry) // Best effort
			return nil, nil, fmt.Errorf("failed to store payload or queue task %s: %w", taskID, execErr)
		}
		logger.Info("Payload stored and task queued.", "taskID", taskID)
	} else {
		status, getErr := q.redisClient.Get(ctx, statusKey).Result()
		if getErr != nil && getErr != redis.Nil {
			logger.Error("Failed to get status for existing task", "taskID", taskID, "key", statusKey, "err", getErr)
			return nil, nil, fmt.Errorf("failed to get status for existing task %s: %w", taskID, getErr)
		}

		switch status {
		case taskStatusCompleted, taskStatusFailed:
			action = "completed_or_failed"
			logger.Info("Task already completed or failed, fetching result directly.", "taskID", taskID, "status", status)
			resultData, getResErr := q.redisClient.Get(ctx, resultKey).Bytes()
			if getResErr == redis.Nil {
				logger.Warn("Task status is completed/failed, but result not found (likely expired or inconsistent)", "taskID", taskID, "status", status, "resultKey", resultKey)
				return nil, nil, fmt.Errorf("task %s status is %s, but result not found (likely expired or inconsistent)", taskID, status)
			} else if getResErr != nil {
				logger.Error("Failed to get result for completed/failed task", "taskID", taskID, "status", status, "resultKey", resultKey, "err", getResErr)
				return nil, nil, fmt.Errorf("failed to get result for task %s (%s): %w", taskID, status, getResErr)
			}

			var result Result
			if umErr := json.Unmarshal(resultData, &result); umErr != nil {
				logger.Warn("Failed to unmarshal direct result for task", "taskID", taskID, "err", umErr)
				result.TaskID = taskID
				result.Error = fmt.Sprintf("failed to unmarshal direct result: %v", umErr)
			}
			return &result, nil, nil

		case taskStatusPending, taskStatusProcessing:
			action = "waiting"
			logger.Info("Task already pending or processing, waiting for result via stream.", "taskID", taskID, "status", status)
		case "":
			logger.Warn("Status key did not exist after SETNX returned false. Race condition? Treating as waiting.", "taskID", taskID, "statusKey", statusKey)
			action = "waiting"
		default:
			logger.Warn("Unexpected task status. Treating as waiting.", "taskID", taskID, "status", status)
			action = "waiting"
		}
	}

	if action == "queued" || action == "waiting" {
		resultCh := make(chan *Result, 1)
		streamKey := q.getKey(keyStreamResultFormat, taskID)

		go func() {
			// Use the logger from the Queue instance within the goroutine
			goroutineLogger := q.config.Logger
			defer close(resultCh)

			waitOverallCtx, cancelOverallWait := context.WithTimeout(ctx, q.config.ProducerWaitTimeout)
			defer cancelOverallWait()

			goroutineLogger.Info("Attempting to fetch existing result or wait on stream", "taskID", taskID, "streamKey", streamKey)

			existingMsgs, revRangeErr := q.redisClient.XRevRangeN(waitOverallCtx, streamKey, "+", "-", 1).Result()
			if revRangeErr == nil && len(existingMsgs) == 1 {
				msg := existingMsgs[0]
				var res Result
				if val, ok := msg.Values["result"].(string); ok {
					if umErr := json.Unmarshal([]byte(val), &res); umErr == nil {
						goroutineLogger.Info("Fetched existing result from stream via XRevRange", "taskID", taskID, "streamMessageID", msg.ID)
						resultCh <- &res
						return
					} else {
						goroutineLogger.Warn("Failed to unmarshal existing stream result from XRevRange. Will proceed to XREAD.", "taskID", taskID, "streamMessageID", msg.ID, "err", umErr)
					}
				} else {
					goroutineLogger.Warn("Existing stream message 'result' field not a string (XRevRange). Will proceed to XREAD.", "taskID", taskID, "streamMessageID", msg.ID)
				}
			} else if revRangeErr != nil && revRangeErr != redis.Nil {
				goroutineLogger.Error("Error checking existing stream result (XRevRange)", "taskID", taskID, "streamKey", streamKey, "err", revRangeErr)
				resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("error checking existing stream result: %v", revRangeErr)}
				return
			}

			goroutineLogger.Info("No valid immediate result from XRevRange, proceeding to XREAD.", "taskID", taskID, "streamKey", streamKey, "timeout", q.config.ProducerWaitTimeout)
			xreadStartID := "0-0"
			xreadArgs := &redis.XReadArgs{
				Streams: []string{streamKey, xreadStartID},
				Count:   1,
				Block:   q.config.ProducerWaitTimeout,
			}

			cmdResult, readErr := q.redisClient.XRead(waitOverallCtx, xreadArgs).Result()
			if readErr != nil {
				errMsg := fmt.Sprintf("wait for stream message timed out after %v", q.config.ProducerWaitTimeout)
				if errors.Is(readErr, context.DeadlineExceeded) || (waitOverallCtx.Err() == context.DeadlineExceeded) {
					goroutineLogger.Warn("Wait for stream message timed out (context deadline)", "taskID", taskID, "streamKey", streamKey, "timeout", q.config.ProducerWaitTimeout, "err", readErr)
				} else if errors.Is(readErr, redis.Nil) {
					goroutineLogger.Warn("Wait for stream message timed out (XRead redis.Nil)", "taskID", taskID, "streamKey", streamKey, "timeout", q.config.ProducerWaitTimeout)
				} else {
					goroutineLogger.Error("Failed to read from stream", "taskID", taskID, "streamKey", streamKey, "err", readErr)
					errMsg = fmt.Sprintf("failed to read from stream: %v", readErr)
				}
				resultCh <- &Result{TaskID: taskID, Error: errMsg}
				return
			}

			if len(cmdResult) > 0 && len(cmdResult[0].Messages) > 0 {
				msg := cmdResult[0].Messages[0]
				goroutineLogger.Info("Received stream message", "taskID", taskID, "streamKey", streamKey, "streamMessageID", msg.ID)
				var res Result
				if val, ok := msg.Values["result"].(string); ok {
					if umErr := json.Unmarshal([]byte(val), &res); umErr != nil {
						goroutineLogger.Error("Failed to unmarshal received stream message", "taskID", taskID, "streamMessageID", msg.ID, "err", umErr)
						resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("failed to unmarshal stream message: %v", umErr)}
					} else {
						resultCh <- &res
					}
				} else {
					goroutineLogger.Error("Received stream message 'result' field not a string", "taskID", taskID, "streamMessageID", msg.ID)
					resultCh <- &Result{TaskID: taskID, Error: "received stream message 'result' field not a string"}
				}
			} else {
				errMsg := "received no message from stream unexpectedly"
				if waitOverallCtx.Err() == context.DeadlineExceeded {
					goroutineLogger.Warn("XRead returned no message and no error, context deadline exceeded", "taskID", taskID, "streamKey", streamKey)
					errMsg = fmt.Sprintf("wait for stream message timed out after %v (context check)", q.config.ProducerWaitTimeout)
				} else {
					goroutineLogger.Error("XRead returned no message and no error (unexpected)", "taskID", taskID, "streamKey", streamKey)
				}
				resultCh <- &Result{TaskID: taskID, Error: errMsg}
			}
		}()
		return nil, resultCh, nil
	}

	logger.Error("Unexpected state after processing task", "taskID", taskID, "action", action)
	return nil, nil, fmt.Errorf("unexpected state after processing task %s, action: %s", taskID, action)
}

// ProduceBlock calls Produce and blocks until the result is available or an error occurs.
func (q *Queue) ProduceBlock(ctx context.Context, taskID string, payload []byte) (*Result, error) {
	logger := q.config.Logger // Use logger from config
	immediateResult, resultCh, err := q.Produce(ctx, taskID, payload)
	if err != nil {
		logger.Error("Produce failed in ProduceBlock", "taskID", taskID, "err", err)
		return nil, fmt.Errorf("produce failed: %w", err)
	}

	if immediateResult != nil {
		if immediateResult.Error != "" {
			logger.Warn("ProduceBlock returning immediate result with error", "taskID", taskID, "resultError", immediateResult.Error)
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
				logger.Warn("ProduceBlock returning final result with error", "taskID", taskID, "resultError", finalResult.Error)
				return finalResult, errors.New(finalResult.Error)
			}
			return finalResult, nil
		case <-ctx.Done():
			logger.Warn("ProduceBlock cancelled by context", "taskID", taskID, "err", ctx.Err())
			return nil, fmt.Errorf("produce block cancelled by context: %w", ctx.Err())
		}
	}
	logger.Error("Produce returned neither immediate result nor channel in ProduceBlock", "taskID", taskID)
	return nil, errors.New("produce returned neither immediate result nor channel")
}

// ProcessTaskFunc is a function type that handles task processing.
type ProcessTaskFunc func(ctx context.Context, taskID string, payload []byte) ([]byte, error)

// Consume processes tasks from the queue. Should be run in a goroutine.
func (q *Queue) Consume(ctx context.Context, processFunc ProcessTaskFunc) {
	logger := q.config.Logger // Use logger from config
	queueKey := q.getKey(keyQueue)
	logger.Info("Consumer starting", "queueKey", queueKey)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Consumer context cancelled, shutting down.", "err", ctx.Err())
			return
		default:
		}

		taskIDs, err := q.redisClient.BRPop(ctx, q.config.ConsumerBRPOPTimeout, queueKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) { // Timeout
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logger.Info("Consumer context cancelled or timed out during BRPOP, shutting down.", "err", err)
				return
			}
			logger.Error("BRPOP error, retrying in 5 seconds.", "queueKey", queueKey, "err", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(taskIDs) != 2 {
			logger.Error("Unexpected BRPOP result", "result", taskIDs)
			continue
		}

		taskID := taskIDs[1]
		logger.Info("Consumer picked up task", "taskID", taskID)

		statusKey := q.getKey(keyStatus, taskID)
		payloadKey := q.getKey(keyPayload, taskID)

		consumeAction, err := consumeScript.Run(
			ctx, q.redisClient,
			[]string{statusKey},
			taskID, taskStatusPending, taskStatusProcessing, int(q.config.TaskExpiry.Seconds()), taskStatusProcessing, taskStatusCompleted,
		).Result()
		if err != nil {
			logger.Error("Consume script error, skipping task.", "taskID", taskID, "err", err)
			continue
		}

		actionStr, ok := consumeAction.(string)
		if !ok {
			logger.Error("Consume script returned non-string action, skipping task.", "taskID", taskID, "actionType", fmt.Sprintf("%T", consumeAction))
			continue
		}
		logger.Info("Consume script returned action", "taskID", taskID, "action", actionStr)

		if actionStr != "processing" {
			logger.Info("Not processing task due to status from script, skipping.", "taskID", taskID, "action", actionStr)
			continue
		}

		payloadData, err := q.redisClient.Get(ctx, payloadKey).Bytes()
		if err != nil {
			errMsg := fmt.Sprintf("payload error: %v", err)
			if errors.Is(err, redis.Nil) {
				logger.Warn("Payload not found (likely expired), marking failed.", "taskID", taskID, "payloadKey", payloadKey)
				errMsg = "payload expired before processing"
			} else {
				logger.Error("Failed to get payload, marking failed.", "taskID", taskID, "payloadKey", payloadKey, "err", err)
			}
			q.updateStatusAndStreamResult(ctx, taskID, taskStatusFailed, &Result{TaskID: taskID, Error: errMsg})
			continue
		}

		logger.Info("Executing processing logic for task...", "taskID", taskID)
		resultData, processErr := processFunc(ctx, taskID, payloadData)

		var finalStatus string
		var taskResult *Result

		if processErr != nil {
			logger.Error("Task processing failed", "taskID", taskID, "err", processErr)
			finalStatus = taskStatusFailed
			taskResult = &Result{TaskID: taskID, Error: processErr.Error()}
		} else {
			logger.Info("Task processing successful", "taskID", taskID)
			finalStatus = taskStatusCompleted
			taskResult = &Result{TaskID: taskID, Data: json.RawMessage(resultData)}
		}
		q.updateStatusAndStreamResult(ctx, taskID, finalStatus, taskResult)
	}
}

// updateStatusAndStreamResult updates status, stores result, adds to stream, and sets stream expiry.
func (q *Queue) updateStatusAndStreamResult(ctx context.Context, taskID string, status string, result *Result) {
	logger := q.config.Logger // Use logger from config
	resultKey := q.getKey(keyResult, taskID)
	statusKey := q.getKey(keyStatus, taskID)
	streamKey := q.getKey(keyStreamResultFormat, taskID)

	pipe := q.redisClient.Pipeline()

	resultJSON, err := json.Marshal(result)
	if err != nil {
		logger.Error("Failed to marshal final result for storage", "taskID", taskID, "err", err)
		errorResult := &Result{TaskID: taskID, Error: fmt.Sprintf("internal: failed to marshal final result: %v", err)}
		resultJSON, _ = json.Marshal(errorResult)
	}

	if resultJSON != nil {
		pipe.Set(ctx, resultKey, resultJSON, q.config.TaskExpiry)
	}
	pipe.Set(ctx, statusKey, status, q.config.TaskExpiry)

	if resultJSON != nil {
		xaddArgs := &redis.XAddArgs{
			Stream: streamKey,
			ID:     "*",
			Values: map[string]interface{}{"result": string(resultJSON)},
		}
		pipe.XAdd(ctx, xaddArgs)
		pipe.Expire(ctx, streamKey, q.config.TaskExpiry)
		logger.Info("Queued XADD to stream and EXPIRE for stream key", "taskID", taskID, "streamKey", streamKey, "expiry", q.config.TaskExpiry)
	} else {
		logger.Error("resultJSON is nil after attempting to marshal error, not adding to stream.", "taskID", taskID)
	}

	cmders, execErr := pipe.Exec(ctx)
	if execErr != nil {
		logger.Error("Pipeline failed for status/result/stream update", "taskID", taskID, "err", execErr)
		// Log individual command errors if available
		for i, cmder := range cmders {
			if cmder.Err() != nil {
				logger.Error("Pipeline command failed", "taskID", taskID, "commandIndex", i, "commandName", cmder.Name(), "commandErr", cmder.Err())
			}
		}
	} else {
		logger.Info("Status updated, result stored, and result streamed.", "taskID", taskID, "newStatus", status, "streamKey", streamKey)
	}
}

// --- End of Queue Implementation ---
