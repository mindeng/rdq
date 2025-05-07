package rdq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	KeyPrefix            string // Prefix for all internal Redis keys
}

// DefaultQueueConfig returns a QueueConfig with default values.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		TaskExpiry:           5 * time.Minute,
		ProducerWaitTimeout:  60 * time.Second,
		ConsumerBRPOPTimeout: 1 * time.Second,
		KeyPrefix:            defaultKeyPrefix,
	}
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
	config      QueueConfig
}

// NewQueue creates a new Queue instance using an existing redis.UniversalClient and configuration.
func NewQueue(client redis.UniversalClient, config QueueConfig) *Queue {
	// Ensure the key prefix ends with a colon for consistent formatting
	if config.KeyPrefix != "" && config.KeyPrefix[len(config.KeyPrefix)-1] != ':' {
		config.KeyPrefix += ":"
	} else if config.KeyPrefix == "" {
		config.KeyPrefix = defaultKeyPrefix
	}

	return &Queue{
		redisClient: client,
		config:      config,
	}
}

// getKey formats an internal Redis key with the configured prefix.
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
	payloadJSON, err := json.Marshal(json.RawMessage(payload))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	statusKey := q.getKey(keyStatus, taskID)
	payloadKey := q.getKey(keyPayload, taskID)
	resultKey := q.getKey(keyResult, taskID)
	queueKey := q.getKey(keyQueue)

	setOk, err := q.redisClient.SetNX(ctx, statusKey, taskStatusPending, q.config.TaskExpiry).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("redis SETNX error for task %s: %w", taskID, err)
	}

	var action string
	if setOk {
		action = "queued"
		log.Printf("Task %s: Status set to pending. Storing payload and queueing.", taskID)

		pipe := q.redisClient.Pipeline()
		pipe.Set(ctx, payloadKey, payloadJSON, q.config.TaskExpiry)
		pipe.LPush(ctx, queueKey, taskID)
		_, execErr := pipe.Exec(ctx)
		if execErr != nil {
			log.Printf("Task %s: CRITICAL - Failed to store payload or queue after setting pending: %v", taskID, execErr)
			q.redisClient.Set(ctx, statusKey, taskStatusFailed, q.config.TaskExpiry) // Best effort
			return nil, nil, fmt.Errorf("failed to store payload or queue task %s: %w", taskID, execErr)
		}
		log.Printf("Task %s: Payload stored and queued.", taskID)
	} else {
		status, getErr := q.redisClient.Get(ctx, statusKey).Result()
		if getErr != nil && getErr != redis.Nil {
			return nil, nil, fmt.Errorf("failed to get status for existing task %s: %w", taskID, getErr)
		}

		switch status {
		case taskStatusCompleted, taskStatusFailed:
			action = "completed_or_failed"
			log.Printf("Task %s: Already %s, fetching result directly.", taskID, status)
			resultData, getResErr := q.redisClient.Get(ctx, resultKey).Bytes()
			if getResErr == redis.Nil {
				return nil, nil, fmt.Errorf("task %s status is %s, but result not found (likely expired or inconsistent)", taskID, status)
			} else if getResErr != nil {
				return nil, nil, fmt.Errorf("failed to get result for task %s (%s): %w", taskID, status, getResErr)
			}

			var result Result
			if umErr := json.Unmarshal(resultData, &result); umErr != nil {
				log.Printf("Warning: Failed to unmarshal direct result for task %s, error: %v", taskID, umErr)
				result.TaskID = taskID
				result.Error = fmt.Sprintf("failed to unmarshal direct result: %v", umErr)
			}
			return &result, nil, nil

		case taskStatusPending, taskStatusProcessing:
			action = "waiting"
			log.Printf("Task %s: Already pending or processing, waiting for result via stream.", taskID)
		case "":
			log.Printf("Task %s: Status key did not exist after SETNX returned false. Race condition? Treating as waiting.", taskID)
			action = "waiting"
		default:
			log.Printf("Task %s: Unexpected status '%s'. Treating as waiting.", taskID, status)
			action = "waiting"
		}
	}

	if action == "queued" || action == "waiting" {
		resultCh := make(chan *Result, 1)
		streamKey := q.getKey(keyStreamResultFormat, taskID)

		go func() {
			defer close(resultCh)

			waitOverallCtx, cancelOverallWait := context.WithTimeout(ctx, q.config.ProducerWaitTimeout)
			defer cancelOverallWait()

			log.Printf("Task %s: Attempting to fetch existing result or wait on stream %s", taskID, streamKey)

			// 1. Check for existing recent result (for late listeners or quick completions)
			existingMsgs, revRangeErr := q.redisClient.XRevRangeN(waitOverallCtx, streamKey, "+", "-", 1).Result()
			if revRangeErr == nil && len(existingMsgs) == 1 {
				msg := existingMsgs[0]
				var res Result
				if val, ok := msg.Values["result"].(string); ok {
					if umErr := json.Unmarshal([]byte(val), &res); umErr == nil {
						log.Printf("Task %s: Fetched existing result from stream via XRevRange: ID %s", taskID, msg.ID)
						resultCh <- &res
						return
					} else {
						log.Printf("Task %s: Failed to unmarshal existing stream result from XRevRange: %v. Will proceed to XREAD.", taskID, umErr)
					}
				} else {
					log.Printf("Task %s: Existing stream message 'result' field not a string (XRevRange). Will proceed to XREAD.", taskID)
				}
			} else if revRangeErr != nil && revRangeErr != redis.Nil {
				log.Printf("Task %s: Error checking existing stream result (XRevRange): %v", taskID, revRangeErr)
				resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("error checking existing stream result: %v", revRangeErr)}
				return
			}
			// If XRevRange returned redis.Nil, or message was unmarshalable/invalid, proceed to XREAD.

			log.Printf("Task %s: No valid immediate result from XRevRange, proceeding to XREAD on stream %s (timeout: %v)", taskID, streamKey, q.config.ProducerWaitTimeout)

			// 2. Wait for messages.
			// If XRevRange found nothing (or bad data), we read from "0-0" to catch any message,
			// including one that might have arrived between XRevRange and XRead.
			// This addresses the race condition when '$' would have been used.
			// We only want one message from the stream for this wait.
			xreadStartID := "0-0"

			xreadArgs := &redis.XReadArgs{
				Streams: []string{streamKey, xreadStartID}, // Read messages with ID > xreadStartID (for "0-0", means all messages from start)
				Count:   1,                                 // We only need the first one that appears.
				Block:   q.config.ProducerWaitTimeout,      // Redis-level block timeout
			}

			cmdResult, readErr := q.redisClient.XRead(waitOverallCtx, xreadArgs).Result()

			if readErr != nil {
				if errors.Is(readErr, context.DeadlineExceeded) || (waitOverallCtx.Err() == context.DeadlineExceeded) {
					log.Printf("Task %s: Wait for stream message timed out after %v (context deadline)", taskID, q.config.ProducerWaitTimeout)
					resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("wait for stream message timed out after %v", q.config.ProducerWaitTimeout)}
				} else if errors.Is(readErr, redis.Nil) {
					log.Printf("Task %s: Wait for stream message timed out (XRead redis.Nil) after %v", taskID, q.config.ProducerWaitTimeout)
					resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("wait for stream message timed out after %v", q.config.ProducerWaitTimeout)}
				} else {
					log.Printf("Task %s: Failed to read from stream: %v", taskID, readErr)
					resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("failed to read from stream: %v", readErr)}
				}
				return
			}

			if len(cmdResult) > 0 && len(cmdResult[0].Messages) > 0 {
				msg := cmdResult[0].Messages[0]
				log.Printf("Task %s: Received stream message: ID %s", taskID, msg.ID)
				var res Result
				if val, ok := msg.Values["result"].(string); ok {
					if umErr := json.Unmarshal([]byte(val), &res); umErr != nil {
						log.Printf("Task %s: Failed to unmarshal received stream message: %v", taskID, umErr)
						resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("failed to unmarshal stream message: %v", umErr)}
					} else {
						resultCh <- &res
					}
				} else {
					log.Printf("Task %s: Received stream message 'result' field not a string.", taskID)
					resultCh <- &Result{TaskID: taskID, Error: "received stream message 'result' field not a string"}
				}
			} else {
				log.Printf("Task %s: XRead returned no message and no error, implies timeout or context done (unexpected).", taskID)
				errMsg := "received no message from stream unexpectedly"
				if waitOverallCtx.Err() == context.DeadlineExceeded {
					errMsg = fmt.Sprintf("wait for stream message timed out after %v (context check)", q.config.ProducerWaitTimeout)
				}
				resultCh <- &Result{TaskID: taskID, Error: errMsg}
			}
		}()
		return nil, resultCh, nil
	}

	return nil, nil, fmt.Errorf("unexpected state after processing task %s, action: %s", taskID, action)
}

// ProduceBlock calls Produce and blocks until the result is available or an error occurs.
func (q *Queue) ProduceBlock(ctx context.Context, taskID string, payload []byte) (*Result, error) {
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
				return nil, errors.New("result channel closed unexpectedly")
			}
			if finalResult == nil {
				return nil, errors.New("result channel delivered nil result")
			}
			if finalResult.Error != "" {
				return finalResult, errors.New(finalResult.Error)
			}
			return finalResult, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("produce block cancelled by context: %w", ctx.Err())
		}
	}
	return nil, errors.New("produce returned neither immediate result nor channel")
}

// ProcessTaskFunc is a function type that handles task processing.
type ProcessTaskFunc func(ctx context.Context, taskID string, payload []byte) ([]byte, error)

// Consume processes tasks from the queue. Should be run in a goroutine.
func (q *Queue) Consume(ctx context.Context, processFunc ProcessTaskFunc) {
	queueKey := q.getKey(keyQueue)

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer context cancelled, shutting down.")
			return
		default:
		}

		taskIDs, err := q.redisClient.BRPop(ctx, q.config.ConsumerBRPOPTimeout, queueKey).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				log.Println("Consumer context cancelled or timed out during BRPOP, shutting down.")
				return
			}
			log.Printf("BRPOP error: %v. Retrying in 5 seconds.", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(taskIDs) != 2 {
			log.Printf("Unexpected BRPOP result: %v", taskIDs)
			continue
		}

		taskID := taskIDs[1]
		log.Printf("Consumer picked up task: %s", taskID)

		statusKey := q.getKey(keyStatus, taskID)
		payloadKey := q.getKey(keyPayload, taskID)

		consumeAction, err := consumeScript.Run(
			ctx, q.redisClient,
			[]string{statusKey},
			taskID, taskStatusPending, taskStatusProcessing, int(q.config.TaskExpiry.Seconds()), taskStatusProcessing, taskStatusCompleted,
		).Result()
		if err != nil {
			log.Printf("Task %s: Consume script error: %v. Skipping task.", taskID, err)
			continue
		}

		actionStr, ok := consumeAction.(string)
		if !ok {
			log.Printf("Task %s: Consume script returned non-string action: %T. Skipping task.", taskID, consumeAction)
			continue
		}
		log.Printf("Task %s: Consume script returned action: %s", taskID, actionStr)

		if actionStr != "processing" {
			log.Printf("Task %s: Not processing due to status '%s'. Skipping.", taskID, actionStr)
			continue
		}

		payloadData, err := q.redisClient.Get(ctx, payloadKey).Bytes()
		if err != nil {
			errMsg := fmt.Sprintf("payload error: %v", err)
			if errors.Is(err, redis.Nil) {
				log.Printf("Task %s: Payload not found (likely expired). Marking failed.", taskID)
				errMsg = "payload expired before processing"
			} else {
				log.Printf("Task %s: Failed to get payload: %v. Marking failed.", taskID, err)
			}
			q.updateStatusAndStreamResult(ctx, taskID, taskStatusFailed, &Result{TaskID: taskID, Error: errMsg})
			continue
		}

		log.Printf("Task %s: Executing processing logic...", taskID)
		resultData, processErr := processFunc(ctx, taskID, payloadData)

		var finalStatus string
		var taskResult *Result

		if processErr != nil {
			log.Printf("Task %s: Processing failed: %v", taskID, processErr)
			finalStatus = taskStatusFailed
			taskResult = &Result{TaskID: taskID, Error: processErr.Error()}
		} else {
			log.Printf("Task %s: Processing successful.", taskID)
			finalStatus = taskStatusCompleted
			taskResult = &Result{TaskID: taskID, Data: json.RawMessage(resultData)}
		}
		q.updateStatusAndStreamResult(ctx, taskID, finalStatus, taskResult)
	}
}

// updateStatusAndStreamResult updates status, stores result, adds to stream, and sets stream expiry.
func (q *Queue) updateStatusAndStreamResult(ctx context.Context, taskID string, status string, result *Result) {
	resultKey := q.getKey(keyResult, taskID)
	statusKey := q.getKey(keyStatus, taskID)
	streamKey := q.getKey(keyStreamResultFormat, taskID)

	pipe := q.redisClient.Pipeline()

	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Printf("Task %s: Failed to marshal final result for storage: %v", taskID, err)
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
		log.Printf("Task %s: Queued XADD to stream %s and EXPIRE for %v", taskID, streamKey, q.config.TaskExpiry)
	} else {
		log.Printf("Task %s: resultJSON is nil after attempting to marshal error, not adding to stream.", taskID)
	}

	_, execErr := pipe.Exec(ctx)
	if execErr != nil {
		log.Printf("Task %s: Pipeline failed for status/result/stream update: %v", taskID, execErr)
	} else {
		log.Printf("Task %s: Status updated to %s, result stored, and result streamed to %s.", taskID, status, streamKey)
	}
}

// --- End of Queue Implementation ---
