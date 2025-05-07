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

	keyStatus  = "task:{%s}:status"
	keyPayload = "task:{%s}:payload"
	keyResult  = "task:{%s}:result"
	keyChannel = "channel:{%s}:finished"

	keyQueue = "queue:tasks"

	// Default prefix for internal Redis keys
	defaultKeyPrefix = "_rdq:"
)

// QueueConfig holds configuration options for the Queue.
type QueueConfig struct {
	// TaskExpiry is the time after which task status, payload, and result keys expire.
	// Should be long enough to cover max processing time + waiting time.
	TaskExpiry time.Duration

	// ProducerWaitTimeout is the maximum time a producer will wait for a task result via Pub/Sub.
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
		ConsumerBRPOPTimeout: 1 * time.Second, // Use a shorter timeout for faster test cycles
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
// The caller is responsible for creating and managing the redisClient.
func NewQueue(client redis.UniversalClient, config QueueConfig) *Queue {
	// Ensure the key prefix ends with a colon for consistent formatting
	if config.KeyPrefix != "" && config.KeyPrefix[len(config.KeyPrefix)-1] != ':' {
		config.KeyPrefix += ":"
	} else if config.KeyPrefix == "" {
		config.KeyPrefix = defaultKeyPrefix // Ensure default if empty string is passed
	}

	return &Queue{
		redisClient: client,
		config:      config,
	}
}

// getKey formats an internal Redis key with the configured prefix and hash tag (if applicable).
func (q *Queue) getKey(format string, args ...interface{}) string {
	// Prepend the configured prefix
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

	// Use getKey for all internal keys
	statusKey := q.getKey(keyStatus, taskID)
	payloadKey := q.getKey(keyPayload, taskID)
	resultKey := q.getKey(keyResult, taskID)
	channelName := q.getKey(keyChannel, taskID)
	queueKey := q.getKey(keyQueue) // Queue key does NOT have a taskID hash tag

	// --- Step 1: Atomically check status and try to set to pending ---
	// Use SETNX with EX to ensure atomicity for the initial state transition.
	// This operation is atomic on the single statusKey (which has a hash tag).
	setOk, err := q.redisClient.SetNX(ctx, statusKey, taskStatusPending, q.config.TaskExpiry).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("redis SETNX error for task %s: %w", taskID, err)
	}

	var action string
	if setOk {
		// Successfully set status to pending. This task is new or expired.
		action = "queued"
		log.Printf("Task %s: Status set to pending. Storing payload and queueing.", taskID)

		// --- Step 2: Store payload and push to queue ---
		// These operations are not atomic with Step 1 in Redis Cluster,
		// but the atomic status setting prevents true duplicate processing.
		// If a crash happens here, the task might be in 'pending' but not in the queue
		// or payload missing. A separate cleanup/monitor might be needed for robustness.
		pipe := q.redisClient.Pipeline()
		pipe.Set(ctx, payloadKey, payloadJSON, q.config.TaskExpiry)
		pipe.LPush(ctx, queueKey, taskID) // Queue key does NOT have a hash tag
		_, execErr := pipe.Exec(ctx)
		if execErr != nil {
			// This is a critical failure. The task is marked pending but might not be queue or payload.
			// Consider marking as failed or requiring external cleanup.
			log.Printf("Task %s: CRITICAL - Failed to store payload or queue after setting pending: %v", taskID, execErr)
			// Attempt to revert status or mark failed (best effort)
			q.redisClient.Set(ctx, statusKey, taskStatusFailed, q.config.TaskExpiry) // Best effort
			// Return error immediately as queueing failed
			return nil, nil, fmt.Errorf("failed to store payload or queue task %s: %w", taskID, execErr)
		}
		log.Printf("Task %s: Payload stored and queued.", taskID)

	} else {
		// SETNX returned false, key already exists. Check current status.
		status, getErr := q.redisClient.Get(ctx, statusKey).Result()
		if getErr != nil && getErr != redis.Nil {
			return nil, nil, fmt.Errorf("failed to get status for existing task %s: %w", taskID, getErr)
		}

		switch status {
		case taskStatusCompleted:
			action = "completed"
			log.Printf("Task %s: Already completed, fetching result.", taskID)
			// Fetch result directly
			resultData, getErr := q.redisClient.Get(ctx, resultKey).Bytes()
			if getErr == redis.Nil {
				// Should not happen if status is completed, but handle defensively
				return nil, nil, fmt.Errorf("task %s status is completed, but result not found (likely expired)", taskID)
			} else if getErr != nil {
				return nil, nil, fmt.Errorf("failed to get result for task %s: %w", taskID, getErr)
			}

			var result Result
			if umErr := json.Unmarshal(resultData, &result); umErr != nil {
				log.Printf("Warning: Failed to unmarshal result for task %s, returning raw data. Error: %v", taskID, umErr)
				result.TaskID = taskID
				result.Data = resultData
				// If unmarshalling fails, package the error in the Result struct's Error field
				result.Error = fmt.Sprintf("failed to unmarshal result: %v", umErr)
			}
			// Return immediate result
			return &result, nil, nil

		case taskStatusFailed:
			action = "failed"
			log.Printf("Task %s: Previously failed, fetching result.", taskID)
			// Fetch result of the failed task
			resultData, getErr := q.redisClient.Get(ctx, resultKey).Bytes()
			if getErr == redis.Nil {
				// Failed status but no result, maybe expired?
				return nil, nil, fmt.Errorf("task %s status is failed, but result not found (likely expired)", taskID)
			} else if getErr != nil {
				return nil, nil, fmt.Errorf("failed to get result for failed task %s: %w", taskID, getErr)
			}

			var result Result
			if umErr := json.Unmarshal(resultData, &result); umErr != nil {
				log.Printf("Warning: Failed to unmarshal result for failed task %s, returning raw data. Error: %v", taskID, umErr)
				result.TaskID = taskID
				result.Data = resultData
				// If unmarshalling fails, package the error in the Result struct's Error field
				result.Error = fmt.Sprintf("failed to unmarshal result: %v", umErr)
			}
			// Return immediate result (which indicates failure via Result.Error)
			return &result, nil, nil

		case taskStatusPending, taskStatusProcessing:
			action = "waiting"
			log.Printf("Task %s: Already pending or processing, waiting for result.", taskID)
			// Fall through to waiting logic

		case "": // Key did not exist (shouldn't happen if SETNX returned false, but defensive)
			log.Printf("Task %s: Status key did not exist after SETNX returned false. Race condition?", taskID)
			// This case is unlikely if SETNX returned false, but if it happens,
			// it suggests a very quick expiry or race. Treat as waiting.
			action = "waiting"

		default: // Unexpected status
			log.Printf("Task %s: Unexpected status '%s'. Treating as waiting.", taskID, status)
			action = "waiting"
		}
	}

	// If action is 'queued' or 'waiting', create a channel and start a goroutine to wait.
	if action == "queued" || action == "waiting" {
		resultCh := make(chan *Result, 1) // Buffered channel

		go func() {
			// Defer closing the channel to ensure it's closed when the goroutine exits.
			defer close(resultCh)

			log.Printf("Task %s: Waiting for result via Pub/Sub on channel %s...", taskID, channelName)
			// Create a new context for the Pub/Sub subscription with a timeout
			waitCtx, cancelWait := context.WithTimeout(ctx, q.config.ProducerWaitTimeout)
			defer cancelWait()

			pubsub := q.redisClient.Subscribe(waitCtx, channelName)
			defer pubsub.Close()

			// Wait for confirmation that subscription is ready
			_, err := pubsub.Receive(waitCtx)
			if err != nil {
				// Check if the error is due to context cancellation (timeout)
				if waitCtx.Err() == context.DeadlineExceeded {
					log.Printf("Task %s: Wait for result timed out after %v", taskID, q.config.ProducerWaitTimeout)
					// Send a Result with error to the channel
					resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("wait for result timed out after %v", q.config.ProducerWaitTimeout)}
					return
				}
				// Send a Result with the subscription error to the channel
				resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("failed to subscribe or receive confirmation: %v", err)}
				return
			}

			msgCh := pubsub.Channel()

			select {
			case msg := <-msgCh:
				log.Printf("Task %s: Received Pub/Sub message: %s", taskID, msg.Payload)
				// Message received, fetch result
				resultData, getErr := q.redisClient.Get(ctx, resultKey).Bytes()
				if getErr == redis.Nil {
					// This is unexpected if we got a finish message, could indicate a timing issue or expiry
					resultCh <- &Result{TaskID: taskID, Error: "task finished signal received, but result not found (likely expired)"}
				} else if getErr != nil {
					resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("failed to get result after finish signal: %v", getErr)}
				} else {
					var result Result
					if umErr := json.Unmarshal(resultData, &result); umErr != nil {
						log.Printf("Warning: Failed to unmarshal result for task %s after signal, returning raw data. Error: %v", taskID, umErr)
						result.TaskID = taskID
						result.Data = resultData
						// Package unmarshalling error in Result struct
						result.Error = fmt.Sprintf("failed to unmarshal result after signal: %v", umErr)
					}
					// Send the fetched result to the channel
					resultCh <- &result
				}

			case <-waitCtx.Done(): // Use the waitCtx's done channel for timeout
				// Timeout occurred while waiting (handled by waitCtx above)
				log.Printf("Task %s: Wait timeout after %v (via context done)", taskID, q.config.ProducerWaitTimeout)
				// Send a Result with timeout error to the channel
				resultCh <- &Result{TaskID: taskID, Error: fmt.Sprintf("wait for result timed out after %v", q.config.ProducerWaitTimeout)}
			}
		}()

		// Return nil result, the channel, and nil error
		return nil, resultCh, nil
	}

	// Should not reach here unless there's an unexpected action
	return nil, nil, fmt.Errorf("unexpected state after processing task %s", taskID)
}

// ProduceBlock calls Produce and blocks until the result is available or an error occurs.
// Returns (*Result, error). Note that if the task processing failed, the Result will contain the error.
// An error returned by ProduceBlock itself indicates a failure in the queueing or waiting mechanism.
func (q *Queue) ProduceBlock(ctx context.Context, taskID string, payload []byte) (*Result, error) {
	immediateResult, resultCh, err := q.Produce(ctx, taskID, payload)
	// Handle immediate errors from Produce
	if err != nil {
		return nil, fmt.Errorf("produce failed: %w", err)
	}

	// If an immediate result was returned, return it
	if immediateResult != nil {
		// Check if the immediate result itself indicates a task processing failure
		if immediateResult.Error != "" {
			// Return the Result struct which contains the error details
			return immediateResult, errors.New(immediateResult.Error) // Wrap the task error
		}
		return immediateResult, nil
	}

	// If a channel was returned, wait on the channel
	if resultCh != nil {
		select {
		case finalResult := <-resultCh:
			// The channel delivered the final Result
			if finalResult == nil {
				// Should not happen with buffered channel, but defensive
				return nil, errors.New("result channel closed unexpectedly or delivered nil")
			}
			// Check if the final result itself indicates a task processing failure
			if finalResult.Error != "" {
				// Return the Result struct which contains the error details
				return finalResult, errors.New(finalResult.Error) // Wrap the task error
			}
			return finalResult, nil

		case <-ctx.Done():
			// Context was cancelled while waiting on the channel
			return nil, fmt.Errorf("produce block cancelled by context: %w", ctx.Err())
		}
	}

	// Should not reach here
	return nil, errors.New("produce returned neither immediate result nor channel")
}

// ProcessTaskFunc is a function type that handles task processing.
// It takes context, task ID and payload, returns result data and error.
type ProcessTaskFunc func(ctx context.Context, taskID string, payload []byte) ([]byte, error)

// Consume processes tasks from the queue. Should be run in a goroutine.
func (q *Queue) Consume(ctx context.Context, processFunc ProcessTaskFunc) {
	queueKey := q.getKey(keyQueue) // Use queueKey

	for {
		// Check if the context has been cancelled before attempting to pop a task.
		select {
		case <-ctx.Done():
			log.Println("Consumer context cancelled, shutting down.")
			return // Exit the goroutine if context is done
		default:
			// Context is not done, continue to BRPOP
		}

		// BRPOP blocks until an element is available or timeout.
		// Using a non-zero timeout allows us to periodically check ctx.Done().
		taskIDs, err := q.redisClient.BRPop(ctx, q.config.ConsumerBRPOPTimeout, queueKey).Result()
		if err != nil {
			// Check if the error is due to the BRPOP timeout
			if err == redis.Nil {
				// No tasks in the queue within the timeout, continue the loop to check context again.
				continue
			}
			// Check if the error is due to context cancellation
			if err == context.Canceled {
				log.Println("Consumer context cancelled during BRPOP, shutting down.")
				return // Exit the goroutine if context is cancelled
			}
			// Handle other potential Redis errors
			log.Printf("BRPOP error: %v. Retrying in 5 seconds.", err)
			time.Sleep(5 * time.Second) // Sleep before retrying BRPOP on other errors
			continue
		}

		// If BRPOP was successful, taskIDs will contain [queue_name, task_id]
		if len(taskIDs) != 2 {
			log.Printf("Unexpected BRPOP result: %v", taskIDs)
			continue // Should get [queue_name, task_id]
		}

		taskID := taskIDs[1]
		log.Printf("Consumer picked up task: %s", taskID)

		statusKey := q.getKey(keyStatus, taskID)
		payloadKey := q.getKey(keyPayload, taskID)

		// Atomically update status from pending to processing using the script.
		// This script operates only on statusKey (with hash tag), so it's fine for Cluster.
		// Pass the passed-in context.
		consumeAction, err := consumeScript.Run(
			ctx,                                                                                                                       // Pass the context here
			q.redisClient,                                                                                                             // Pass the client here
			[]string{statusKey},                                                                                                       // KEYS
			taskID, taskStatusPending, taskStatusProcessing, q.config.TaskExpiry.Seconds(), taskStatusProcessing, taskStatusCompleted, // ARGV
		).Result()
		if err != nil {
			log.Printf("Task %s: Consume script error: %v. Skipping task.", taskID, err)
			continue // Skip this task if script fails
		}

		action := consumeAction.(string)
		log.Printf("Task %s: Consume script returned action: %s", taskID, action)

		if action != "processing" {
			log.Printf("Task %s: Not processing due to status '%s'. Skipping or potential retry needed.", taskID, action)
			// Depending on strategy, might re-queue if action is 'invalid_status' and not expired.
			// For simplicity here, we skip and rely on expiry for cleanup or a separate monitor.
			continue
		}

		// Get task payload (uses Hash Tagged key)
		payloadData, err := q.redisClient.Get(ctx, payloadKey).Bytes()
		if err != nil {
			if err == redis.Nil {
				log.Printf("Task %s: Payload not found. Likely expired before processing. Marking failed.", taskID)
				// Pass the context to updateStatusAndPublish
				q.updateStatusAndPublish(ctx, taskID, taskStatusFailed, &Result{TaskID: taskID, Error: "payload expired before processing"})
			} else {
				log.Printf("Task %s: Failed to get payload: %v. Marking failed.", taskID, err)
				// Pass the context to updateStatusAndPublish
				q.updateStatusAndPublish(ctx, taskID, taskStatusFailed, &Result{TaskID: taskID, Error: fmt.Sprintf("failed to get payload: %v", err.Error())})
			}
			continue // Move to the next task
		}

		// Process the task
		log.Printf("Task %s: Executing processing logic...", taskID)
		// Pass the context to processFunc
		resultData, processErr := processFunc(ctx, taskID, payloadData)

		// Handle result and update status
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

		// Store result and update status (uses Hash Tagged keys and pipeline)
		// Pass the context to updateStatusAndPublish
		q.updateStatusAndPublish(ctx, taskID, finalStatus, taskResult)
	}
}

// updateStatusAndPublish updates the task status, stores the result, and publishes a completion message.
// Uses Hash Tagged keys. The pipeline should work in Cluster as statusKey and resultKey
// for the same taskID are in the same hash slot due to the Hash Tag.
func (q *Queue) updateStatusAndPublish(ctx context.Context, taskID string, status string, result *Result) {
	resultKey := q.getKey(keyResult, taskID)
	statusKey := q.getKey(keyStatus, taskID)
	channelName := q.getKey(keyChannel, taskID)

	// Use Pipelining for atomicity of result storage and status update.
	// This pipeline involves keys with the same hash tag ({taskID}),
	// so it will be sent to the same node in the cluster.
	pipe := q.redisClient.Pipeline()

	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Printf("Task %s: Failed to marshal final result for storage: %v", taskID, err)
		// Try to store a failure result if marshalling failed
		failResult := &Result{TaskID: taskID, Error: fmt.Sprintf("failed to marshal final result: %v", err)}
		if failJSON, mErr := json.Marshal(failResult); mErr == nil {
			pipe.Set(ctx, resultKey, failJSON, q.config.TaskExpiry)
		} else {
			log.Printf("Task %s: Also failed to marshal fail result: %v", taskID, mErr)
		}
	} else {
		pipe.Set(ctx, resultKey, resultJSON, q.config.TaskExpiry)
	}

	pipe.Set(ctx, statusKey, status, q.config.TaskExpiry) // Update status to completed/failed
	_, execErr := pipe.Exec(ctx)                          // Execute SET and SET
	if execErr != nil {
		log.Printf("Task %s: Failed to update status or store result: %v", taskID, execErr)
		// This is a critical failure. The task might be stuck in 'processing' or 'failed'
		// without results. A separate monitoring process might be needed.
		// We still attempt to publish the signal below, but the result might be missing.
	} else {
		log.Printf("Task %s: Status updated to %s, result stored.", taskID, status)
	}

	// Publish completion signal. Pub/Sub is a broadcast and doesn't rely on hash slots.
	// We publish even if the pipeline failed, as the status might have been updated
	// before the failure, and clients might still be waiting.
	publishErr := q.redisClient.Publish(ctx, channelName, "done").Err()
	if publishErr != nil {
		log.Printf("Task %s: Failed to publish completion signal: %v", taskID, publishErr)
		// Clients waiting for this task might time out.
	} else {
		log.Printf("Task %s: Published completion signal.", taskID)
	}
}

// --- End of Queue Implementation ---
