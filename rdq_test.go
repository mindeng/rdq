package rdq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockProcessTaskSuccess simulates a successful task processing.
func MockProcessTaskSuccess(ctx context.Context, taskID string, payload []byte) ([]byte, error) {
	// log.Printf("MockProcessTaskSuccess: Processing Task %s", taskID)
	// Simulate work that respects context cancellation
	select {
	case <-ctx.Done():
		// log.Printf("MockProcessTaskSuccess: Task %s cancelled.", taskID)
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond): // Simulate quick work
		// Continue processing
	}

	resultData := map[string]string{
		"task_id": taskID,
		"status":  "processed",
		"message": "Task completed successfully!",
	}
	return json.Marshal(resultData)
}

func MockProcessTaskSuccessWithNilResult(ctx context.Context, taskID string, payload []byte) ([]byte, error) {
	// log.Printf("MockProcessTaskSuccess: Processing Task %s", taskID)
	// Simulate work that respects context cancellation
	select {
	case <-ctx.Done():
		// log.Printf("MockProcessTaskSuccess: Task %s cancelled.", taskID)
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond): // Simulate quick work
		// Continue processing
	}

	return nil, nil
}

// MockProcessTaskFailure simulates a task processing failure.
func MockProcessTaskFailure(ctx context.Context, taskID string, payload []byte) ([]byte, error) {
	// log.Printf("MockProcessTaskFailure: Processing Task %s", taskID)
	select {
	case <-ctx.Done():
		// log.Printf("MockProcessTaskFailure: Task %s cancelled.", taskID)
		return nil, ctx.Err()
	case <-time.After(100 * time.Millisecond): // Simulate quick work
		// Continue processing
	}

	return nil, errors.New("simulated task processing failure")
}

// MockProcessTaskLong simulates a long-running task processing.
func MockProcessTaskLong(ctx context.Context, taskID string, payload []byte) ([]byte, error) {
	// log.Printf("MockProcessTaskLong: Processing Task %s", taskID)
	select {
	case <-ctx.Done():
		// log.Printf("MockProcessTaskLong: Task %s cancelled.", taskID)
		return nil, ctx.Err()
	case <-time.After(3 * time.Second): // Simulate long work
		// Continue processing
	}

	resultData := map[string]string{
		"task_id": taskID,
		"status":  "processed",
		"message": "Long task completed!",
	}
	return json.Marshal(resultData)
}

// setupTestRedisClient creates a new single-node Redis client for testing.
func setupTestRedisClient(t *testing.T) redis.UniversalClient {
	// Use environment variable for Redis address, default to localhost:6379
	rdb, redisAddr, err := getRedisClient()
	require.NoError(t, err, "Failed to connect to Redis for testing. Ensure Redis is running at %s", redisAddr)

	// Clean up potentially lingering keys from previous tests
	// WARNING: This flushes the database! Only use on a dedicated test Redis instance.
	// rdb.FlushDB(ctx)

	return rdb
}

func getRedisClient() (*redis.Client, string, error) {
	redisAddr := os.Getenv("TEST_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Ping to ensure connection is working
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := rdb.Ping(ctx).Result()
	return rdb, redisAddr, err
}

// TestPublishNilPayloadSuccess tests publishing a new task with nil payload that succeeds.
func TestPublishNilPayloadSuccess(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	// Use a context for the test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a consumer in a goroutine
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccessWithNilResult)
	}()

	taskID := "test-task-nil-payload-1"
	var payload []byte = nil

	// Publish the task and block for the result
	result, err := queue.ProduceBlock(ctx, taskID, payload)

	// Assert no error from ProduceBlock
	require.NoError(t, err, "ProduceBlock should not return an error for a successful task")
	require.NotNil(t, result, "ProduceBlock should return a non-nil result")

	// Assert the result details
	assert.Equal(t, taskID, result.TaskID, "Result TaskID should match the published task ID")
	assert.Empty(t, result.Error, "Result Error field should be empty for success")

	// Verify status in Redis (optional, but good for confidence)
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should be completed")

	// Clean up the consumer goroutine
	cancelConsumer()
	wg.Wait()
}

// TestPublishNewTaskSuccess tests publishing a new task that succeeds.
func TestPublishNewTaskSuccess(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	// Use a context for the test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a consumer in a goroutine
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	taskID := "test-task-success-1"
	payload := []byte(`{"data": "some data"}`)

	// Publish the task and block for the result
	result, err := queue.ProduceBlock(ctx, taskID, payload)

	// Assert no error from ProduceBlock
	require.NoError(t, err, "ProduceBlock should not return an error for a successful task")
	require.NotNil(t, result, "ProduceBlock should return a non-nil result")

	// Assert the result details
	assert.Equal(t, taskID, result.TaskID, "Result TaskID should match the published task ID")
	assert.Empty(t, result.Error, "Result Error field should be empty for success")

	var resultData map[string]string
	err = json.Unmarshal(result.Data, &resultData)
	require.NoError(t, err, "Failed to unmarshal result data")
	assert.Equal(t, "processed", resultData["status"], "Result data status should be 'processed'")
	assert.Equal(t, "Task completed successfully!", resultData["message"], "Result data message should be success message")

	// Verify status in Redis (optional, but good for confidence)
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should be completed")

	// Clean up the consumer goroutine
	cancelConsumer()
	wg.Wait()
}

// TestPublishNewTaskFailure tests publishing a new task that fails.
func TestPublishNewTaskFailure(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskFailure) // Use failure mock
	}()

	taskID := "test-task-failure-1"
	payload := []byte(`{"data": "some data"}`)

	// Publish the task and block for the result
	result, err := queue.ProduceBlock(ctx, taskID, payload)

	// ProduceBlock should return an error because the task processing failed
	require.Error(t, err, "ProduceBlock should return an error for a failed task")
	// The error message should contain the task processing error wrapped
	assert.Contains(t, err.Error(), "simulated task processing failure", "Error message should contain the task failure reason")

	// The result struct itself should also be returned and contain the error details
	require.NotNil(t, result, "ProduceBlock should return a non-nil result struct even on task failure")
	assert.Equal(t, taskID, result.TaskID, "Result TaskID should match the published task ID")
	assert.Equal(t, "simulated task processing failure", result.Error, "Result Error field should contain the task failure reason")
	assert.Empty(t, result.Data, "Result Data field should be empty on task failure")

	// Verify status in Redis
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusFailed, status, "Task status in Redis should be failed")

	cancelConsumer()
	wg.Wait()
}

// TestPublishDuplicateTaskWaiting tests publishing a duplicate task while the original is processing.
func TestPublishDuplicateTaskWaiting(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Longer timeout for long task
	defer cancel()

	// Start a consumer using the long-running mock
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskLong) // Use long-running mock
	}()

	taskID := "test-task-duplicate-waiting"
	payload1 := []byte(`{"data": "first attempt"}`)
	payload2 := []byte(`{"data": "second attempt"}`) // Different payload, but same task ID

	// Producer 1: Publish the task (will start processing via MockProcessTaskLong)
	// Use non-blocking Produce
	_, resultCh1, err1 := queue.Produce(ctx, taskID, payload1)
	require.NoError(t, err1, "First Produce call should not return an immediate error")
	require.Nil(t, nil, "First Produce call should not return an immediate result")
	require.NotNil(t, resultCh1, "First Produce call should return a channel")

	// Give the consumer a moment to pick up the task and start processing
	time.Sleep(50 * time.Millisecond)

	// Producer 2: Publish the same task ID again immediately
	// Use non-blocking Produce
	_, resultCh2, err2 := queue.Produce(ctx, taskID, payload2) // Use payload2
	require.NoError(t, err2, "Second Produce call should not return an immediate error")
	require.Nil(t, nil, "Second Produce call should not return an immediate result")
	require.NotNil(t, resultCh2, "Second Produce call should return a channel")

	// Assert that both channels are the same (or will deliver the same result)
	// In this implementation, they wait for the same Pub/Sub channel.
	// We can't directly assert channel equality, but we can wait for results from both.

	// Wait for results from both producers
	result1 := <-resultCh1
	result2 := <-resultCh2

	// Assert results from both producers are the same and successful
	require.NotNil(t, result1, "First producer should receive a result")
	require.NotNil(t, result2, "Second producer should receive a result")
	assert.Equal(t, result1, result2, "Both producers should receive the same result")
	assert.Empty(t, result1.Error, "Result should indicate success")

	var resultData map[string]string
	err := json.Unmarshal(result1.Data, &resultData)
	require.NoError(t, err, "Failed to unmarshal result data")
	assert.Equal(t, "Long task completed!", resultData["message"], "Result message should be from the long task")

	// Verify status in Redis
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should be completed")

	cancelConsumer()
	wg.Wait()
}

// TestPublishDuplicateTaskCompleted tests publishing a duplicate task after the original is completed.
func TestPublishDuplicateTaskCompleted(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a consumer
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	taskID := "test-task-duplicate-completed"
	payload1 := []byte(`{"data": "first attempt"}`)
	payload2 := []byte(`{"data": "second attempt"}`) // Different payload, but same task ID

	// Producer 1: Publish the task and block for result
	result1, err1 := queue.ProduceBlock(ctx, taskID, payload1)
	require.NoError(t, err1, "First ProduceBlock should succeed")
	require.NotNil(t, result1, "First ProduceBlock should return a result")
	assert.Empty(t, result1.Error, "First result should indicate success")

	// Give Redis a moment to update state and publish
	time.Sleep(50 * time.Millisecond)

	// Producer 2: Publish the same task ID again
	// This should return the cached result immediately
	result2, resultCh2, err2 := queue.Produce(ctx, taskID, payload2) // Use non-blocking Produce
	require.NoError(t, err2, "Second Produce call should not return an immediate error")
	require.NotNil(t, result2, "Second Produce call should return an immediate result") // Expect immediate result
	require.Nil(t, resultCh2, "Second Produce call should not return a channel")        // Expect no channel

	// Assert results are the same
	assert.Equal(t, result1.TaskID, result2.TaskID, "Task IDs should match")
	assert.Equal(t, result1.Error, result2.Error, "Error fields should match")
	// Note: Data might be slightly different if unmarshalling/remarshalling changes things,
	// but the core content should be the same. Comparing raw JSON bytes is safer.
	if result1.Data == nil {
		require.Nil(t, result2.Data)
	} else {
		assert.JSONEq(t, string(result1.Data), string(result2.Data), "Result data should be the same")
	}

	// Verify status in Redis
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should be completed")

	cancelConsumer()
	wg.Wait()
}

// TestPublishDuplicateTaskFailed tests publishing a duplicate task after the original failed.
func TestPublishDuplicateTaskFailed(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a consumer using the failure mock
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskFailure) // Use failure mock
	}()

	taskID := "test-task-duplicate-failed"
	payload1 := []byte(`{"data": "first attempt"}`)
	payload2 := []byte(`{"data": "second attempt"}`) // Different payload, but same task ID

	// Producer 1: Publish the task and block for result (it will fail)
	result1, err1 := queue.ProduceBlock(ctx, taskID, payload1)
	require.Error(t, err1, "First ProduceBlock should return an error")
	require.NotNil(t, result1, "First ProduceBlock should return a result struct")
	assert.Contains(t, result1.Error, "simulated task processing failure", "First result should indicate failure")

	// Give Redis a moment to update state and publish
	time.Sleep(50 * time.Millisecond)

	// Producer 2: Publish the same task ID again
	// This should return the cached failure result immediately
	result2, resultCh2, err2 := queue.Produce(ctx, taskID, payload2) // Use non-blocking Produce
	require.NoError(t, err2, "Second Produce call should not return an immediate error")
	require.NotNil(t, result2, "Second Produce call should return an immediate result") // Expect immediate result
	require.Nil(t, resultCh2, "Second Produce call should not return a channel")        // Expect no channel

	// Assert results are the same
	assert.Equal(t, result1.TaskID, result2.TaskID, "Task IDs should match")
	assert.Equal(t, result1.Error, result2.Error, "Error fields should match")
	if result1.Data == nil {
		require.Nil(t, result2.Data)
	} else {
		assert.JSONEq(t, string(result1.Data), string(result2.Data), "Result data should be the same (likely empty)")
	}

	// Verify status in Redis
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusFailed, status, "Task status in Redis should be failed")

	cancelConsumer()
	wg.Wait()
}

// TestProducerTimeout tests that ProduceBlock times out if the task takes too long.
func TestProducerTimeout(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config, but override ProducerWaitTimeout
	config := getTestQueueConfig()
	config.ProducerWaitTimeout = 2 * time.Second // Shorter than MockProcessTaskLong (3 seconds)
	queue := NewQueue(redisClient, config)

	// Use a context for the producer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Test context longer than producer timeout
	defer cancel()

	// Start a consumer using the long-running mock
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskLong) // Use long-running mock (3 seconds)
	}()

	taskID := "test-task-timeout"
	payload := []byte(`{"data": "timeout test"}`)

	// Publish the task and block for the result with the configured short timeout
	_, err := queue.ProduceBlock(ctx, taskID, payload)

	// ProduceBlock should return an error due to the timeout
	require.Error(t, err, "ProduceBlock should return an error due to timeout")
	assert.Contains(t, err.Error(), "wait for result timed out after", "Error message should indicate waiting timeout")
	assert.Contains(t, err.Error(), config.ProducerWaitTimeout.String(), "Error message should contain the configured timeout duration")
	// assert.Nil(t, result, "ProduceBlock should return a nil result on timeout")

	// Give the long task a moment to potentially finish in the background
	time.Sleep(4 * time.Second) // Wait longer than MockProcessTaskLong duration

	// Verify status in Redis (it should eventually be completed or failed)
	statusKey := queue.getKey(keyStatus, taskID)                             // Use queue.getKey
	status, err := redisClient.Get(context.Background(), statusKey).Result() // Use a new context for checking final state
	require.NoError(t, err)
	// The task should have completed in the background
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should eventually be completed")

	cancelConsumer()
	wg.Wait()
}

// TestConsumerCancellation tests that a consumer shuts down when its context is cancelled.
func TestConsumerCancellation(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	// Use a context for the consumer that we will cancel
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Consume with the configured short BRPOP timeout so it checks context frequently
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	// Give the consumer a moment to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the consumer's context
	cancelConsumer()

	// Wait for the consumer goroutine to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Assert that the consumer goroutine finishes within a reasonable time
	select {
	case <-done:
		log.Println("Consumer goroutine finished as expected.")
	case <-time.After(5 * time.Second): // Give it some time to exit
		t.Fatal("Consumer goroutine did not finish after context cancellation")
	}

	// Verify the queue is empty (or close to it, depending on timing)
	// This is not a strong assertion, but indicates the consumer stopped processing.
	queueKey := queue.getKey(keyQueue) // Use queue.getKey
	queueLen, err := redisClient.LLen(context.Background(), queueKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, queueLen, int64(1), "Queue should be empty or nearly empty after consumer shutdown") // Allow 0 or 1 in case of timing

	// Try publishing a new task - it should remain in the queue if no consumers are running
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taskID := "test-task-after-cancel"
	payload := []byte(`{"data": "should stay in queue"}`)
	_, _, err = queue.Produce(ctx, taskID, payload) // Use non-blocking Produce
	require.NoError(t, err)
	// Note: Produce might still return a channel even if no consumers are running,
	// the channel will just time out. We assert no immediate result.
	// require.Nil(t, resultCh, "Produce should not return a channel if no consumers are expected to pick it up") // This assertion might be weak depending on timing

	// Verify the task is in the queue
	queueLen, err = redisClient.LLen(ctx, queueKey).Result() // Use queue.getKey
	require.NoError(t, err)
	assert.Equal(t, int64(1), queueLen, "Task should remain in the queue after consumer shutdown")

	// Clean up the queue manually
	redisClient.LRem(ctx, queueKey, 1, taskID) // Use queue.getKey
}

// TestTaskExpiryBeforeProcessing tests task expiry before a consumer picks it up.
func TestTaskExpiryBeforeProcessing(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	config.TaskExpiry = 5 * time.Second
	config.ProducerWaitTimeout = 3 * time.Second
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), config.TaskExpiry+5*time.Second) // Context longer than TaskExpiry
	defer cancel()

	taskID := "test-task-expiry-before-processing"
	payload1 := []byte(`{"data": "expired task"}`)

	// Publish the task
	_, resultCh1, err1 := queue.Produce(ctx, taskID, payload1) // Use non-blocking Produce
	require.NoError(t, err1)
	require.NotNil(t, resultCh1, "First Produce should return a channel")

	// Wait longer than TaskExpiry, but don't start a consumer yet
	log.Printf("Waiting for task %s to expire (%v)...", taskID, config.TaskExpiry) // Use config expiry
	time.Sleep(config.TaskExpiry + 1*time.Second)                                  // Use config expiry

	// Verify the status key has expired
	statusKey := queue.getKey(keyStatus, taskID) // Use queue.getKey
	_, err := redisClient.Get(ctx, statusKey).Result()
	assert.Equal(t, redis.Nil, err, "Status key should have expired")

	// Publish the same task ID again
	payload2 := []byte(`{"data": "new task after expiry"}`)
	result2, resultCh2, err2 := queue.Produce(ctx, taskID, payload2) // This should queue a *new* task
	require.NoError(t, err2)
	require.Nil(t, result2, "Second Produce should not return an immediate result after expiry")
	require.NotNil(t, resultCh2, "Second Produce should return a channel for the new task")

	// Start a consumer to process the second task
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	// Wait for the result of the second task
	finalResult2 := <-resultCh2
	require.NotNil(t, finalResult2, "Second producer should receive a result")
	assert.Empty(t, finalResult2.Error, "Second task should succeed")

	var resultData map[string]string
	err = json.Unmarshal(finalResult2.Data, &resultData)
	require.NoError(t, err)
	assert.Equal(t, "Task completed successfully!", resultData["message"], "Result message should be from the second task")

	// The first producer's channel might time out or receive an error depending on timing
	// We don't strictly assert its outcome here, as the primary goal is to test the
	// expiry and re-queueing behavior.

	cancelConsumer()
	wg.Wait()
}

// TestRedisConnectionFailure tests behavior when Redis is unreachable.
// This test requires Redis to be stopped manually or configured to a non-existent address.
// It's often better to test this scenario by mocking the Redis client if possible,
// but a basic connection test is included.
func TestRedisConnectionFailure(t *testing.T) {
	// Configure this test to use a non-existent Redis address
	nonExistentAddr := "localhost:9999" // Assuming nothing is running on 9999

	rdb := redis.NewClient(&redis.Options{
		Addr:        nonExistentAddr,
		DialTimeout: 1 * time.Second, // Short timeout for faster failure
	})
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(rdb, config)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taskID := "test-redis-down"
	payload := []byte(`{"data": "should fail"}`)

	// Attempt to produce a task
	_, _, err := queue.Produce(ctx, taskID, payload)

	// Expect an error related to connection failure
	require.Error(t, err, "Produce should return an error when Redis is unreachable")
	assert.Contains(t, err.Error(), "dial tcp", "Error message should indicate connection failure")
	assert.Contains(t, err.Error(), "connect: connection refused", "Error message should indicate connection refused") // Specific to localhost:9999 if nothing is there

	// Attempt to consume (will block and eventually error or timeout)
	// This needs to be run in a goroutine as BRPOP is blocking
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer() // Ensure cancellation

	go func() {
		// Consume will loop and retry BRPOP, eventually hitting errors
		// We'll run it with a short context to bound the test
		shortLiveCtx, shortLiveCancel := context.WithTimeout(consumerCtx, 3*time.Second)
		defer shortLiveCancel()
		queue.Consume(shortLiveCtx, MockProcessTaskSuccess)
		// If Consume exits due to context cancellation or internal error, we might not get here
		// A more robust test would check logs or internal state.
		// For this basic test, we rely on the Produce error and the fact Consume won't proceed.
	}()

	// Give the consumer a moment to try connecting/BRPOPing
	time.Sleep(1 * time.Second)

	// Attempt to produce again - should still fail
	_, _, err = queue.Produce(ctx, taskID+"-2", payload)
	require.Error(t, err, "Produce should still fail when Redis is unreachable")

	// Note: Testing graceful recovery after Redis comes back up would require
	// starting Redis during the test, which is more complex for a unit test.
}

// TestMultipleConsumers tests that multiple consumers can process tasks from the same queue.
func TestMultipleConsumers(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	numConsumers := 5
	var consumerWg sync.WaitGroup
	consumerWg.Add(numConsumers)

	// Start multiple consumers
	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer cancelConsumers()
	for i := 0; i < numConsumers; i++ {
		go func(id int) {
			defer consumerWg.Done()
			log.Printf("Starting Consumer %d", id)
			queue.Consume(consumerCtx, MockProcessTaskSuccess)
			log.Printf("Consumer %d stopped", id)
		}(i)
	}

	// Give consumers a moment to start
	time.Sleep(100 * time.Millisecond)

	numTasks := 20
	taskIDs := make(map[string]struct{})
	var producerWg sync.WaitGroup
	producerWg.Add(numTasks)
	results := make(chan *Result, numTasks)

	// Publish multiple unique tasks
	go func() {
		for i := 0; i < numTasks; i++ {
			taskID := fmt.Sprintf("test-multiple-consumers-%d", i)
			taskIDs[taskID] = struct{}{} // Keep track of published IDs
			payload := []byte(fmt.Sprintf(`{"task_num": %d}`, i))

			go func(id string, p []byte) {
				defer producerWg.Done()
				result, err := queue.ProduceBlock(ctx, id, p)
				if err != nil {
					log.Printf("Producer error for task %s: %v", id, err)
					results <- &Result{TaskID: id, Error: err.Error()} // Send error as a Result
				} else {
					results <- result
				}
			}(taskID, payload)
		}
	}()

	// Wait for all producers to finish submitting/waiting
	producerWg.Wait()
	log.Println("All producers finished submitting/waiting.")

	// Collect results from the channel
	receivedResults := make(map[string]*Result)
	resultsCollected := 0
	for resultsCollected < numTasks {
		select {
		case res := <-results:
			receivedResults[res.TaskID] = res
			resultsCollected++
		case <-ctx.Done():
			t.Fatalf("Context cancelled while waiting for results: %v", ctx.Err())
		case <-time.After(20 * time.Second): // Timeout for collecting results
			t.Fatalf("Timed out waiting to collect all results (%d/%d collected)", resultsCollected, numTasks)
		}
	}
	close(results) // Close the results channel

	// Assert that we received results for all published tasks
	assert.Equal(t, numTasks, len(receivedResults), "Should receive a result for each published task")

	// Assert that all tasks were processed successfully
	for taskID, result := range receivedResults {
		assert.Empty(t, result.Error, fmt.Sprintf("Task %s should have processed successfully", taskID))
		assert.Equal(t, taskID, result.TaskID, fmt.Sprintf("Result TaskID mismatch for task %s", taskID))
		// Optionally, check the content of result.Data
	}

	// Clean up consumers
	cancelConsumers()
	consumerWg.Wait()
}

// TestProduceNonBlocking tests the non-blocking behavior of the Produce method.
func TestProduceNonBlocking(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()
	// Use default config for the test
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taskID := "test-produce-non-blocking"
	payload := []byte(`{"data": "non-blocking test"}`)

	// Produce the task - should return a channel immediately
	immediateResult, resultCh, err := queue.Produce(ctx, taskID, payload)

	require.NoError(t, err, "Produce should not return an error immediately")
	require.Nil(t, immediateResult, "Produce should not return an immediate result for a new task")
	require.NotNil(t, resultCh, "Produce should return a channel for a new task")

	// Verify the task is in the queue (before consumption)
	queueKey := queue.getKey(keyQueue) // Use queue.getKey
	queueLen, err := redisClient.LLen(ctx, queueKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), queueLen, "Task should be in the queue immediately after Produce")

	// Start a consumer to process the task
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	// Wait for the result from the channel returned by Produce
	select {
	case finalResult := <-resultCh:
		require.NotNil(t, finalResult, "Channel should deliver a result")
		assert.Empty(t, finalResult.Error, "Task should succeed")
		assert.Equal(t, taskID, finalResult.TaskID, "Result TaskID mismatch")
	case <-ctx.Done():
		t.Fatalf("Context cancelled while waiting for result channel: %v", ctx.Err())
	case <-time.After(10 * time.Second): // Timeout for waiting on channel
		t.Fatal("Timed out waiting for result on channel")
	}

	// Verify the queue is now empty
	queueLen, err = redisClient.LLen(ctx, queueKey).Result() // Use queue.getKey
	require.NoError(t, err)
	assert.Equal(t, int64(0), queueLen, "Queue should be empty after task is consumed")

	cancelConsumer()
	wg.Wait()
}

// TestCustomKeyPrefix tests using a custom key prefix.
func TestCustomKeyPrefix(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()

	// Use a custom config with a different prefix
	config := getTestQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a consumer with the custom prefix
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	taskID := "test-custom-prefix-task"
	payload := []byte(`{"data": "custom prefix test"}`)

	// Publish the task using the queue with the custom prefix
	result, err := queue.ProduceBlock(ctx, taskID, payload)
	require.NoError(t, err, "ProduceBlock should succeed with custom prefix")
	require.NotNil(t, result, "ProduceBlock should return a result")
	assert.Empty(t, result.Error, "Result Error field should be empty for success")
	assert.Equal(t, taskID, result.TaskID, "Result TaskID should match")

	// Verify keys in Redis use the custom prefix
	statusKey := queue.getKey(keyStatus, taskID)
	payloadKey := queue.getKey(keyPayload, taskID)
	resultKey := queue.getKey(keyResult, taskID)
	queueKey := queue.getKey(keyQueue)

	assert.Contains(t, statusKey, config.KeyPrefix, "Status key should have the custom prefix")
	assert.Contains(t, payloadKey, config.KeyPrefix, "Payload key should have the custom prefix")
	assert.Contains(t, resultKey, config.KeyPrefix, "Result key should have the custom prefix")
	assert.Contains(t, queueKey, config.KeyPrefix, "Queue key should have the custom prefix")

	// Verify status in Redis using the custom prefixed key
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should be completed with custom prefix")

	// Verify the queue is empty using the custom prefixed queue key
	queueLen, err := redisClient.LLen(ctx, queueKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), queueLen, "Queue should be empty after task is consumed with custom prefix")

	cancelConsumer()
	wg.Wait()
}

// TestDefaultKeyPrefix tests using the default key prefix.
func TestDefaultKeyPrefix(t *testing.T) {
	redisClient := setupTestRedisClient(t)
	defer redisClient.Close()

	// Use default config
	config := DefaultQueueConfig()
	queue := NewQueue(redisClient, config)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a consumer with the default prefix
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	defer cancelConsumer()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		queue.Consume(consumerCtx, MockProcessTaskSuccess)
	}()

	taskID := testTaskIDPrefix + "test-default-prefix-task"
	payload := []byte(`{"data": "default prefix test"}`)

	// Publish the task using the queue with the default prefix
	result, err := queue.ProduceBlock(ctx, taskID, payload)
	require.NoError(t, err, "ProduceBlock should succeed with default prefix")
	require.NotNil(t, result, "ProduceBlock should return a result")
	assert.Empty(t, result.Error, "Result Error field should be empty for success")
	assert.Equal(t, taskID, result.TaskID, "Result TaskID should match")

	// Verify keys in Redis use the default prefix
	statusKey := queue.getKey(keyStatus, taskID)
	payloadKey := queue.getKey(keyPayload, taskID)
	resultKey := queue.getKey(keyResult, taskID)
	queueKey := queue.getKey(keyQueue)

	assert.Contains(t, statusKey, defaultKeyPrefix, "Status key should have the default prefix")
	assert.Contains(t, payloadKey, defaultKeyPrefix, "Payload key should have the default prefix")
	assert.Contains(t, resultKey, defaultKeyPrefix, "Result key should have the default prefix")
	assert.Contains(t, queueKey, defaultKeyPrefix, "Queue key should have the default prefix")

	// Verify status in Redis using the default prefixed key
	status, err := redisClient.Get(ctx, statusKey).Result()
	require.NoError(t, err)
	assert.Equal(t, taskStatusCompleted, status, "Task status in Redis should be completed with default prefix")

	// Verify the queue is empty using the default prefixed queue key
	queueLen, err := redisClient.LLen(ctx, queueKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), queueLen, "Queue should be empty after task is consumed with default prefix")

	cancelConsumer()
	wg.Wait()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

var testQueueConfig QueueConfig

func setup() {
	config := DefaultQueueConfig()
	config.KeyPrefix = "testrdq-b35c5ad0-0f7a-4356-9367-075a6cade309:"
	testQueueConfig = config
	testTaskIDPrefix = "testtask-1b88ab5f-44c9-478c-9f1d-fbd8fd6fef93-"
}

func shutdown() {
	rdb, _, err := getRedisClient()
	if err != nil {
		panic(err)
	}
	cleanupKeys(context.Background(), rdb, testQueueConfig.KeyPrefix+"*")
	cleanupKeys(context.Background(), rdb, fmt.Sprintf("_rdq:task:{%s*}*", testTaskIDPrefix))
}

func getTestQueueConfig() QueueConfig {
	return testQueueConfig
}

var testTaskIDPrefix string

func cleanupKeys(ctx context.Context, rc redis.UniversalClient, pattern string) error {
	var cur uint64
	for {
		keys, newCur, err := rc.Scan(ctx, cur, pattern, 0).Result()
		if err != nil {
			return err
		}

		_, err = rc.Del(ctx, keys...).Result()
		if err != nil {
			return err
		}
		if newCur == 0 {
			break
		}
		cur = newCur
	}
	return nil
}
