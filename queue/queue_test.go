package queue

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestQueueStores(t *testing.T) {
	// Setup SQLite path
	tmpDir, err := os.MkdirTemp("", "queue-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	sqlitePath := filepath.Join(tmpDir, "test.db")

	memoryStore := NewMemoryStore()
	sqliteStore, err := NewSQLiteStore(sqlitePath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer sqliteStore.Close()

	stores := map[string]Store{
		"MemoryStore": memoryStore,
		"SQLiteStore": sqliteStore,
	}

	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// Test Enqueue and Dequeue
			job1 := &Job{
				ID:         "job-1",
				Type:       "email",
				Payload:    []byte("hello"),
				MaxRetries: 3,
			}
			err := store.Enqueue(ctx, job1)
			if err != nil {
				t.Fatalf("Enqueue failed: %v", err)
			}

			// Dequeue
			dqJob, err := store.Dequeue(ctx, []string{"default"}, []string{"email"}, 10*time.Second)
			if err != nil {
				t.Fatalf("Dequeue failed: %v", err)
			}
			if dqJob == nil {
				t.Fatalf("expected job to be returned, got nil")
			}
			if dqJob.ID != "job-1" {
				t.Errorf("expected job-1, got %s", dqJob.ID)
			}
			if dqJob.State != StateProcessing {
				t.Errorf("expected state to be processing, got %s", dqJob.State)
			}

			// Ack
			err = store.Ack(ctx, dqJob.ID)
			if err != nil {
				t.Fatalf("Ack failed: %v", err)
			}

			// Check stats
			stats, err := store.GetStats(ctx)
			if err != nil {
				t.Fatalf("GetStats failed: %v", err)
			}
			if stats.Completed != 1 {
				t.Errorf("expected completed=1, got %d", stats.Completed)
			}

			// Test Retries & DLQ
			job2 := &Job{
				ID:         "job-2",
				Type:       "sms",
				Payload:    []byte("world"),
				MaxRetries: 2,
			}
			if err := store.Enqueue(ctx, job2); err != nil {
				t.Fatalf("Enqueue failed: %v", err)
			}

			// 1st processing
			dqJob2, err := store.Dequeue(ctx, []string{"default"}, []string{"sms"}, 500*time.Millisecond)
			if err != nil || dqJob2 == nil {
				t.Fatalf("Dequeue job-2 failed: %v", err)
			}

			// Nack 1
			err = store.Nack(ctx, dqJob2.ID, 10*time.Millisecond, errors.New("err 1"))
			if err != nil {
				t.Fatalf("Nack failed: %v", err)
			}

			// Sleep for run_at
			time.Sleep(20 * time.Millisecond)

			// 2nd processing
			dqJob2, err = store.Dequeue(ctx, []string{"default"}, []string{"sms"}, 500*time.Millisecond)
			if err != nil || dqJob2 == nil {
				t.Fatalf("Dequeue job-2 second time failed: %v", err)
			}
			if dqJob2.Retries != 1 {
				t.Errorf("expected retries=1, got %d", dqJob2.Retries)
			}

			// Nack 2 -> DLQ
			err = store.Nack(ctx, dqJob2.ID, 10*time.Millisecond, errors.New("err 2"))
			if err != nil {
				t.Fatalf("Nack failed: %v", err)
			}

			// Must be in Dead Letter State
			stats, err = store.GetStats(ctx)
			if err != nil {
				t.Fatalf("GetStats failed: %v", err)
			}
			if stats.DeadLetter != 1 {
				t.Errorf("expected dead_letter=1, got %d", stats.DeadLetter)
			}

			// List DLQ
			dlqJobs, err := store.ListJobs(ctx, StateDeadLetter, 10, 0)
			if err != nil {
				t.Fatalf("ListJobs failed: %v", err)
			}
			if len(dlqJobs) != 1 || dlqJobs[0].ID != "job-2" {
				t.Errorf("expected job-2 in DLQ, got %v", dlqJobs)
			}

			// Redrive DLQ
			redriven, err := store.RedriveDeadLetter(ctx, []string{"job-2"})
			if err != nil {
				t.Fatalf("Redrive failed: %v", err)
			}
			if redriven != 1 {
				t.Errorf("expected redriven=1, got %d", redriven)
			}

			// Verify it's pending again
			stats, err = store.GetStats(ctx)
			if err != nil {
				t.Fatalf("GetStats failed: %v", err)
			}
			if stats.Pending != 1 || stats.DeadLetter != 0 {
				t.Errorf("expected pending=1 dead_letter=0 after redrive, got pending=%d dlq=%d", stats.Pending, stats.DeadLetter)
			}

			// Test Sweeper lease expiry
			job3 := &Job{
				ID:         "job-3",
				Type:       "render",
				Payload:    []byte("image"),
				MaxRetries: 3,
			}
			_ = store.Enqueue(ctx, job3)
			dqJob3, _ := store.Dequeue(ctx, []string{"default"}, []string{"render"}, 10*time.Millisecond) // short lease
			if dqJob3 == nil {
				t.Fatalf("dqJob3 is nil")
			}

			// Wait for lease to expire
			time.Sleep(30 * time.Millisecond)

			// Run Sweeper
			released, err := store.SweeperReleaseExpired(ctx)
			if err != nil {
				t.Fatalf("Sweeper failed: %v", err)
			}
			if released != 1 {
				t.Errorf("expected 1 job released by sweeper, got %d", released)
			}

			// It should be retried and in Failed (or Pending) state
			stats, err = store.GetStats(ctx)
			if err != nil {
				t.Fatalf("GetStats failed: %v", err)
			}
			if stats.Failed == 0 && stats.Pending == 0 {
				t.Errorf("expected job-3 to be failed or pending after sweep, got %+v", stats)
			}

			// Test Named Queue Isolation
			job4 := &Job{
				ID:         "job-4",
				Queue:      "critical",
				Type:       "alert",
				Payload:    []byte("critical-alert"),
				MaxRetries: 3,
			}
			_ = store.Enqueue(ctx, job4)

			// Try dequeuing from 'default' queue - should be nil
			dqDefault, err := store.Dequeue(ctx, []string{"default"}, []string{"alert"}, 10*time.Second)
			if err != nil {
				t.Fatalf("Default dequeue failed: %v", err)
			}
			if dqDefault != nil {
				t.Errorf("expected no job from default queue, got %s", dqDefault.ID)
			}

			// Try dequeuing from 'critical' queue - should find job-4
			dqCritical, err := store.Dequeue(ctx, []string{"critical"}, []string{"alert"}, 10*time.Second)
			if err != nil {
				t.Fatalf("Critical dequeue failed: %v", err)
			}
			if dqCritical == nil || dqCritical.ID != "job-4" {
				t.Errorf("expected job-4 from critical queue, got %v", dqCritical)
			}

			// Clean up critical job
			_ = store.Ack(ctx, "job-4")
		})
	}
}
