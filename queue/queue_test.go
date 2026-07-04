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

			// 1. Test Enqueue and Dequeue
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

			// 2. Test Retries & DLQ
			job2 := &Job{
				ID:         "job-2",
				Type:       "sms",
				Payload:    []byte("world"),
				MaxRetries: 1,
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

			// Dequeue and Ack the redriven job to keep store clean
			cleanDq, _ := store.Dequeue(ctx, []string{"default"}, []string{"sms"}, 5*time.Second)
			if cleanDq != nil {
				_ = store.Ack(ctx, cleanDq.ID)
			}

			// 3. Test Sweeper lease expiry
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

			// Clean up sweeper job
			cleanDq3, _ := store.Dequeue(ctx, []string{"default"}, []string{"render"}, 5*time.Second)
			if cleanDq3 != nil {
				_ = store.Ack(ctx, cleanDq3.ID)
			}

			// 4. Test Named Queue Isolation
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

			// 5. Test Priority Queuing
			pLow := &Job{
				ID:       "p-low",
				Priority: 1,
				Type:     "p-test",
				Payload:  []byte("low"),
			}
			pHigh := &Job{
				ID:       "p-high",
				Priority: 10,
				Type:     "p-test",
				Payload:  []byte("high"),
			}
			_ = store.Enqueue(ctx, pLow)
			_ = store.Enqueue(ctx, pHigh)

			// First dequeue MUST return p-high due to higher priority
			dqP1, err := store.Dequeue(ctx, []string{"default"}, []string{"p-test"}, 10*time.Second)
			if err != nil || dqP1 == nil {
				t.Fatalf("Priority dequeue 1 failed: %v", err)
			}
			if dqP1.ID != "p-high" {
				t.Errorf("expected highest priority job p-high, got %s", dqP1.ID)
			}

			dqP2, _ := store.Dequeue(ctx, []string{"default"}, []string{"p-test"}, 10*time.Second)
			if dqP2 == nil || dqP2.ID != "p-low" {
				t.Errorf("expected lower priority job p-low next, got %v", dqP2)
			}

			_ = store.Ack(ctx, "p-high")
			_ = store.Ack(ctx, "p-low")

			// 6. Test Idempotency Keys & Deduplication
			dedupKey := "dedup-123"
			d1 := &Job{
				ID:                     "d-1",
				DeduplicationKey:       dedupKey,
				DeduplicationExpiresAt: time.Now().Add(500 * time.Millisecond),
				Type:                   "d-test",
				Payload:                []byte("d1"),
			}
			d2 := &Job{
				ID:                     "d-2",
				DeduplicationKey:       dedupKey,
				DeduplicationExpiresAt: time.Now().Add(500 * time.Millisecond),
				Type:                   "d-test",
				Payload:                []byte("d2"),
			}

			// Enqueue d1
			if err := store.Enqueue(ctx, d1); err != nil {
				t.Fatalf("D1 Enqueue failed: %v", err)
			}

			// Enqueue d2 (duplicate key) -> should be deduplicated
			if err := store.Enqueue(ctx, d2); err != nil {
				t.Fatalf("D2 Enqueue errored instead of being deduplicated: %v", err)
			}

			// Verify only d1 is pending, d2 is not enqueued
			dqD, err := store.Dequeue(ctx, []string{"default"}, []string{"d-test"}, 10*time.Second)
			if err != nil || dqD == nil {
				t.Fatalf("Deduplication dequeue failed: %v", err)
			}
			if dqD.ID != "d-1" {
				t.Errorf("expected d-1, got %s", dqD.ID)
			}

			// Try dequeuing again - should be nil (d2 was ignored)
			dqD2, _ := store.Dequeue(ctx, []string{"default"}, []string{"d-test"}, 10*time.Second)
			if dqD2 != nil {
				t.Errorf("expected no other job (d2 should have been deduplicated), got %s", dqD2.ID)
			}

			_ = store.Ack(ctx, "d-1")

			// 7. Test Heartbeat lease extensions
			hbJob := &Job{
				ID:      "hb-job",
				Type:    "hb-test",
				Payload: []byte("hb"),
			}
			_ = store.Enqueue(ctx, hbJob)

			// Dequeue with very short lease (100ms)
			dqHB, err := store.Dequeue(ctx, []string{"default"}, []string{"hb-test"}, 100*time.Millisecond)
			if err != nil || dqHB == nil {
				t.Fatalf("Heartbeat dequeue failed: %v", err)
			}

			// Trigger Heartbeat to extend by another 300ms
			time.Sleep(30 * time.Millisecond) // after 30ms
			err = store.Heartbeat(ctx, dqHB.ID, 300*time.Millisecond)
			if err != nil {
				t.Fatalf("Heartbeat call failed: %v", err)
			}

			// Wait 120ms (total 150ms since claim). Original 100ms lease expired, but extended is active!
			time.Sleep(120 * time.Millisecond)

			// Sweeper should NOT release the job
			releasedCount, _ := store.SweeperReleaseExpired(ctx)
			if releasedCount != 0 {
				t.Errorf("expected sweeper to release 0 jobs, but released %d (heartbeat failed to extend lease)", releasedCount)
			}

			_ = store.Ack(ctx, "hb-job")

			// 8. Test Batch Enqueue & Dequeue
			bJobs := []*Job{
				{ID: "b-1", Type: "b-test", Payload: []byte("1")},
				{ID: "b-2", Type: "b-test", Payload: []byte("2")},
				{ID: "b-3", Type: "b-test", Payload: []byte("3")},
			}

			err = store.EnqueueBatch(ctx, bJobs)
			if err != nil {
				t.Fatalf("EnqueueBatch failed: %v", err)
			}

			dqBatch, err := store.DequeueBatch(ctx, []string{"default"}, []string{"b-test"}, 5, 10*time.Second)
			if err != nil {
				t.Fatalf("DequeueBatch failed: %v", err)
			}
			if len(dqBatch) != 3 {
				t.Errorf("expected 3 jobs in batch, got %d", len(dqBatch))
			}

			for _, j := range dqBatch {
				_ = store.Ack(ctx, j.ID)
			}
		})
	}
}
