package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"reliable-job-queue/queue"
)

func main() {
	log.SetOutput(os.Stdout)
	fmt.Println("=======================================================")
	fmt.Println("💥 Running Job Queue Chaos Crash Recovery Simulation...")
	fmt.Println("=======================================================")

	tmpDir, err := os.MkdirTemp("", "queue-chaos-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "chaos.db")
	store, err := queue.NewSQLiteStore(dbPath)
	if err != nil {
		log.Fatalf("failed to create sqlite store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	jobCount := 5

	fmt.Printf("1. Enqueuing %d long-running jobs...\n", jobCount)
	for i := 0; i < jobCount; i++ {
		job := &queue.Job{
			ID:         fmt.Sprintf("chaos-job-%d", i),
			Type:       "chaos_task",
			Payload:    []byte("chaos payload"),
			MaxRetries: 3,
		}
		if err := store.Enqueue(ctx, job); err != nil {
			log.Fatalf("failed to enqueue: %v", err)
		}
	}

	// Setup first worker pool (which will be "killed" mid-execution)
	fmt.Println("2. Starting initial worker pool to claim jobs...")
	pool1 := queue.NewWorkerPool(
		store,
		3,
		queue.WithPollInterval(10*time.Millisecond),
		queue.WithLeaseDuration(1*time.Second), // short lease
	)

	// Channel to signal when workers have started processing
	startedProcessing := make(chan bool, jobCount)

	pool1.Register("chaos_task", func(ctx context.Context, job *queue.Job) error {
		startedProcessing <- true
		fmt.Printf("   [Worker Pool 1] Claimed %s. Simulating processing...\n", job.ID)
		time.Sleep(3 * time.Second) // takes longer than lease, or represents slow work
		return nil
	})

	pool1.Start()

	// Wait for at least 2 jobs to be claimed and processing
	for i := 0; i < 2; i++ {
		<-startedProcessing
	}

	// Crash the workers!
	fmt.Println("🔥 CHAOS EVENT: Force killing Worker Pool 1 mid-execution!")
	pool1.Stop() // stops workers without letting them complete/ack the database

	// Verify jobs are locked in 'processing' state in the database
	stats, _ := store.GetStats(ctx)
	fmt.Printf("   Queue status post-crash: %+v\n", stats)
	if stats.Processing == 0 {
		log.Fatalf("Expected jobs to be stuck in processing state, got: %+v", stats)
	}

	// Start a second worker pool representing service restart
	fmt.Println("3. Simulating service recovery. Starting Worker Pool 2 with lease sweeper...")
	
	// Create wait group for final successful completions
	var finalWg sync.WaitGroup
	finalWg.Add(jobCount)

	pool2 := queue.NewWorkerPool(
		store,
		3,
		queue.WithPollInterval(50*time.Millisecond),
		queue.WithLeaseDuration(2*time.Second),
		queue.WithSweeperInterval(200*time.Millisecond), // aggressive sweeper for testing
	)

	pool2.Register("chaos_task", func(ctx context.Context, job *queue.Job) error {
		fmt.Printf("   [Worker Pool 2] Dequeued/Recovered %s successfully!\n", job.ID)
		finalWg.Done()
		return nil
	})

	startTime := time.Now()
	pool2.Start()

	// Wait for final successful completions after sweeper recovers the crashed leases
	finalWg.Wait()
	pool2.Stop()

	recoveryTime := time.Since(startTime)
	fmt.Println("-------------------------------------------------------")
	fmt.Printf("✅ Success: All crashed jobs recovered and completed!\n")
	fmt.Printf("   Total Recovery & Execution Time: %v\n", recoveryTime)
	fmt.Println("=======================================================")
}
