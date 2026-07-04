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
	fmt.Println("🚀 Running Job Queue Performance Benchmark Suite...")
	fmt.Println("=======================================================")

	// Benchmark In-Memory Store
	runBenchmark("In-Memory Store", func() (queue.Store, func()) {
		return queue.NewMemoryStore(), func() {}
	})

	// Benchmark SQLite Store
	runBenchmark("SQLite Store (WAL Mode)", func() (queue.Store, func()) {
		tmpDir, err := os.MkdirTemp("", "queue-bench-*")
		if err != nil {
			log.Fatalf("failed to create temp dir: %v", err)
		}
		dbPath := filepath.Join(tmpDir, "bench.db")
		store, err := queue.NewSQLiteStore(dbPath)
		if err != nil {
			log.Fatalf("failed to create sqlite store: %v", err)
		}
		cleanup := func() {
			store.Close()
			os.RemoveAll(tmpDir)
		}
		return store, cleanup
	})
}

func runBenchmark(name string, setup func() (queue.Store, func())) {
	store, cleanup := setup()
	defer cleanup()

	ctx := context.Background()
	jobCount := 1000
	concurrency := 10

	fmt.Printf("\nBenchmarking: %s\n", name)
	fmt.Printf("Config: Jobs=%d, Workers=%d\n", jobCount, concurrency)

	// 1. Measure Enqueue Throughput (Sequentially to avoid SQLite setup lock collisions)
	startEnqueue := time.Now()
	batchSize := 100
	batches := jobCount / batchSize

	for i := 0; i < batches; i++ {
		batchJobs := make([]*queue.Job, 0, batchSize)
		for j := 0; j < batchSize; j++ {
			jobID := fmt.Sprintf("bench-job-%s-%d-%d", name, i, j)
			batchJobs = append(batchJobs, &queue.Job{
				ID:         jobID,
				Type:       "bench_task",
				Payload:    []byte("benchmark payload"),
				MaxRetries: 3,
			})
		}
		err := store.EnqueueBatch(ctx, batchJobs)
		if err != nil {
			log.Fatalf("EnqueueBatch failed for %s: %v", name, err)
		}
	}
	enqueueDuration := time.Since(startEnqueue)
	enqueueTPS := float64(jobCount) / enqueueDuration.Seconds()

	// 2. Measure Dequeue & Process Throughput (Concurrent worker routines)
	var processWg sync.WaitGroup
	processWg.Add(jobCount)

	pool := queue.NewWorkerPool(
		store,
		concurrency,
		queue.WithPollInterval(5*time.Millisecond),
		queue.WithLeaseDuration(10*time.Second),
	)

	// Keep track of processing latencies
	var latenciesMu sync.Mutex
	var totalLatency time.Duration

	pool.Register("bench_task", func(ctx context.Context, job *queue.Job) error {
		latency := time.Since(job.CreatedAt)
		latenciesMu.Lock()
		totalLatency += latency
		latenciesMu.Unlock()
		
		processWg.Done()
		return nil
	})

	startProcess := time.Now()
	pool.Start()

	// Wait for all jobs to be completed
	processWg.Wait()
	processDuration := time.Since(startProcess)
	pool.Stop()

	processTPS := float64(jobCount) / processDuration.Seconds()
	avgLatency := totalLatency / time.Duration(jobCount)

	fmt.Println("-------------------------------------------------------")
	fmt.Printf(" Enqueue Speed  : %.2f jobs/sec (Time: %v)\n", enqueueTPS, enqueueDuration)
	fmt.Printf(" Process Speed  : %.2f jobs/sec (Time: %v)\n", processTPS, processDuration)
	fmt.Printf(" Avg Latency    : %v\n", avgLatency)
	fmt.Println("=======================================================")
}
