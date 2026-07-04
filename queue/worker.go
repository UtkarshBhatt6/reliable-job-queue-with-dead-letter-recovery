package queue

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Handler represents a function that processes a job.
type Handler func(ctx context.Context, job *Job) error

// WorkerPool manages concurrent execution of jobs.
type WorkerPool struct {
	store           Store
	concurrency     int
	handlers        map[string]Handler
	pollInterval    time.Duration
	leaseDuration   time.Duration
	sweeperInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu sync.RWMutex
}

// WorkerOption defines custom configurations for the WorkerPool.
type WorkerOption func(*WorkerPool)

// WithPollInterval sets the polling frequency when the queue is empty.
func WithPollInterval(d time.Duration) WorkerOption {
	return func(w *WorkerPool) { w.pollInterval = d }
}

// WithLeaseDuration sets the visibility timeout of dequeued jobs.
func WithLeaseDuration(d time.Duration) WorkerOption {
	return func(w *WorkerPool) { w.leaseDuration = d }
}

// WithSweeperInterval sets how often the visibility sweeper checks for expired leases.
func WithSweeperInterval(d time.Duration) WorkerOption {
	return func(w *WorkerPool) { w.sweeperInterval = d }
}

// NewWorkerPool creates a new WorkerPool.
func NewWorkerPool(store Store, concurrency int, options ...WorkerOption) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	w := &WorkerPool{
		store:           store,
		concurrency:     concurrency,
		handlers:        make(map[string]Handler),
		pollInterval:    200 * time.Millisecond,
		leaseDuration:   30 * time.Second,
		sweeperInterval: 5 * time.Second,
		ctx:             ctx,
		cancel:          cancel,
	}

	for _, opt := range options {
		opt(w)
	}

	return w
}

// Register registers a handler function for a specific job type.
func (w *WorkerPool) Register(jobType string, handler Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[jobType] = handler
}

// Start spawns the worker routines and lease sweeper.
func (w *WorkerPool) Start() {
	// Start workers
	w.wg.Add(w.concurrency)
	for i := 0; i < w.concurrency; i++ {
		go w.workerLoop(i)
	}

	// Start lease sweeper
	w.wg.Add(1)
	go w.sweeperLoop()

	log.Printf("[WorkerPool] Started with %d concurrent workers (lease visibility: %v)", w.concurrency, w.leaseDuration)
}

// Stop signals all workers to stop and waits for active jobs to complete.
func (w *WorkerPool) Stop() {
	log.Println("[WorkerPool] Stopping workers...")
	w.cancel()
	w.wg.Wait()
	log.Println("[WorkerPool] Stopped successfully")
}

func (w *WorkerPool) workerLoop(workerID int) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			// Fetch types we have handlers registered for
			w.mu.RLock()
			var types []string
			for t := range w.handlers {
				types = append(types, t)
			}
			w.mu.RUnlock()

			if len(types) == 0 {
				continue // No handlers registered yet
			}

			// Poll a job
			job, err := w.store.Dequeue(w.ctx, types, w.leaseDuration)
			if err != nil {
				log.Printf("[Worker %d] error dequeuing job: %v", workerID, err)
				continue
			}

			if job == nil {
				continue // No jobs available
			}

			w.processJob(job)
		}
	}
}

func (w *WorkerPool) processJob(job *Job) {
	w.mu.RLock()
	handler, ok := w.handlers[job.Type]
	w.mu.RUnlock()

	if !ok {
		log.Printf("[Worker] No handler registered for job type %q", job.Type)
		return
	}

	startTime := time.Now()

	// Extract tracing context from job to link producer span to worker execution
	parentCtx := ExtractTraceContext(w.ctx, job)
	ctx, span := Tracer().Start(
		parentCtx,
		fmt.Sprintf("job_handler:%s", job.Type),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("job.id", job.ID),
			attribute.String("job.type", job.Type),
			attribute.Int("job.retries", job.Retries),
			attribute.Int("job.max_retries", job.MaxRetries),
		),
	)
	defer span.End()

	// Execute handler
	err := handler(ctx, job)

	duration := time.Since(startTime).Seconds()
	JobsProcessingDuration.WithLabelValues(job.Type).Observe(duration)

	if err != nil {
		// Job execution failed
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("[Worker] Job %s (type: %s) failed: %v", job.ID, job.Type, err)

		backoff := w.calculateBackoff(job.Retries)
		nackErr := w.store.Nack(w.ctx, job.ID, backoff, err)
		if nackErr != nil {
			log.Printf("[Worker] Error calling Nack for job %s: %v", job.ID, nackErr)
		}
	} else {
		// Job execution succeeded
		span.SetStatus(codes.Ok, "success")
		log.Printf("[Worker] Job %s (type: %s) completed successfully in %.3fs", job.ID, job.Type, duration)

		ackErr := w.store.Ack(w.ctx, job.ID)
		if ackErr != nil {
			log.Printf("[Worker] Error calling Ack for job %s: %v", job.ID, ackErr)
		}
	}
}

func (w *WorkerPool) sweeperLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.sweeperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			count, err := w.store.SweeperReleaseExpired(w.ctx)
			if err != nil {
				log.Printf("[Sweeper] error sweeping expired leases: %v", err)
				continue
			}
			if count > 0 {
				log.Printf("[Sweeper] Released %d expired job leases", count)
			}
		}
	}
}

func (w *WorkerPool) calculateBackoff(retries int) time.Duration {
	// Exponential backoff: 2^retries * 1 second (1s, 2s, 4s, 8s, 16s...)
	// capped at 5 minutes
	base := float64(1)
	exponent := math.Min(float64(retries), 8) // capped at 2^8 = 256s (~4.2 min)
	backoffSec := base * math.Pow(2, exponent)
	return time.Duration(backoffSec) * time.Second
}
