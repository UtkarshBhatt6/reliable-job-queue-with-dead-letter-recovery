package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Handler represents a function that processes a job.
type Handler func(ctx context.Context, job *Job) error

// contextKey defines a private type for context keys to avoid collisions.
type contextKey string

const heartbeatKey contextKey = "job_heartbeat"

// HeartbeatFunc represents a function type registered in the context to trigger heartbeats.
type HeartbeatFunc func(extendBy time.Duration) error

// Heartbeat allows handlers to extend the visibility timeout of a running job.
func Heartbeat(ctx context.Context, extendBy time.Duration) error {
	f, ok := ctx.Value(heartbeatKey).(HeartbeatFunc)
	if !ok {
		return fmt.Errorf("heartbeat not supported in this context")
	}
	return f(extendBy)
}

// WorkerPool manages concurrent execution of jobs.
type WorkerPool struct {
	store           Store
	concurrency     int
	queues          []string
	handlers        map[string]Handler
	pollInterval    time.Duration
	leaseDuration   time.Duration
	sweeperInterval time.Duration
	retryPolicy     RetryPolicy

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

// WithQueues sets which queues this worker pool polls from.
func WithQueues(queues ...string) WorkerOption {
	return func(w *WorkerPool) { w.queues = queues }
}

// WithRetryPolicy configures a custom retry policy for backoffs.
func WithRetryPolicy(p RetryPolicy) WorkerOption {
	return func(w *WorkerPool) { w.retryPolicy = p }
}

// NewWorkerPool creates a new WorkerPool.
func NewWorkerPool(store Store, concurrency int, options ...WorkerOption) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	w := &WorkerPool{
		store:           store,
		concurrency:     concurrency,
		queues:          []string{"default"},
		handlers:        make(map[string]Handler),
		pollInterval:    200 * time.Millisecond,
		leaseDuration:   30 * time.Second,
		sweeperInterval: 5 * time.Second,
		retryPolicy:     DefaultRetryPolicy(),
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

	log.Printf("[WorkerPool] Started with %d concurrent workers (queues: %v, lease visibility: %v)", w.concurrency, w.queues, w.leaseDuration)
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
			w.mu.RLock()
			var types []string
			for t := range w.handlers {
				types = append(types, t)
			}
			w.mu.RUnlock()

			if len(types) == 0 {
				continue
			}

			// Poll a job
			job, err := w.store.Dequeue(w.ctx, w.queues, types, w.leaseDuration)
			if err != nil {
				log.Printf("[Worker %d] error dequeuing job: %v", workerID, err)
				continue
			}

			if job == nil {
				continue
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

	// Extract context tracing
	parentCtx := ExtractTraceContext(w.ctx, job)
	ctx, span := Tracer().Start(
		parentCtx,
		fmt.Sprintf("job_handler:%s", job.Type),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("job.id", job.ID),
			attribute.String("job.queue", job.Queue),
			attribute.String("job.type", job.Type),
			attribute.Int("job.retries", job.Retries),
			attribute.Int("job.max_retries", job.MaxRetries),
		),
	)
	defer span.End()

	// Inject heartbeat callback into context
	heartbeatFn := func(extendBy time.Duration) error {
		return w.store.Heartbeat(w.ctx, job.ID, extendBy)
	}
	ctx = context.WithValue(ctx, heartbeatKey, HeartbeatFunc(heartbeatFn))

	// Execute handler
	err := handler(ctx, job)

	duration := time.Since(startTime).Seconds()
	JobsProcessingDuration.WithLabelValues(job.Type).Observe(duration)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("[Worker] Job %s (type: %s) failed: %v", job.ID, job.Type, err)

		backoff := w.retryPolicy.CalculateBackoff(job.Retries)
		nackErr := w.store.Nack(w.ctx, job.ID, backoff, err)
		if nackErr != nil {
			log.Printf("[Worker] Error calling Nack for job %s: %v", job.ID, nackErr)
		}
	} else {
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
