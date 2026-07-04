package queue

import (
	"context"
	"errors"
	"time"
)

// Common errors.
var (
	ErrJobNotFound = errors.New("job not found")
	ErrInvalidState = errors.New("invalid job state transition")
)

// JobState represents the lifecycle state of a job.
type JobState string

const (
	StatePending    JobState = "pending"
	StateProcessing JobState = "processing"
	StateCompleted  JobState = "completed"
	StateFailed     JobState = "failed" // Transient fail (retrying)
	StateDeadLetter JobState = "dead_letter"
)

// Job represents a single unit of work.
type Job struct {
	ID            string            `json:"id"`
	Queue         string            `json:"queue"`
	Type          string            `json:"type"`
	Payload       []byte            `json:"payload"`
	State         JobState          `json:"state"`
	Retries       int               `json:"retries"`
	MaxRetries    int               `json:"max_retries"`
	RunAt         time.Time         `json:"run_at"`
	ReservedUntil time.Time         `json:"reserved_until"`
	LastError     string            `json:"last_error,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	TraceContext  map[string]string `json:"trace_context,omitempty"` // Trace context carrier for OTel
}

// Stats represents the counts of jobs in various states.
type Stats struct {
	Pending    int64 `json:"pending"`
	Processing int64 `json:"processing"`
	Completed  int64 `json:"completed"`
	Failed     int64 `json:"failed"`
	DeadLetter int64 `json:"dead_letter"`
}

// Store defines the storage engine interface for the reliable job queue.
type Store interface {
	// Enqueue adds a new job to the store.
	Enqueue(ctx context.Context, job *Job) error

	// Dequeue selects and reserves the next available job of the specified queues and types.
	// It returns nil, nil if no jobs are available.
	Dequeue(ctx context.Context, queues []string, types []string, leaseDuration time.Duration) (*Job, error)

	// Ack marks the job as completed.
	Ack(ctx context.Context, jobID string) error

	// Nack increments retries, saves the error, and schedules the job for a retry after nextRunIn.
	// If the job exceeds MaxRetries, it transitions to StateDeadLetter.
	Nack(ctx context.Context, jobID string, nextRunIn time.Duration, err error) error

	// GetStats returns the queue metrics.
	GetStats(ctx context.Context) (Stats, error)

	// ListJobs lists jobs filtered by state with pagination.
	ListJobs(ctx context.Context, state JobState, limit, offset int) ([]*Job, error)

	// RedriveDeadLetter resets retries and schedules dead-lettered jobs to run immediately.
	RedriveDeadLetter(ctx context.Context, jobIDs []string) (int, error)

	// DeleteDeadLetter permanently removes dead-lettered jobs.
	DeleteDeadLetter(ctx context.Context, jobIDs []string) error

	// SweeperReleaseExpired releases jobs locked in the 'processing' state where ReservedUntil is past.
	// Returns the number of jobs released or sent to DLQ.
	SweeperReleaseExpired(ctx context.Context) (int, error)
}
