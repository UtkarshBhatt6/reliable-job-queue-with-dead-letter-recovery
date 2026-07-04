package queue

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore implements Store using in-memory data structures.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

// NewMemoryStore creates a new in-memory job store.
func NewMemoryStore() *MemoryStore {
	store := &MemoryStore{
		jobs: make(map[string]*Job),
	}

	// Update queue depth metrics in the background
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			store.updateMetrics()
		}
	}()

	return store
}

func (s *MemoryStore) updateMetrics() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var pending, processing, completed, failed, dlq int64
	for _, j := range s.jobs {
		switch j.State {
		case StatePending:
			pending++
		case StateProcessing:
			processing++
		case StateCompleted:
			completed++
		case StateFailed:
			failed++
		case StateDeadLetter:
			dlq++
		}
	}

	QueueDepth.WithLabelValues(string(StatePending)).Set(float64(pending))
	QueueDepth.WithLabelValues(string(StateProcessing)).Set(float64(processing))
	QueueDepth.WithLabelValues(string(StateCompleted)).Set(float64(completed))
	QueueDepth.WithLabelValues(string(StateFailed)).Set(float64(failed))
	QueueDepth.WithLabelValues(string(StateDeadLetter)).Set(float64(dlq))
}

// checkDeduplicationLocked returns true if a duplicate active/valid job exists.
func (s *MemoryStore) checkDeduplicationLocked(job *Job) bool {
	if job.DeduplicationKey == "" {
		return false
	}
	now := time.Now()
	for _, j := range s.jobs {
		if j.DeduplicationKey == job.DeduplicationKey {
			// Skip if TTL has expired
			if !j.DeduplicationExpiresAt.IsZero() && j.DeduplicationExpiresAt.Before(now) {
				continue
			}
			// Active or non-expired completed job qualifies as duplicate
			if j.State != StateCompleted || (!j.DeduplicationExpiresAt.IsZero() && j.DeduplicationExpiresAt.After(now)) {
				return true
			}
		}
	}
	return false
}

func (s *MemoryStore) enqueueLocked(job *Job) {
	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	if job.RunAt.IsZero() {
		job.RunAt = now
	}
	job.State = StatePending
	if job.Queue == "" {
		job.Queue = "default"
	}

	copiedJob := *job
	s.jobs[job.ID] = &copiedJob
	JobsEnqueued.WithLabelValues(job.Type).Inc()
}

// Enqueue adds a new job to the store.
func (s *MemoryStore) Enqueue(ctx context.Context, job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; exists {
		return fmt.Errorf("job %s already exists", job.ID)
	}

	if s.checkDeduplicationLocked(job) {
		// Silently ignore duplicates to maintain idempotency
		return nil
	}

	s.enqueueLocked(job)
	return nil
}

// EnqueueBatch adds multiple new jobs to the store atomically.
func (s *MemoryStore) EnqueueBatch(ctx context.Context, jobs []*Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Verify IDs and existence first
	for _, job := range jobs {
		if _, exists := s.jobs[job.ID]; exists {
			return fmt.Errorf("job %s already exists", job.ID)
		}
	}

	// Enqueue valid, non-duplicate jobs
	for _, job := range jobs {
		if s.checkDeduplicationLocked(job) {
			continue // Skip duplicates
		}
		s.enqueueLocked(job)
	}
	return nil
}

// Dequeue selects and reserves the next available job.
func (s *MemoryStore) Dequeue(ctx context.Context, queues []string, types []string, leaseDuration time.Duration) (*Job, error) {
	jobs, err := s.DequeueBatch(ctx, queues, types, 1, leaseDuration)
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	return jobs[0], nil
}

// DequeueBatch selects and reserves up to batchSize next available jobs.
func (s *MemoryStore) DequeueBatch(ctx context.Context, queues []string, types []string, batchSize int, leaseDuration time.Duration) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if batchSize <= 0 {
		return []*Job{}, nil
	}

	now := time.Now()
	var candidates []*Job

	for _, j := range s.jobs {
		if j.State != StatePending && j.State != StateFailed {
			continue
		}
		if j.RunAt.After(now) {
			continue
		}

		if len(queues) > 0 {
			matched := false
			for _, q := range queues {
				if j.Queue == q {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		if len(types) > 0 {
			matched := false
			for _, t := range types {
				if j.Type == t {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		candidates = append(candidates, j)
	}

	if len(candidates) == 0 {
		return []*Job{}, nil
	}

	// Sort: 1) Priority (descending) 2) RunAt (ascending) 3) CreatedAt (ascending)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority > candidates[j].Priority
		}
		if candidates[i].RunAt.Equal(candidates[j].RunAt) {
			return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
		}
		return candidates[i].RunAt.Before(candidates[j].RunAt)
	})

	limit := batchSize
	if limit > len(candidates) {
		limit = len(candidates)
	}

	results := make([]*Job, 0, limit)
	for i := 0; i < limit; i++ {
		selected := candidates[i]
		selected.State = StateProcessing
		selected.ReservedUntil = now.Add(leaseDuration)
		selected.UpdatedAt = now

		copiedJob := *selected
		results = append(results, &copiedJob)
	}

	return results, nil
}

// Ack marks the job as completed.
func (s *MemoryStore) Ack(ctx context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}

	if j.State != StateProcessing {
		return fmt.Errorf("%w: job is in %s state, not processing", ErrInvalidState, j.State)
	}

	j.State = StateCompleted
	j.ReservedUntil = time.Time{}
	j.UpdatedAt = time.Now()

	JobsProcessed.WithLabelValues(j.Type, string(StateCompleted)).Inc()
	return nil
}

// Nack schedules the job for retry or marks it as dead-lettered.
func (s *MemoryStore) Nack(ctx context.Context, jobID string, nextRunIn time.Duration, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}

	if j.State != StateProcessing {
		return fmt.Errorf("%w: job is in %s state, not processing", ErrInvalidState, j.State)
	}

	j.Retries++
	j.LastError = err.Error()
	j.ReservedUntil = time.Time{}
	j.UpdatedAt = time.Now()

	if j.Retries >= j.MaxRetries {
		j.State = StateDeadLetter
		j.RunAt = time.Time{}
		JobsProcessed.WithLabelValues(j.Type, string(StateDeadLetter)).Inc()
	} else {
		j.State = StateFailed
		j.RunAt = time.Now().Add(nextRunIn)
		JobsRetried.WithLabelValues(j.Type).Inc()
	}

	return nil
}

// Heartbeat extends the visibility timeout of a processing job.
func (s *MemoryStore) Heartbeat(ctx context.Context, jobID string, extendBy time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, exists := s.jobs[jobID]
	if !exists {
		return ErrJobNotFound
	}

	if j.State != StateProcessing {
		return fmt.Errorf("%w: job is in %s state, not processing", ErrInvalidState, j.State)
	}

	j.ReservedUntil = time.Now().Add(extendBy)
	j.UpdatedAt = time.Now()
	return nil
}

// GetStats returns the queue metrics.
func (s *MemoryStore) GetStats(ctx context.Context) (Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var stats Stats
	for _, j := range s.jobs {
		switch j.State {
		case StatePending:
			stats.Pending++
		case StateProcessing:
			stats.Processing++
		case StateCompleted:
			stats.Completed++
		case StateFailed:
			stats.Failed++
		case StateDeadLetter:
			stats.DeadLetter++
		}
	}
	return stats, nil
}

// ListJobs lists jobs filtered by state with pagination.
func (s *MemoryStore) ListJobs(ctx context.Context, state JobState, limit, offset int) ([]*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []*Job
	for _, j := range s.jobs {
		if j.State == state {
			copied := *j
			matched = append(matched, &copied)
		}
	}

	// Sort by CreatedAt descending (latest first)
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	if offset >= len(matched) {
		return []*Job{}, nil
	}

	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}

	return matched[offset:end], nil
}

// RedriveDeadLetter resets retries and schedules dead-lettered jobs to run immediately.
func (s *MemoryStore) RedriveDeadLetter(ctx context.Context, jobIDs []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	now := time.Now()
	redriveAll := len(jobIDs) == 0

	idMap := make(map[string]bool)
	for _, id := range jobIDs {
		idMap[id] = true
	}

	for _, j := range s.jobs {
		if j.State == StateDeadLetter && (redriveAll || idMap[j.ID]) {
			j.State = StatePending
			j.Retries = 0
			j.RunAt = now
			j.ReservedUntil = time.Time{}
			j.LastError = ""
			j.UpdatedAt = now
			count++
		}
	}

	return count, nil
}

// DeleteDeadLetter permanently removes dead-lettered jobs.
func (s *MemoryStore) DeleteDeadLetter(ctx context.Context, jobIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	deleteAll := len(jobIDs) == 0

	idMap := make(map[string]bool)
	for _, id := range jobIDs {
		idMap[id] = true
	}

	for id, j := range s.jobs {
		if j.State == StateDeadLetter && (deleteAll || idMap[id]) {
			delete(s.jobs, id)
		}
	}

	return nil
}

// SweeperReleaseExpired releases jobs locked in the 'processing' state where ReservedUntil is past.
func (s *MemoryStore) SweeperReleaseExpired(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	now := time.Now()

	for _, j := range s.jobs {
		if j.State == StateProcessing && j.ReservedUntil.Before(now) {
			j.Retries++
			j.ReservedUntil = time.Time{}
			j.UpdatedAt = now
			j.LastError = "lease expired: worker heartbeat timeout"

			if j.Retries >= j.MaxRetries {
				j.State = StateDeadLetter
				j.RunAt = time.Time{}
				JobsProcessed.WithLabelValues(j.Type, string(StateDeadLetter)).Inc()
			} else {
				j.State = StateFailed
				j.RunAt = now.Add(5 * time.Second) // retry in 5s
				JobsRetried.WithLabelValues(j.Type).Inc()
			}
			count++
		}
	}

	return count, nil
}
