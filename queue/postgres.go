package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates and initializes a new Postgres-backed job store.
func NewPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	// Schema setup
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id VARCHAR(255) PRIMARY KEY,
		queue VARCHAR(255) NOT NULL DEFAULT 'default',
		priority INTEGER NOT NULL DEFAULT 0,
		deduplication_key VARCHAR(255),
		deduplication_expires_at TIMESTAMP WITH TIME ZONE,
		type VARCHAR(255) NOT NULL,
		payload BYTEA NOT NULL,
		state VARCHAR(50) NOT NULL,
		retries INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 3,
		run_at TIMESTAMP WITH TIME ZONE NOT NULL,
		reserved_until TIMESTAMP WITH TIME ZONE,
		last_error TEXT,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL,
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
		trace_context TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_priority_run_at ON jobs (queue, state, priority DESC, run_at);
	CREATE INDEX IF NOT EXISTS idx_jobs_dedup_key ON jobs (deduplication_key);
	CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (type);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize postgres schema: %w", err)
	}

	store := &PostgresStore{db: db}

	// Update queue depth metrics in the background
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			store.updateMetrics()
		}
	}()

	return store, nil
}

func (s *PostgresStore) updateMetrics() {
	stats, err := s.GetStats(context.Background())
	if err != nil {
		return
	}
	QueueDepth.WithLabelValues(string(StatePending)).Set(float64(stats.Pending))
	QueueDepth.WithLabelValues(string(StateProcessing)).Set(float64(stats.Processing))
	QueueDepth.WithLabelValues(string(StateCompleted)).Set(float64(stats.Completed))
	QueueDepth.WithLabelValues(string(StateFailed)).Set(float64(stats.Failed))
	QueueDepth.WithLabelValues(string(StateDeadLetter)).Set(float64(stats.DeadLetter))
}

// Close closes the database connection.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// checkDeduplication checks if an active duplicate job exists.
func (s *PostgresStore) checkDeduplication(ctx context.Context, tx *sql.Tx, job *Job, now time.Time) (bool, error) {
	if job.DeduplicationKey == "" {
		return false, nil
	}

	query := `
		SELECT COUNT(*) FROM jobs
		WHERE deduplication_key = $1
		  AND (state != 'completed' OR deduplication_expires_at > $2)
	`
	var count int
	var err error
	if tx != nil {
		err = tx.QueryRowContext(ctx, query, job.DeduplicationKey, now).Scan(&count)
	} else {
		err = s.db.QueryRowContext(ctx, query, job.DeduplicationKey, now).Scan(&count)
	}

	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Enqueue adds a job to the Postgres store.
func (s *PostgresStore) Enqueue(ctx context.Context, job *Job) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin enqueue transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now()
	duplicate, err := s.checkDeduplication(ctx, tx, job, now)
	if err != nil {
		return fmt.Errorf("failed deduplication check: %w", err)
	}
	if duplicate {
		return nil // Deduplicated successfully
	}

	if err := s.enqueueTx(ctx, tx, job, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit enqueue: %w", err)
	}

	return nil
}

// EnqueueBatch adds multiple new jobs to the store atomically.
func (s *PostgresStore) EnqueueBatch(ctx context.Context, jobs []*Job) error {
	if len(jobs) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin batch enqueue transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now()
	for _, job := range jobs {
		duplicate, err := s.checkDeduplication(ctx, tx, job, now)
		if err != nil {
			return fmt.Errorf("failed deduplication check: %w", err)
		}
		if duplicate {
			continue // Skip duplicate
		}

		if err := s.enqueueTx(ctx, tx, job, now); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch enqueue: %w", err)
	}

	return nil
}

func (s *PostgresStore) enqueueTx(ctx context.Context, tx *sql.Tx, job *Job, now time.Time) error {
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

	var traceJSON []byte
	if job.TraceContext != nil {
		var err error
		traceJSON, err = json.Marshal(job.TraceContext)
		if err != nil {
			return fmt.Errorf("failed to marshal trace context: %w", err)
		}
	}

	query := `
	INSERT INTO jobs (id, queue, priority, deduplication_key, deduplication_expires_at, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
	`

	var reservedUntil interface{}
	if !job.ReservedUntil.IsZero() {
		reservedUntil = job.ReservedUntil
	}

	var dedupExpiresAt interface{}
	if !job.DeduplicationExpiresAt.IsZero() {
		dedupExpiresAt = job.DeduplicationExpiresAt
	}

	_, err := tx.ExecContext(ctx, query,
		job.ID,
		job.Queue,
		job.Priority,
		sql.NullString{String: job.DeduplicationKey, Valid: job.DeduplicationKey != ""},
		dedupExpiresAt,
		job.Type,
		job.Payload,
		string(job.State),
		job.Retries,
		job.MaxRetries,
		job.RunAt,
		reservedUntil,
		job.LastError,
		job.CreatedAt,
		job.UpdatedAt,
		string(traceJSON),
	)

	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	JobsEnqueued.WithLabelValues(job.Type).Inc()
	return nil
}

// Dequeue selects and reserves the next available job using FOR UPDATE SKIP LOCKED.
func (s *PostgresStore) Dequeue(ctx context.Context, queues []string, types []string, leaseDuration time.Duration) (*Job, error) {
	jobs, err := s.DequeueBatch(ctx, queues, types, 1, leaseDuration)
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	return jobs[0], nil
}

// DequeueBatch selects and reserves up to batchSize next available jobs.
func (s *PostgresStore) DequeueBatch(ctx context.Context, queues []string, types []string, batchSize int, leaseDuration time.Duration) ([]*Job, error) {
	if batchSize <= 0 {
		return []*Job{}, nil
	}

	now := time.Now()
	newReservedUntil := now.Add(leaseDuration)

	// Build sub-select query filters
	var subFilters []string
	var args []interface{}
	args = append(args, now) // $1

	subFilters = append(subFilters, "(state = 'pending' OR state = 'failed')")
	subFilters = append(subFilters, "run_at <= $1")

	argIdx := 2
	if len(queues) > 0 {
		placeholders := make([]string, len(queues))
		for i, q := range queues {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, q)
			argIdx++
		}
		subFilters = append(subFilters, fmt.Sprintf("queue IN (%s)", strings.Join(placeholders, ",")))
	}

	if len(types) > 0 {
		placeholders := make([]string, len(types))
		for i, t := range types {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, t)
			argIdx++
		}
		subFilters = append(subFilters, fmt.Sprintf("type IN (%s)", strings.Join(placeholders, ",")))
	}

	subSelect := fmt.Sprintf(`
		SELECT id FROM jobs
		WHERE %s
		ORDER BY priority DESC, run_at ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT $%d
	`, strings.Join(subFilters, " AND "), argIdx)
	
	// Add batchSize argument ($argIdx)
	args = append(args, batchSize)
	argIdx++

	// Final update query using $argIdx and $(argIdx+1) for reserved_until and updated_at
	updateQuery := fmt.Sprintf(`
		UPDATE jobs
		SET state = 'processing', reserved_until = $%d, updated_at = $%d
		WHERE id IN (%s)
		RETURNING id, queue, priority, deduplication_key, deduplication_expires_at, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context
	`, argIdx, argIdx+1, subSelect)

	// Append lease durations and now timestamp to arguments
	args = append(args, newReservedUntil, now)

	rows, err := s.db.QueryContext(ctx, updateQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed dequeuing postgres batch: %w", err)
	}
	defer rows.Close()

	var claimed []*Job
	for rows.Next() {
		var j Job
		var reservedUntilStr sql.NullTime
		var dedupExpiresAt sql.NullTime
		var dedupKeyStr sql.NullString
		var lastErrStr sql.NullString
		var traceContextStr sql.NullString

		err := rows.Scan(
			&j.ID,
			&j.Queue,
			&j.Priority,
			&dedupKeyStr,
			&dedupExpiresAt,
			&j.Type,
			&j.Payload,
			&j.State,
			&j.Retries,
			&j.MaxRetries,
			&j.RunAt,
			&reservedUntilStr,
			&lastErrStr,
			&j.CreatedAt,
			&j.UpdatedAt,
			&traceContextStr,
		)
		if err != nil {
			return nil, err
		}

		if reservedUntilStr.Valid {
			j.ReservedUntil = reservedUntilStr.Time
		}
		if dedupExpiresAt.Valid {
			j.DeduplicationExpiresAt = dedupExpiresAt.Time
		}
		if dedupKeyStr.Valid {
			j.DeduplicationKey = dedupKeyStr.String
		}
		if lastErrStr.Valid {
			j.LastError = lastErrStr.String
		}
		if traceContextStr.Valid && traceContextStr.String != "" {
			_ = json.Unmarshal([]byte(traceContextStr.String), &j.TraceContext)
		}

		claimed = append(claimed, &j)
	}

	return claimed, nil
}

// Ack marks the job as completed successfully.
func (s *PostgresStore) Ack(ctx context.Context, jobID string) error {
	now := time.Now()
	query := `
		UPDATE jobs
		SET state = 'completed', reserved_until = NULL, updated_at = $1
		WHERE id = $2 AND state = 'processing'
	`
	result, err := s.db.ExecContext(ctx, query, now, jobID)
	if err != nil {
		return fmt.Errorf("failed to ack job: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}

	var jobType string
	err = s.db.QueryRowContext(ctx, "SELECT type FROM jobs WHERE id = $1", jobID).Scan(&jobType)
	if err == nil {
		JobsProcessed.WithLabelValues(jobType, string(StateCompleted)).Inc()
	}

	return nil
}

// Nack handles failures. Increments retries and transitions to DLQ if max_retries reached.
func (s *PostgresStore) Nack(ctx context.Context, jobID string, nextRunIn time.Duration, lastErr error) error {
	now := time.Now()

	var retries, maxRetries int
	var jobType string
	querySelect := "SELECT retries, max_retries, type FROM jobs WHERE id = $1 AND state = 'processing'"
	err := s.db.QueryRowContext(ctx, querySelect, jobID).Scan(&retries, &maxRetries, &jobType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return fmt.Errorf("failed to fetch job for nack: %w", err)
	}

	newRetries := retries + 1
	var newState string
	var nextRunAt interface{}

	if newRetries > maxRetries {
		newState = string(StateDeadLetter)
		nextRunAt = time.Time{}
	} else {
		newState = string(StateFailed)
		nextRunAt = now.Add(nextRunIn)
	}

	queryUpdate := `
		UPDATE jobs
		SET state = $1, retries = $2, last_error = $3, reserved_until = NULL, run_at = $4, updated_at = $5
		WHERE id = $6 AND state = 'processing'
	`

	_, err = s.db.ExecContext(ctx, queryUpdate, newState, newRetries, lastErr.Error(), nextRunAt, now, jobID)
	if err != nil {
		return fmt.Errorf("failed to nack job: %w", err)
	}

	if newRetries > maxRetries {
		JobsProcessed.WithLabelValues(jobType, string(StateDeadLetter)).Inc()
	} else {
		JobsRetried.WithLabelValues(jobType).Inc()
	}

	return nil
}

// Heartbeat extends the visibility timeout of a processing job.
func (s *PostgresStore) Heartbeat(ctx context.Context, jobID string, extendBy time.Duration) error {
	now := time.Now()
	newReservedUntil := now.Add(extendBy)

	query := `
		UPDATE jobs
		SET reserved_until = $1, updated_at = $2
		WHERE id = $3 AND state = 'processing'
	`
	result, err := s.db.ExecContext(ctx, query, newReservedUntil, now, jobID)
	if err != nil {
		return fmt.Errorf("failed heartbeat update: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrJobNotFound
	}
	return nil
}

// GetStats returns current counts of jobs by state.
func (s *PostgresStore) GetStats(ctx context.Context) (Stats, error) {
	query := "SELECT state, COUNT(*) FROM jobs GROUP BY state"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return Stats{}, fmt.Errorf("failed to query stats: %w", err)
	}
	defer rows.Close()

	var stats Stats
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return Stats{}, err
		}
		switch JobState(state) {
		case StatePending:
			stats.Pending = count
		case StateProcessing:
			stats.Processing = count
		case StateCompleted:
			stats.Completed = count
		case StateFailed:
			stats.Failed = count
		case StateDeadLetter:
			stats.DeadLetter = count
		}
	}

	return stats, nil
}

// ListJobs returns jobs filtered by state with pagination.
func (s *PostgresStore) ListJobs(ctx context.Context, state JobState, limit, offset int) ([]*Job, error) {
	query := `
		SELECT id, queue, priority, deduplication_key, deduplication_expires_at, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context
		FROM jobs
		WHERE state = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
	rows, err := s.db.QueryContext(ctx, query, string(state), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		var j Job
		var reservedUntilStr sql.NullTime
		var dedupExpiresAt sql.NullTime
		var dedupKeyStr sql.NullString
		var lastErrStr sql.NullString
		var traceContextStr sql.NullString

		err := rows.Scan(
			&j.ID,
			&j.Queue,
			&j.Priority,
			&dedupKeyStr,
			&dedupExpiresAt,
			&j.Type,
			&j.Payload,
			&j.State,
			&j.Retries,
			&j.MaxRetries,
			&j.RunAt,
			&reservedUntilStr,
			&lastErrStr,
			&j.CreatedAt,
			&j.UpdatedAt,
			&traceContextStr,
		)
		if err != nil {
			return nil, err
		}

		if reservedUntilStr.Valid {
			j.ReservedUntil = reservedUntilStr.Time
		}
		if dedupExpiresAt.Valid {
			j.DeduplicationExpiresAt = dedupExpiresAt.Time
		}
		if dedupKeyStr.Valid {
			j.DeduplicationKey = dedupKeyStr.String
		}
		if lastErrStr.Valid {
			j.LastError = lastErrStr.String
		}
		if traceContextStr.Valid && traceContextStr.String != "" {
			_ = json.Unmarshal([]byte(traceContextStr.String), &j.TraceContext)
		}

		jobs = append(jobs, &j)
	}

	return jobs, nil
}

// RedriveDeadLetter resets retries and schedules dead-lettered jobs to run immediately.
func (s *PostgresStore) RedriveDeadLetter(ctx context.Context, jobIDs []string) (int, error) {
	now := time.Now()
	var query string
	var args []interface{}
	args = append(args, now, now)

	if len(jobIDs) == 0 {
		query = `
			UPDATE jobs
			SET state = 'pending', retries = 0, last_error = NULL, reserved_until = NULL, run_at = $1, updated_at = $2
			WHERE state = 'dead_letter'
		`
	} else {
		placeholders := make([]string, len(jobIDs))
		for i, id := range jobIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+3)
			args = append(args, id)
		}
		query = fmt.Sprintf(`
			UPDATE jobs
			SET state = 'pending', retries = 0, last_error = NULL, reserved_until = NULL, run_at = $1, updated_at = $2
			WHERE state = 'dead_letter' AND id IN (%s)
		`, strings.Join(placeholders, ","))
	}

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to redrive jobs: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(rows), nil
}

// DeleteDeadLetter permanently removes dead-lettered jobs.
func (s *PostgresStore) DeleteDeadLetter(ctx context.Context, jobIDs []string) error {
	var query string
	var args []interface{}

	if len(jobIDs) == 0 {
		query = "DELETE FROM jobs WHERE state = 'dead_letter'"
	} else {
		placeholders := make([]string, len(jobIDs))
		for i, id := range jobIDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, id)
		}
		query = fmt.Sprintf("DELETE FROM jobs WHERE state = 'dead_letter' AND id IN (%s)", strings.Join(placeholders, ","))
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete dead-lettered jobs: %w", err)
	}

	return nil
}

// SweeperReleaseExpired releases jobs locked in 'processing' whose lease has expired.
func (s *PostgresStore) SweeperReleaseExpired(ctx context.Context) (int, error) {
	now := time.Now()

	querySelect := `
		SELECT id, type, retries, max_retries
		FROM jobs
		WHERE state = 'processing' AND reserved_until < $1
	`
	rows, err := s.db.QueryContext(ctx, querySelect, now)
	if err != nil {
		return 0, fmt.Errorf("failed to query expired jobs: %w", err)
	}
	defer rows.Close()

	type expiredJob struct {
		id         string
		jobType    string
		retries    int
		maxRetries int
	}

	var expired []expiredJob
	for rows.Next() {
		var ej expiredJob
		if err := rows.Scan(&ej.id, &ej.jobType, &ej.retries, &ej.maxRetries); err != nil {
			return 0, err
		}
		expired = append(expired, ej)
	}
	rows.Close()

	count := 0
	for _, ej := range expired {
		newRetries := ej.retries + 1
		var newState string
		var nextRunAt interface{}

		if newRetries > ej.maxRetries {
			newState = string(StateDeadLetter)
			nextRunAt = time.Time{}
		} else {
			newState = string(StateFailed)
			nextRunAt = now.Add(5 * time.Second)
		}

		queryUpdate := `
			UPDATE jobs
			SET state = $1, retries = $2, last_error = 'lease expired: worker heartbeat timeout', reserved_until = NULL, run_at = $3, updated_at = $4
			WHERE id = $5 AND state = 'processing' AND reserved_until < $6
		`
		result, err := s.db.ExecContext(ctx, queryUpdate, newState, newRetries, nextRunAt, now, ej.id, now)
		if err != nil {
			continue
		}

		affected, err := result.RowsAffected()
		if err == nil && affected > 0 {
			count++
			if newRetries > ej.maxRetries {
				JobsProcessed.WithLabelValues(ej.jobType, string(StateDeadLetter)).Inc()
			} else {
				JobsRetried.WithLabelValues(ej.jobType).Inc()
			}
		}
	}

	return count, nil
}
