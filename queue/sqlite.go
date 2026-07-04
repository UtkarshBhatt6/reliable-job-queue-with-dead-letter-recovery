package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore creates and initializes a new SQLite-backed job store.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Configure database WAL mode for high concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	// Create tables with priority and deduplication fields
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		queue TEXT NOT NULL DEFAULT 'default',
		priority INTEGER NOT NULL DEFAULT 0,
		deduplication_key TEXT,
		deduplication_expires_at DATETIME,
		type TEXT NOT NULL,
		payload BLOB NOT NULL,
		state TEXT NOT NULL,
		retries INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 3,
		run_at DATETIME NOT NULL,
		reserved_until DATETIME,
		last_error TEXT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		trace_context TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_priority_run_at ON jobs (queue, state, priority DESC, run_at);
	CREATE INDEX IF NOT EXISTS idx_jobs_dedup_key ON jobs (deduplication_key);
	CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs (type);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	store := &SQLiteStore{db: db}

	// Update queue depth metrics in the background
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			store.updateMetrics()
		}
	}()

	return store, nil
}

func (s *SQLiteStore) updateMetrics() {
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
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

// checkDeduplicationLocked checks if an active duplicate job exists.
func (s *SQLiteStore) checkDeduplication(ctx context.Context, tx *sql.Tx, job *Job, now time.Time) (bool, error) {
	if job.DeduplicationKey == "" {
		return false, nil
	}

	query := `
		SELECT COUNT(*) FROM jobs
		WHERE deduplication_key = ?
		  AND (state != 'completed' OR deduplication_expires_at > ?)
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

// Enqueue adds a job to the SQLite store.
func (s *SQLiteStore) Enqueue(ctx context.Context, job *Job) error {
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

// EnqueueBatch adds multiple jobs to the store atomically.
func (s *SQLiteStore) EnqueueBatch(ctx context.Context, jobs []*Job) error {
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

func (s *SQLiteStore) enqueueTx(ctx context.Context, tx *sql.Tx, job *Job, now time.Time) error {
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
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		return fmt.Errorf("failed to insert job inside tx: %w", err)
	}

	JobsEnqueued.WithLabelValues(job.Type).Inc()
	return nil
}

// Dequeue fetches the next available job.
func (s *SQLiteStore) Dequeue(ctx context.Context, queues []string, types []string, leaseDuration time.Duration) (*Job, error) {
	jobs, err := s.DequeueBatch(ctx, queues, types, 1, leaseDuration)
	if err != nil || len(jobs) == 0 {
		return nil, err
	}
	return jobs[0], nil
}

// DequeueBatch fetches up to batchSize next available jobs.
func (s *SQLiteStore) DequeueBatch(ctx context.Context, queues []string, types []string, batchSize int, leaseDuration time.Duration) ([]*Job, error) {
	if batchSize <= 0 {
		return []*Job{}, nil
	}

	now := time.Now()
	var whereParts []string
	var args []interface{}
	args = append(args, now)

	whereParts = append(whereParts, "(state = 'pending' OR state = 'failed')")
	whereParts = append(whereParts, "run_at <= ?")

	if len(queues) > 0 {
		placeholders := make([]string, len(queues))
		for i, q := range queues {
			placeholders[i] = "?"
			args = append(args, q)
		}
		whereParts = append(whereParts, fmt.Sprintf("queue IN (%s)", strings.Join(placeholders, ",")))
	}

	if len(types) > 0 {
		placeholders := make([]string, len(types))
		for i, t := range types {
			placeholders[i] = "?"
			args = append(args, t)
		}
		whereParts = append(whereParts, fmt.Sprintf("type IN (%s)", strings.Join(placeholders, ",")))
	}

	selectQuery := fmt.Sprintf(`
		SELECT id, queue, priority, deduplication_key, deduplication_expires_at, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context
		FROM jobs
		WHERE %s
		ORDER BY priority DESC, run_at ASC, created_at ASC
		LIMIT ?
	`, strings.Join(whereParts, " AND "))
	
	args = append(args, batchSize)

	// Optimistic locking claim loop
	for attempt := 0; attempt < 5; attempt++ {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to start dequeue batch transaction: %w", err)
		}

		rows, err := tx.QueryContext(ctx, selectQuery, args...)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to query jobs in batch: %w", err)
		}

		var candidates []*Job
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
				rows.Close()
				tx.Rollback()
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

			candidates = append(candidates, &j)
		}
		rows.Close()

		if len(candidates) == 0 {
			tx.Rollback()
			return []*Job{}, nil
		}

		// Try to claim all candidates
		claimed := make([]*Job, 0, len(candidates))
		newReservedUntil := now.Add(leaseDuration)

		for _, j := range candidates {
			updateQuery := `
				UPDATE jobs
				SET state = 'processing', reserved_until = ?, updated_at = ?
				WHERE id = ? AND (state = 'pending' OR state = 'failed')
			`
			result, err := tx.ExecContext(ctx, updateQuery, newReservedUntil, now, j.ID)
			if err != nil {
				break
			}
			affected, err := result.RowsAffected()
			if err == nil && affected == 1 {
				j.State = StateProcessing
				j.ReservedUntil = newReservedUntil
				j.UpdatedAt = now
				claimed = append(claimed, j)
			}
		}

		if len(claimed) > 0 {
			if err := tx.Commit(); err != nil {
				tx.Rollback()
				continue
			}
			return claimed, nil
		}

		tx.Rollback()
		time.Sleep(10 * time.Millisecond)
	}

	return []*Job{}, nil
}

// Ack marks the job as completed successfully.
func (s *SQLiteStore) Ack(ctx context.Context, jobID string) error {
	now := time.Now()
	query := `
		UPDATE jobs
		SET state = 'completed', reserved_until = NULL, updated_at = ?
		WHERE id = ? AND state = 'processing'
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
	err = s.db.QueryRowContext(ctx, "SELECT type FROM jobs WHERE id = ?", jobID).Scan(&jobType)
	if err == nil {
		JobsProcessed.WithLabelValues(jobType, string(StateCompleted)).Inc()
	}

	return nil
}

// Nack handles failures. Increments retries and transitions to DLQ if max_retries reached.
func (s *SQLiteStore) Nack(ctx context.Context, jobID string, nextRunIn time.Duration, lastErr error) error {
	now := time.Now()

	var retries, maxRetries int
	var jobType string
	querySelect := "SELECT retries, max_retries, type FROM jobs WHERE id = ? AND state = 'processing'"
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
		SET state = ?, retries = ?, last_error = ?, reserved_until = NULL, run_at = ?, updated_at = ?
		WHERE id = ? AND state = 'processing'
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
func (s *SQLiteStore) Heartbeat(ctx context.Context, jobID string, extendBy time.Duration) error {
	now := time.Now()
	newReservedUntil := now.Add(extendBy)

	query := `
		UPDATE jobs
		SET reserved_until = ?, updated_at = ?
		WHERE id = ? AND state = 'processing'
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
func (s *SQLiteStore) GetStats(ctx context.Context) (Stats, error) {
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
func (s *SQLiteStore) ListJobs(ctx context.Context, state JobState, limit, offset int) ([]*Job, error) {
	query := `
		SELECT id, queue, priority, deduplication_key, deduplication_expires_at, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context
		FROM jobs
		WHERE state = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
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
func (s *SQLiteStore) RedriveDeadLetter(ctx context.Context, jobIDs []string) (int, error) {
	now := time.Now()
	var query string
	var args []interface{}
	args = append(args, now, now)

	if len(jobIDs) == 0 {
		query = `
			UPDATE jobs
			SET state = 'pending', retries = 0, last_error = NULL, reserved_until = NULL, run_at = ?, updated_at = ?
			WHERE state = 'dead_letter'
		`
	} else {
		placeholders := make([]string, len(jobIDs))
		for i, id := range jobIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		query = fmt.Sprintf(`
			UPDATE jobs
			SET state = 'pending', retries = 0, last_error = NULL, reserved_until = NULL, run_at = ?, updated_at = ?
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
func (s *SQLiteStore) DeleteDeadLetter(ctx context.Context, jobIDs []string) error {
	var query string
	var args []interface{}

	if len(jobIDs) == 0 {
		query = "DELETE FROM jobs WHERE state = 'dead_letter'"
	} else {
		placeholders := make([]string, len(jobIDs))
		for i, id := range jobIDs {
			placeholders[i] = "?"
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
func (s *SQLiteStore) SweeperReleaseExpired(ctx context.Context) (int, error) {
	now := time.Now()

	querySelect := `
		SELECT id, type, retries, max_retries
		FROM jobs
		WHERE state = 'processing' AND reserved_until < ?
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
			SET state = ?, retries = ?, last_error = 'lease expired: worker heartbeat timeout', reserved_until = NULL, run_at = ?, updated_at = ?
			WHERE id = ? AND state = 'processing' AND reserved_until < ?
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
