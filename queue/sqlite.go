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
	// Use modernc.org/sqlite driver ("sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Configure database for high concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	// Create tables
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		queue TEXT NOT NULL DEFAULT 'default',
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
	CREATE INDEX IF NOT EXISTS idx_jobs_queue_state_run_at ON jobs (queue, state, run_at);
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

// Enqueue adds a job to the SQLite store.
func (s *SQLiteStore) Enqueue(ctx context.Context, job *Job) error {
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

	var traceJSON []byte
	if job.TraceContext != nil {
		var err error
		traceJSON, err = json.Marshal(job.TraceContext)
		if err != nil {
			return fmt.Errorf("failed to marshal trace context: %w", err)
		}
	}

	query := `
	INSERT INTO jobs (id, queue, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	var reservedUntil interface{}
	if !job.ReservedUntil.IsZero() {
		reservedUntil = job.ReservedUntil
	}

	_, err := s.db.ExecContext(ctx, query,
		job.ID,
		job.Queue,
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

// Dequeue fetches the next available job, updates its state to 'processing', and locks it.
func (s *SQLiteStore) Dequeue(ctx context.Context, queues []string, types []string, leaseDuration time.Duration) (*Job, error) {
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
		SELECT id, queue, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context
		FROM jobs
		WHERE %s
		ORDER BY run_at ASC, created_at ASC
		LIMIT 1
	`, strings.Join(whereParts, " AND "))

	// Loop to handle optimistic locking retries if concurrent workers conflict
	for i := 0; i < 5; i++ {
		// Use a transaction to ensure atomic read & update
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}

		var j Job
		var reservedUntilStr sql.NullTime
		var lastErrStr sql.NullString
		var traceContextStr sql.NullString

		err = tx.QueryRowContext(ctx, selectQuery, args...).Scan(
			&j.ID,
			&j.Queue,
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
			tx.Rollback()
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil // No jobs available
			}
			return nil, fmt.Errorf("failed to query job: %w", err)
		}

		if reservedUntilStr.Valid {
			j.ReservedUntil = reservedUntilStr.Time
		}
		if lastErrStr.Valid {
			j.LastError = lastErrStr.String
		}
		if traceContextStr.Valid && traceContextStr.String != "" {
			if err := json.Unmarshal([]byte(traceContextStr.String), &j.TraceContext); err != nil {
				// Ignore parse errors for telemetry context, just log or skip
				j.TraceContext = nil
			}
		}

		// Try to claim this job
		newReservedUntil := now.Add(leaseDuration)
		updateQuery := `
			UPDATE jobs
			SET state = 'processing', reserved_until = ?, updated_at = ?
			WHERE id = ? AND (state = 'pending' OR state = 'failed')
		`
		result, err := tx.ExecContext(ctx, updateQuery, newReservedUntil, now, j.ID)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to update job state: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to check rows affected: %w", err)
		}

		if rowsAffected == 1 {
			// Successfully claimed! Commit transaction.
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("failed to commit transaction: %w", err)
			}
			j.State = StateProcessing
			j.ReservedUntil = newReservedUntil
			j.UpdatedAt = now
			return &j, nil
		}

		// Row was claimed by someone else in the split second. Rollback and try again.
		tx.Rollback()
		time.Sleep(10 * time.Millisecond)
	}

	return nil, nil // Return nil if we couldn't resolve after retries
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

	// Fetch type to update metrics
	var jobType string
	err = s.db.QueryRowContext(ctx, "SELECT type FROM jobs WHERE id = ?", jobID).Scan(&jobType)
	if err == nil {
		JobsProcessed.WithLabelValues(jobType, string(StateCompleted)).Inc()
	}

	return nil
}

// Nack handles failures. Increments retries and transitions to dead_letter if max_retries is reached.
func (s *SQLiteStore) Nack(ctx context.Context, jobID string, nextRunIn time.Duration, lastErr error) error {
	now := time.Now()

	// Select current retries and max_retries
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

	if newRetries >= maxRetries {
		newState = string(StateDeadLetter)
		nextRunAt = time.Time{} // Dead lettered jobs don't run automatically
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

	if newRetries >= maxRetries {
		JobsProcessed.WithLabelValues(jobType, string(StateDeadLetter)).Inc()
	} else {
		JobsRetried.WithLabelValues(jobType).Inc()
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
		SELECT id, queue, type, payload, state, retries, max_retries, run_at, reserved_until, last_error, created_at, updated_at, trace_context
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
		var lastErrStr sql.NullString
		var traceContextStr sql.NullString

		err := rows.Scan(
			&j.ID,
			&j.Queue,
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
		// Redrive all dead-lettered jobs
		query = `
			UPDATE jobs
			SET state = 'pending', retries = 0, last_error = NULL, reserved_until = NULL, run_at = ?, updated_at = ?
			WHERE state = 'dead_letter'
		`
	} else {
		// Redrive specific job IDs
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

// SweeperReleaseExpired releases jobs locked in the 'processing' state where ReservedUntil is past.
func (s *SQLiteStore) SweeperReleaseExpired(ctx context.Context) (int, error) {
	now := time.Now()

	// Select jobs that have expired leases
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

		if newRetries >= ej.maxRetries {
			newState = string(StateDeadLetter)
			nextRunAt = time.Time{}
		} else {
			newState = string(StateFailed)
			nextRunAt = now.Add(5 * time.Second) // retry in 5 seconds
		}

		queryUpdate := `
			UPDATE jobs
			SET state = ?, retries = ?, last_error = 'lease expired: worker heartbeat timeout', reserved_until = NULL, run_at = ?, updated_at = ?
			WHERE id = ? AND state = 'processing' AND reserved_until < ?
		`
		result, err := s.db.ExecContext(ctx, queryUpdate, newState, newRetries, nextRunAt, now, ej.id, now)
		if err != nil {
			continue // skip and let next sweep try
		}

		affected, err := result.RowsAffected()
		if err == nil && affected > 0 {
			count++
			if newRetries >= ej.maxRetries {
				JobsProcessed.WithLabelValues(ej.jobType, string(StateDeadLetter)).Inc()
			} else {
				JobsRetried.WithLabelValues(ej.jobType).Inc()
			}
		}
	}

	return count, nil
}
