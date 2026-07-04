# Reliable Job Queue with Dead Letter Recovery

A production-grade, transactionally resilient, concurrent job queue engine written in Go. The system features automatic retries with exponential backoff, visibility timeout lease management, pluggable SQLite/PostgreSQL storage backends, OpenTelemetry tracing, Prometheus metrics, a Grafana performance dashboard, and a sleek web-based management UI.

## Features

- **Named & Priority Queues**: Organizes jobs into prioritized pipelines (`critical`, `high`, `default`, `low`). Workers pick high-priority tasks first (ordered by `Priority DESC`, `RunAt ASC`).
- **Idempotency Keys & Deduplication**: Enqueue jobs with a unique deduplication key. Duplicate jobs within their TTL window are safely ignored, preventing accidental duplicate executions.
- **Advanced Retry Policies with Jitter**: Configurable `RetryPolicy` with base/max delays, multiplier, and randomized jitter to prevent the "thundering herd" problem.
- **Batch Enqueue/Fetch**: Atomically enqueue (`EnqueueBatch`) and dequeue (`DequeueBatch`) multiple jobs in a single database round-trip.
- **Heartbeats**: Handlers can extend job visibility timeout leases dynamically for long-running tasks using a context-based heartbeat (`queue.Heartbeat(ctx, extendBy)`).
- **Pluggable Storage**: Drop-in storage backends with a clean `Store` interface:
  - **In-Memory Store**: Thread-safe store, ideal for testing.
  - **SQLite Store**: WAL-mode database backend with optimistic locking, perfect for single-instance apps.
  - **PostgreSQL Store**: Concurrent storage engine leveraging PG's atomic `FOR UPDATE SKIP LOCKED` for thread-safe dequeue claims.
- **Telemetry & Monitoring**:
  - **OpenTelemetry Tracing**: Context propagation across publisher/consumer boundaries.
  - **Prometheus Metrics**: Exposes queue depth, latency, retries, and processing throughput metrics via `/metrics`.
  - **Grafana Dashboard**: Importable Grafana dashboard config (`telemetry/grafana_dashboard.json`).
- **Interactive Web Dashboard**: Beautiful, real-time UI built with glassmorphism aesthetics, utilizing Server-Sent Events (SSE).

---

## Architecture & Code Map

```
reliable-job-queue-with-dead-letter-recovery/
├── main.go                       # Demo application & entry point
├── go.mod                        # Go dependencies configuration
├── queue/                        # Reliable queue core package
│   ├── queue.go                  # Main interfaces, Job models, and State transitions
│   ├── memory.go                 # Concurrently safe In-Memory storage implementation
│   ├── sqlite.go                 # Transactional, WAL-mode SQLite storage engine
│   ├── postgres.go               # PostgreSQL database driver implementation
│   ├── retry.go                  # Advanced exponential retry with Jitter policy
│   ├── worker.go                 # Worker Pool, heartbeats, lease sweeper, and graceful shutdown
│   ├── telemetry.go              # Prometheus registries & OTel propagation helpers
│   └── queue_test.go             # Comprehensive unit test suite
├── benchmark/                    # Benchmark suite
│   └── benchmark.go              # Evaluates throughput (TPS) and average latency
├── chaos/                        # Chaos testing simulation
│   └── chaos.go                  # Force crashes workers mid-execution to verify recovery
├── dashboard/                    # Dashboard UI HTTP Server
│   ├── server.go                 # HTTP server APIs, prometheus scraper, SSE events
│   └── static/                   # Dashboard single-page application UI
│       ├── index.html            # UI layout (Outfit font, semantic components)
│       ├── app.css               # Rich glassmorphic dark mode styling
│       └── app.js                # SSE client & interactive API actions
└── telemetry/                    # Monitoring assets
    └── grafana_dashboard.json    # Grafana dashboard JSON layout model
```

### Key Components

- [Job](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/queue.go#L27): Models job records including `Priority`, `DeduplicationKey`, and `DeduplicationExpiresAt`.
- [Store](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/queue.go#L52): Abstract CRUD layer implemented by [MemoryStore](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/memory.go), [SQLiteStore](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/sqlite.go), and [PostgresStore](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/postgres.go).
- [WorkerPool](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/worker.go#L29): Coordinates threads, tracing, backoff, and heartbeat context injections.

---

## Quick Start

### 1. Run Tests
Verify both SQLite/Memory stores, queue isolation, batch claim, and heartbeats:
```bash
go test -v ./queue
```

### 2. Run Benchmarks
Evaluate throughput (jobs/sec) and processing latency:
```bash
go run benchmark/benchmark.go
```

### 3. Run Chaos Simulations
Validate automatic sweeper recovery after forced worker deaths:
```bash
go run chaos/chaos.go
```

### 4. Launch the Demo App
To run using default SQLite storage:
```bash
go run main.go
```

To run using pluggable PostgreSQL storage:
```bash
export DATABASE_URL="postgres://user:password@localhost:5432/dbname?sslmode=disable"
go run main.go
```

Once running:
- **Web Dashboard**: Access the UI at [http://localhost:8080](http://localhost:8080).
- **Prometheus Metrics**: Scrape metrics at [http://localhost:8080/metrics](http://localhost:8080/metrics).

### 5. Launch Grafana & Prometheus Dashboards
We provide a pre-configured Docker Compose stack to automatically collect Prometheus metrics and visualize them in a Grafana dashboard.

To start the monitoring stack:
```bash
docker compose up -d
```

This will spin up:
- **Prometheus**: Accessible at [http://localhost:9090](http://localhost:9090) (auto-configured to scrape the Go application metrics).
- **Grafana**: Accessible at [http://localhost:3000](http://localhost:3000) (pre-loaded with the Prometheus datasource and the **Reliable Job Queue Performance Dashboard**).
  - *Username*: `admin`
  - *Password*: `admin`

---

## UI Guide

The Web UI allows you to configure advanced enqueuing features:
1. **Priority & Queue Selection**: Choose target queue (`critical`, `high`, `default`, `low`) and set priority. High-priority jobs execute first.
2. **Idempotency Keys**: Set an optional deduplication key and TTL. Enqueuing multiple jobs with the same key will only trigger one active run.
3. **Dead-Lettering**: Check the **Force failure** box. Watch the job retry with exponential backoff (incorporating jitter) before transitioning to the **Dead Letter (DLQ)** panel.
4. **Replays (Redrive)**: Trigger bulk or individual redrives to reset retries and replay failed jobs.
