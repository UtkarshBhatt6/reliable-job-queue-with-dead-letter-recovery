# Reliable Job Queue with Dead Letter Recovery

A production-grade, transactionally resilient, concurrent job queue engine written in Go. The system features automatic retries with exponential backoff, visibility timeout lease management, a CGO-free SQLite storage backend, OpenTelemetry tracing, Prometheus metrics, a Grafana performance dashboard, and a sleek web-based management UI.

## Features

- **Double-Safe Reliability**:
  - **In-Memory Store**: Fast, concurrent-safe store using Go RWMutex and memory lists, ideal for tests.
  - **SQLite Store**: Fully persistent, transaction-safe, WAL-enabled SQL store using CGO-free SQLite, ensuring jobs survive application restarts without extra database infrastructure.
- **Lease-Based Visibility**: When a worker claims a job, it's locked for a configurable visibility timeout. If a worker crashes or hangs, a background sweeper automatically releases the lease for retry.
- **Dead Letter Queue (DLQ)**: Failed jobs automatically retry. After reaching `MaxRetries`, they transition to the Dead Letter state where they are quarantined.
- **DLQ Redrive**: Admin control to bulk-redrive (replay) failed jobs back to the pending queue or purge them via API/Web UI.
- **Telemetry & Monitoring**:
  - **OpenTelemetry Tracing**: Full context propagation. Enqueueing a job injects the publisher's span context, which is extracted by the worker to form a continuous tracing chain.
  - **Prometheus Metrics**: Exposes queue depth, latency, retries, and processing throughput metrics via a `/metrics` scraper endpoint.
  - **Grafana Dashboard**: Ready-to-import Grafana dashboard configuration (`telemetry/grafana_dashboard.json`).
- **Interactive Web Dashboard**: Beautiful, real-time UI built with glassmorphism aesthetics, utilizing Server-Sent Events (SSE) for zero-latency queue synchronization.

---

## Architecture & Code Map

The project is structured cleanly into modules:

```
reliable-job-queue-with-dead-letter-recovery/
├── main.go                       # Demo application & entry point
├── go.mod                        # Go dependencies configuration
├── queue/                        # Reliable queue core package
│   ├── queue.go                  # Main interfaces, Job models, and State transitions
│   ├── memory.go                 # Concurrently safe In-Memory storage implementation
│   ├── sqlite.go                 # Transactional, WAL-mode SQLite storage engine
│   ├── worker.go                 # Worker Pool, backoffs, lease sweeper, and graceful shutdown
│   ├── telemetry.go              # Prometheus registries & OTel propagation helpers
│   └── queue_test.go             # Comprehensive unit test suite
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

- [Job](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/queue.go#L23): Struct modeling the job record (ID, Type, State, Payload, Retries, Lease info, Trace context).
- [Store Interface](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/queue.go#L44): Abstract CRUD layer enabling drop-in database migrations (supported by [MemoryStore](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/memory.go#L11) and [SQLiteStore](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/sqlite.go#L14)).
- [WorkerPool](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/worker.go#L15): Poller routine executing concurrency pools, tracing, and exponential retry scheduling.
- [Telemetry Helpers](file:///Users/fbin-blr-0025/finbox-work/reliable-job-queue-with-dead-letter-recovery/queue/telemetry.go): Standardizing counters, gauges, histograms, and Context carrier propagators.

---

## Quick Start

### 1. Prerequisite & Dependencies
Tidy up Go modules:
```bash
go mod tidy
```

### 2. Run Tests
Verify both backend engines and retry managers by running the unit tests:
```bash
go test -v ./queue
```

### 3. Launch the Demo
Start the main Go application:
```bash
go run main.go
```

Once running:
- **Web Dashboard**: Open [http://localhost:8080](http://localhost:8080) to access the real-time UI.
- **Prometheus Metrics**: Scrape metrics at [http://localhost:8080/metrics](http://localhost:8080/metrics).
- **Graceful Shutdown**: Press `Ctrl+C` to watch the workers finish current jobs and close database buffers safely before exiting.

---

## UI Guide

The Web UI allows you to test all reliability scenarios visually:
1. **Successful Executions**: Choose `send_email`, type `hello@domain.com`, and click "Enqueue". It executes and instantly appears in **Completed**.
2. **Delayed Executions**: Enter a "Delay (seconds)" value of `10`. The job will sit in **Pending** as scheduled, and run precisely after 10 seconds.
3. **Dead-Lettering**: Check the **Force failure** checkbox and submit a job. You will watch it fail, retry with exponential backoff (state: **Failed (Retrying)**), and finally quarantine into the **Dead Letter (DLQ)** panel.
4. **Replays (Redrive)**: Open the **Dead Letter (DLQ)** tab, inspect the job's last failure message, and click **Redrive** to reset retries and re-run.

---

## Telemetry Metrics Exporter

The following Prometheus metrics are exported for Grafana dashboards:

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `reliable_queue_depth` | Gauge | `state` | Number of jobs in pending, processing, failed, or dead_letter states. |
| `reliable_queue_jobs_enqueued_total` | Counter | `type` | Total count of enqueued jobs. |
| `reliable_queue_jobs_processed_total` | Counter | `type`, `status` | Total count of processed jobs (completed, failed, dead_letter). |
| `reliable_queue_jobs_retried_total` | Counter | `type` | Number of transient job retries. |
| `reliable_queue_jobs_processing_duration_seconds` | Histogram | `type` | Bucketed processing latencies. |
