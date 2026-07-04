package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	"reliable-job-queue/dashboard"
	"reliable-job-queue/queue"
)

// initTracer initializes an OpenTelemetry TracerProvider.
func initTracer() (*sdktrace.TracerProvider, error) {
	// For local demo, we use a basic TracerProvider.
	// You can add exporters like Jaeger, Zipkin, or OTLP collector here.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("reliable-queue-demo"),
		)),
	)
	otel.SetTracerProvider(tp)

	// Register the W3C Trace Context propagator globally.
	// This is critical for trace context propagation across queue boundaries.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp, nil
}

type jobPayload struct {
	Data      string `json:"data"`
	ForceFail bool   `json:"force_fail"`
}

func main() {
	log.Println("[App] Starting Reliable Job Queue Service...")

	// Initialize OpenTelemetry Tracing
	tp, err := initTracer()
	if err != nil {
		log.Fatalf("failed to initialize tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	// Initialize pluggable storage engine
	var store queue.Store
	dsn := os.Getenv("DATABASE_URL")
	if dsn != "" {
		log.Println("[App] Connecting to pluggable PostgreSQL backend store...")
		store, err = queue.NewPostgresStore(dsn)
		if err != nil {
			log.Fatalf("Failed to initialize PostgreSQL store: %v", err)
		}
		log.Println("[App] PostgreSQL backend store successfully connected")
	} else {
		dbPath := "./jobs.db"
		log.Printf("[App] Connecting to default SQLite backend store at %s...", dbPath)
		store, err = queue.NewSQLiteStore(dbPath)
		if err != nil {
			log.Fatalf("Failed to initialize SQLite store: %v", err)
		}
		log.Printf("[App] SQLite backend store successfully initialized")
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			closer.Close()
		}
	}()

	// Advanced Retry Policy with Jitter
	retryPolicy := queue.RetryPolicy{
		BaseDelay:  1 * time.Second,
		MaxDelay:   30 * time.Second,
		Multiplier: 2.0,
		Jitter:     true,
	}

	// Setup Server-Sent Events HTTP dashboard server
	dashboardAddr := ":8080"
	server := dashboard.NewServer(store, dashboardAddr)

	// Create Worker Pool with concurrency of 3
	pool := queue.NewWorkerPool(
		store,
		3,
		queue.WithPollInterval(200*time.Millisecond),
		queue.WithLeaseDuration(15*time.Second),
		queue.WithSweeperInterval(3*time.Second),
		queue.WithQueues("critical", "high", "default", "low"),
		queue.WithRetryPolicy(retryPolicy),
		queue.WithStateChangeCallback(func() {
			server.NotifyChange()
		}),
	)

	// Handler wrappers that parse payload and print nice console outputs
	wrapHandler := func(handlerName string, simulateWork func(ctx context.Context, data string) error) queue.Handler {
		return func(ctx context.Context, j *queue.Job) error {
			// Extract trace ID from span inside context
			span := trace.SpanFromContext(ctx)
			traceID := span.SpanContext().TraceID().String()

			log.Printf("[Worker] ==> Processing Job %s [Queue: %s, Type: %s, Retry: %d/%d, TraceID: %s]",
				j.ID, j.Queue, j.Type, j.Retries, j.MaxRetries, traceID)

			var payload jobPayload
			if err := json.Unmarshal(j.Payload, &payload); err != nil {
				return fmt.Errorf("failed to parse job payload: %w", err)
			}

			// Simulate forced errors for demonstrating dead letter queues
			if payload.ForceFail {
				time.Sleep(500 * time.Millisecond)
				return fmt.Errorf("simulated handler execution failure for test purposes")
			}

			// Execute actual worker logic
			err := simulateWork(ctx, payload.Data)
			return err
		}
	}

	// Register workers handlers
	pool.Register("send_email", wrapHandler("send_email", func(ctx context.Context, data string) error {
		// Simulate network call
		time.Sleep(1 * time.Second)
		log.Printf("[Worker] 📧 Email sent to: %s", data)
		return nil
	}))

	pool.Register("process_payment", wrapHandler("process_payment", func(ctx context.Context, data string) error {
		// Simulate payment gateway delay
		time.Sleep(1500 * time.Millisecond)
		log.Printf("[Worker] 💳 Payment processed for data: %s", data)
		return nil
	}))

	pool.Register("generate_report", wrapHandler("generate_report", func(ctx context.Context, data string) error {
		// Simulate computation time
		time.Sleep(2 * time.Second)
		log.Printf("[Worker] 📊 Report generated for client: %s", data)
		return nil
	}))

	pool.Register("sync_crm", wrapHandler("sync_crm", func(ctx context.Context, data string) error {
		// Simulate external API call
		time.Sleep(800 * time.Millisecond)
		log.Printf("[Worker] 🔄 CRM sync finished: %s", data)
		return nil
	}))

	// Start Worker Pool
	pool.Start()

	// Start web server in a goroutine
	go server.Start()

	// Wait for OS interrupt signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Print interactive logs
	fmt.Printf("\n=======================================================\n")
	fmt.Printf("🚀 Reliable Job Queue engine is running!\n")
	fmt.Printf("🖥️  Access Dashboard UI: http://localhost%s\n", dashboardAddr)
	fmt.Printf("📊 Access Prometheus metrics: http://localhost%s/metrics\n", dashboardAddr)
	fmt.Printf("💡 Enqueue jobs using the web dashboard to see real-time updates!\n")
	fmt.Printf("=======================================================\n\n")

	<-sigChan

	fmt.Println("\n[App] Shutdown signal received. Cleaning up...")

	// 1. Stop processing new jobs and wait for active workers to drain
	pool.Stop()

	// 2. Close db
	log.Println("[App] Closing database and exiting.")
}
