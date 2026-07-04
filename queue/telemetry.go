package queue

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// MapCarrier implements propagation.TextMapCarrier for map[string]string.
type MapCarrier map[string]string

func (m MapCarrier) Get(key string) string {
	return m[key]
}

func (m MapCarrier) Set(key, value string) {
	m[key] = value
}

func (m MapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Global Prometheus metrics for the job queue.
var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reliable_queue_jobs_enqueued_total",
		Help: "Total number of jobs enqueued.",
	}, []string{"type"})

	JobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reliable_queue_jobs_processed_total",
		Help: "Total number of jobs processed.",
	}, []string{"type", "status"}) // status can be "completed", "failed", or "dead_letter"

	JobsProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "reliable_queue_jobs_processing_duration_seconds",
		Help:    "Duration of job processing in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "reliable_queue_depth",
		Help: "Current depth of the job queue.",
	}, []string{"state"}) // state: "pending", "processing", "completed", "failed", "dead_letter"

	JobsRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reliable_queue_jobs_retried_total",
		Help: "Total number of times jobs have been retried.",
	}, []string{"type"})
)

// InjectTraceContext injects the current OpenTelemetry span context into the job's trace context map.
func InjectTraceContext(ctx context.Context, job *Job) {
	if job.TraceContext == nil {
		job.TraceContext = make(map[string]string)
	}
	otel.GetTextMapPropagator().Inject(ctx, MapCarrier(job.TraceContext))
}

// ExtractTraceContext extracts the OpenTelemetry span context from the job's trace context map.
func ExtractTraceContext(ctx context.Context, job *Job) context.Context {
	if job.TraceContext == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, MapCarrier(job.TraceContext))
}

// Tracer returns the Tracer instance for the job queue.
func Tracer() trace.Tracer {
	return otel.Tracer("reliable-job-queue")
}
