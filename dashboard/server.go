package dashboard

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"reliable-job-queue/queue"
)

// Broker handles Server-Sent Events broadcasting.
type Broker struct {
	clients    map[chan string]bool
	register   chan chan string
	unregister chan chan string
	broadcast  chan string
	mu         sync.Mutex
}

func NewBroker() *Broker {
	b := &Broker{
		clients:    make(map[chan string]bool),
		register:   make(chan chan string),
		unregister: make(chan chan string),
		broadcast:  make(chan string, 100),
	}
	go b.start()
	return b
}

func (b *Broker) start() {
	for {
		select {
		case c := <-b.register:
			b.mu.Lock()
			b.clients[c] = true
			b.mu.Unlock()
		case c := <-b.unregister:
			b.mu.Lock()
			if _, ok := b.clients[c]; ok {
				delete(b.clients, c)
				close(c)
			}
			b.mu.Unlock()
		case msg := <-b.broadcast:
			b.mu.Lock()
			for c := range b.clients {
				select {
				case c <- msg:
				default:
					// Skip if client is blocked or slow
				}
			}
			b.mu.Unlock()
		}
	}
}

func (b *Broker) Broadcast(event string) {
	b.broadcast <- event
}

type Server struct {
	store  queue.Store
	broker *Broker
	addr   string
}

func NewServer(store queue.Store, addr string) *Server {
	return &Server{
		store:  store,
		broker: NewBroker(),
		addr:   addr,
	}
}

// NotifyChange broadcasts a refresh signal to all connected dashboards.
func (s *Server) NotifyChange() {
	s.broker.Broadcast("refresh")
}

func (s *Server) Start() {
	mux := http.NewServeMux()

	// Serves the metrics for Prometheus scraping
	mux.Handle("/metrics", promhttp.Handler())

	// API Endpoints
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/jobs", s.handleJobs)
	mux.HandleFunc("/api/jobs/batch", s.handleJobsBatch)
	mux.HandleFunc("/api/jobs/redrive", s.handleRedrive)
	mux.HandleFunc("/api/jobs/delete", s.handleDelete)
	mux.HandleFunc("/api/events", s.handleEvents)

	// Static files (dashboard UI)
	staticPath := "./dashboard/static"
	if _, err := os.Stat(staticPath); os.IsNotExist(err) {
		log.Printf("Static files directory not found at %s. Creating stub files...", staticPath)
		// We will create the directory and files later in next steps.
	}
	mux.Handle("/", http.FileServer(http.Dir(staticPath)))

	log.Printf("[DashboardServer] Web dashboard UI and Prometheus metrics running on %s", s.addr)
	if err := http.ListenAndServe(s.addr, mux); err != nil {
		log.Fatalf("Dashboard server failed: %v", err)
	}
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

type enqueueRequest struct {
	Queue            string `json:"queue"`
	Priority         int    `json:"priority"`
	DeduplicationKey string `json:"deduplication_key"`
	DeduplicationTTL int    `json:"deduplication_ttl"`
	Type             string `json:"type"`
	Payload          string `json:"payload"`
	DelaySec         int    `json:"delay_sec"`
	MaxRetries       int    `json:"max_retries"`
	ForceFail        bool   `json:"force_fail"`
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		state := queue.JobState(r.URL.Query().Get("state"))
		if state == "" {
			state = queue.StatePending
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 50
		}
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

		jobs, err := s.store.ListJobs(r.Context(), state, limit, offset)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobs)

	case http.MethodPost:
		var req enqueueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Type == "" {
			http.Error(w, "job type is required", http.StatusBadRequest)
			return
		}

		if req.MaxRetries <= 0 {
			req.MaxRetries = 3
		}

		payloadMap := map[string]interface{}{
			"data":       req.Payload,
			"force_fail": req.ForceFail,
		}
		payloadBytes, err := json.Marshal(payloadMap)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		runAt := time.Now()
		if req.DelaySec > 0 {
			runAt = runAt.Add(time.Duration(req.DelaySec) * time.Second)
		}

		if req.Queue == "" {
			req.Queue = "default"
		}

		var dedupExpires time.Time
		if req.DeduplicationKey != "" && req.DeduplicationTTL > 0 {
			dedupExpires = time.Now().Add(time.Duration(req.DeduplicationTTL) * time.Second)
		}

		job := &queue.Job{
			ID:                     uuid.New().String(),
			Queue:                  req.Queue,
			Priority:               req.Priority,
			DeduplicationKey:       req.DeduplicationKey,
			DeduplicationExpiresAt: dedupExpires,
			Type:                   req.Type,
			Payload:                payloadBytes,
			MaxRetries:             req.MaxRetries,
			RunAt:                  runAt,
		}

		// Inject trace context from request context (if any) to demonstrate tracing propagation
		queue.InjectTraceContext(r.Context(), job)

		if err := s.store.Enqueue(r.Context(), job); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.NotifyChange()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(job)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type redriveRequest struct {
	JobIDs []string `json:"job_ids"`
}

func (s *Server) handleRedrive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req redriveRequest
	// Try parsing body, if empty it means redrive all
	_ = json.NewDecoder(r.Body).Decode(&req)

	count, err := s.store.RedriveDeadLetter(r.Context(), req.JobIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.NotifyChange()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"redriven_count": count,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req redriveRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	err := s.store.DeleteDeadLetter(r.Context(), req.JobIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.NotifyChange()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientChan := make(chan string, 10)
	s.broker.register <- clientChan

	defer func() {
		s.broker.unregister <- clientChan
	}()

	// Send initial ping/connection check
	fmt.Fprintf(w, "data: connected\n\n")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-clientChan:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (s *Server) handleJobsBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req []enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var jobs []*queue.Job
	for _, item := range req {
		if item.Type == "" {
			http.Error(w, "job type is required for all batch items", http.StatusBadRequest)
			return
		}
		if item.MaxRetries <= 0 {
			item.MaxRetries = 3
		}
		payloadMap := map[string]interface{}{
			"data":       item.Payload,
			"force_fail": item.ForceFail,
		}
		payloadBytes, err := json.Marshal(payloadMap)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		runAt := time.Now()
		if item.DelaySec > 0 {
			runAt = runAt.Add(time.Duration(item.DelaySec) * time.Second)
		}
		if item.Queue == "" {
			item.Queue = "default"
		}
		var dedupExpires time.Time
		if item.DeduplicationKey != "" && item.DeduplicationTTL > 0 {
			dedupExpires = time.Now().Add(time.Duration(item.DeduplicationTTL) * time.Second)
		}

		job := &queue.Job{
			ID:                     uuid.New().String(),
			Queue:                  item.Queue,
			Priority:               item.Priority,
			DeduplicationKey:       item.DeduplicationKey,
			DeduplicationExpiresAt: dedupExpires,
			Type:                   item.Type,
			Payload:                payloadBytes,
			MaxRetries:             item.MaxRetries,
			RunAt:                  runAt,
		}
		queue.InjectTraceContext(r.Context(), job)
		jobs = append(jobs, job)
	}

	if err := s.store.EnqueueBatch(r.Context(), jobs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.NotifyChange()
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status":"success"}`))
}

// Help create the static directory structure programmatically
func EnsureStaticFiles() error {
	dir := "./dashboard/static"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return nil
}
