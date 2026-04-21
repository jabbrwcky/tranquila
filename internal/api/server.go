package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jabbrwcky/tranquila/internal/state"
	internalsync "github.com/jabbrwcky/tranquila/internal/sync"
	"github.com/rs/zerolog/log"
)

// Config holds the dependencies required by the management API server.
type Config struct {
	Addr     string
	State    *state.Store
	Progress *internalsync.Progress
}

// Server is the HTTP management API server.
type Server struct {
	srv      *http.Server
	state    *state.Store
	progress *internalsync.Progress
}

// NewServer creates a Server and registers all routes. Call ListenAndServe to start.
func NewServer(cfg Config) *Server {
	s := &Server{state: cfg.State, progress: cfg.Progress}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/buckets", s.listBuckets)
	mux.HandleFunc("GET /api/v1/buckets/{name}", s.getBucket)
	mux.HandleFunc("GET /api/v1/sync", s.getSyncStatus)
	s.srv = &http.Server{Addr: cfg.Addr, Handler: mux}
	return s
}

func (s *Server) ListenAndServe() error {
	log.Info().Str("addr", s.srv.Addr).Msg("management API listening")
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// ── JSON response types ──────────────────────────────────────────────────────

type BucketStats struct {
	Total   int64 `json:"total"`
	Synced  int64 `json:"synced"`
	Pending int64 `json:"pending"`
	Failed  int64 `json:"failed"`
}

type BucketSyncProgress struct {
	StartedAt      time.Time `json:"started_at"`
	PendingAtStart int64     `json:"pending_at_start"`
	SyncedThisRun  int64     `json:"synced_this_run"`
	FailedThisRun  int64     `json:"failed_this_run"`
	RatePerSec     float64   `json:"rate_per_sec"`
	ETASeconds     *float64  `json:"eta_seconds,omitempty"`
}

type BucketStatus struct {
	Name          string              `json:"name"`
	LastCollected *time.Time          `json:"last_collected,omitempty"`
	Stats         BucketStats         `json:"stats"`
	SyncProgress  *BucketSyncProgress `json:"sync_progress,omitempty"`
}

type SyncStatus struct {
	Running   bool           `json:"running"`
	StartedAt *time.Time     `json:"started_at,omitempty"`
	Buckets   []BucketStatus `json:"buckets"`
}

type apiError struct {
	Error string `json:"error"`
}

// ── helpers ──────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) bucketStatus(ctx context.Context, name string, snap internalsync.ProgressSnapshot) (BucketStatus, error) {
	bs := BucketStatus{Name: name}

	ct, err := s.state.GetCollectionTime(ctx, name)
	if err != nil {
		return bs, err
	}
	if !ct.IsZero() {
		bs.LastCollected = &ct
	}

	st, err := s.state.BucketStats(ctx, name)
	if err != nil {
		return bs, err
	}
	bs.Stats = BucketStats{
		Total:   st.Total,
		Synced:  st.Synced,
		Pending: st.Pending,
		Failed:  st.Failed,
	}

	if bp, ok := snap.Buckets[name]; ok {
		elapsed := time.Since(bp.StartedAt).Seconds()
		var rate float64
		if elapsed > 0 && bp.Synced > 0 {
			rate = float64(bp.Synced) / elapsed
		}
		p := &BucketSyncProgress{
			StartedAt:      bp.StartedAt,
			PendingAtStart: bp.PendingAtStart,
			SyncedThisRun:  bp.Synced,
			FailedThisRun:  bp.Failed,
			RatePerSec:     rate,
		}
		if rate > 0 {
			remaining := float64(bp.PendingAtStart-bp.Synced-bp.Failed)
			if remaining > 0 {
				eta := remaining / rate
				p.ETASeconds = &eta
			}
		}
		bs.SyncProgress = p
	}

	return bs, nil
}

// ── handlers ─────────────────────────────────────────────────────────────────

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	names, err := s.state.ListBuckets(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}

	snap := s.progress.Snapshot()
	result := make([]BucketStatus, 0, len(names))
	for _, name := range names {
		bs, err := s.bucketStatus(ctx, name, snap)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		result = append(result, bs)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	ct, err := s.state.GetCollectionTime(ctx, name)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	if ct.IsZero() {
		writeJSON(w, http.StatusNotFound, apiError{Error: "bucket not found: " + name})
		return
	}

	snap := s.progress.Snapshot()
	bs, err := s.bucketStatus(ctx, name, snap)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, bs)
}

func (s *Server) getSyncStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	snap := s.progress.Snapshot()

	status := SyncStatus{
		Running: snap.Running,
		Buckets: make([]BucketStatus, 0, len(snap.Buckets)),
	}
	if !snap.StartedAt.IsZero() {
		status.StartedAt = &snap.StartedAt
	}

	for name := range snap.Buckets {
		bs, err := s.bucketStatus(ctx, name, snap)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, apiError{Error: err.Error()})
			return
		}
		status.Buckets = append(status.Buckets, bs)
	}
	writeJSON(w, http.StatusOK, status)
}
