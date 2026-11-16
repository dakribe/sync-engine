package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dakribe/sync-engine/wire"
)

// MaxBodyBytes caps the size of a request body.
const MaxBodyBytes = 8 << 20

// NewHandler serves the sync API:
//
//	POST /push   wire.PushRequest  -> wire.PushResponse
//	POST /pull   wire.PullRequest  -> wire.PullResponse
//	GET  /events  a stream of server-sent events, see events
//	GET  /healthz
//
// Errors are JSON objects with an "error" field.
func NewHandler(e *Engine, logger *slog.Logger) http.Handler {
	h := &handler{engine: e, log: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /push", h.push)
	mux.HandleFunc("POST /pull", h.pull)
	mux.HandleFunc("GET /events", h.events)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type handler struct {
	engine *Engine
	log    *slog.Logger
}

func (h *handler) push(w http.ResponseWriter, r *http.Request) {
	var req wire.PushRequest
	if !h.decode(w, r, &req) {
		return
	}
	resp, err := h.engine.Push(r.Context(), req)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *handler) pull(w http.ResponseWriter, r *http.Request) {
	var req wire.PullRequest
	if !h.decode(w, r, &req) {
		return
	}
	resp, err := h.engine.Pull(r.Context(), req)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HeartbeatInterval is how often an idle event stream sends a comment, so proxies and
// clients can tell a quiet connection from a dead one.
var HeartbeatInterval = 15 * time.Second

// events streams a "poke" event with the current version as soon as a client connects,
// then another each time data changes:
//
//	event: poke
//	data: {"version":42}
//
// A poke says only that there's something new; the client pulls to get it. Pokes are
// small, and a client that's behind skips straight to the latest one.
func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	ch, stop := h.engine.Subscribe()
	defer stop()

	version, err := h.engine.Version(r.Context())
	if err != nil {
		h.fail(w, r, err)
		return
	}

	rc := http.NewResponseController(w)
	rc.SetWriteDeadline(time.Time{}) // the stream outlives the server's write timeout
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // stop nginx from buffering the stream
	w.WriteHeader(http.StatusOK)

	send := func(format string, args ...any) bool {
		if _, err := fmt.Fprintf(w, format, args...); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send("event: poke\ndata: {\"version\":%d}\n\n", version) {
		return
	}

	heartbeat := time.NewTicker(HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case v, ok := <-ch:
			if !ok || !send("event: poke\ndata: {\"version\":%d}\n\n", v) {
				return
			}
		case <-heartbeat.C:
			if !send(": ping\n\n") {
				return
			}
		}
	}
}

func (h *handler) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		if _, tooBig := errors.AsType[*http.MaxBytesError](err); tooBig {
			writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		} else {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return false
	}
	return true
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrNoClientID):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.Canceled):
		// The client went away; there's no one to answer.
	default:
		h.log.Error("request failed", "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
