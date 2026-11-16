package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/dakribe/sync-engine/wire"
)

// MaxBodyBytes caps the size of a request body.
const MaxBodyBytes = 8 << 20

// NewHandler serves the sync API:
//
//	POST /push   wire.PushRequest  -> wire.PushResponse
//	POST /pull   wire.PullRequest  -> wire.PullResponse
//	GET  /healthz
//
// Errors are JSON objects with an "error" field.
func NewHandler(e *Engine, logger *slog.Logger) http.Handler {
	h := &handler{engine: e, log: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /push", h.push)
	mux.HandleFunc("POST /pull", h.pull)
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
