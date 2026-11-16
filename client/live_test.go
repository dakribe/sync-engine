package client

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dakribe/sync-engine/server"
)

// TestLiveOverHTTP runs two clients against a real HTTP server, with polling effectively
// off, so a change can only reach the other client through the event stream.
func TestLiveOverHTTP(t *testing.T) {
	e := newEngine()
	srv := httptest.NewServer(server.NewHandler(e, slog.New(slog.DiscardHandler)))
	defer srv.Close()
	defer e.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	opts := Options{PollInterval: time.Hour}
	a := New(&HTTPTransport{BaseURL: srv.URL}, opts)
	b := New(&HTTPTransport{BaseURL: srv.URL}, opts)
	go a.Run(ctx)
	go b.Run(ctx)

	changes, stop := b.Subscribe()
	defer stop()

	start := time.Now()
	mustPut(t, a, "todos", "1", map[string]any{"title": "milk"})
	deadline := time.After(5 * time.Second)
	for {
		if _, ok := b.Get("todos", "1"); ok {
			break
		}
		select {
		case <-changes:
		case <-deadline:
			t.Fatal("change didn't reach b over the event stream")
		}
	}
	t.Logf("a's write reached b in %v", time.Since(start))
}
