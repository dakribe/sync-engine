package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/wire"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(NewHandler(NewEngine(memory.New()), slog.New(slog.DiscardHandler)))
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, srv *httptest.Server, path, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHTTPPushThenPull(t *testing.T) {
	srv := newTestServer(t)

	status, body := post(t, srv, "/push",
		`{"clientId":"a","mutations":[{"seq":1,"ops":[{"kind":"put","collection":"todos","id":"1","fields":{"title":"milk"}}]}]}`)
	if status != http.StatusOK {
		t.Fatalf("push: %d %s", status, body)
	}
	var push wire.PushResponse
	if err := json.Unmarshal([]byte(body), &push); err != nil || push.LastSeq != 1 {
		t.Fatalf("push response = %s, want lastSeq 1", body)
	}

	status, body = post(t, srv, "/pull", `{"clientId":"b","since":0}`)
	if status != http.StatusOK {
		t.Fatalf("pull: %d %s", status, body)
	}
	var pull wire.PullResponse
	if err := json.Unmarshal([]byte(body), &pull); err != nil {
		t.Fatalf("pull response: %v", err)
	}
	if pull.Version != 1 || len(pull.Records) != 1 || string(pull.Records[0].Fields["title"]) != `"milk"` {
		t.Fatalf("pull response = %s", body)
	}
}

func TestHTTPErrors(t *testing.T) {
	srv := newTestServer(t)

	tests := []struct {
		name, path, body string
		status           int
	}{
		{"bad json", "/push", `{`, http.StatusBadRequest},
		{"no client id", "/pull", `{"since":0}`, http.StatusBadRequest},
		{"too large", "/push", `{"clientId":"` + strings.Repeat("x", MaxBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
		{"unknown path", "/nope", `{}`, http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := post(t, srv, tt.path, tt.body)
			if status != tt.status {
				t.Fatalf("status = %d, want %d (%s)", status, tt.status, body)
			}
			if tt.status != http.StatusNotFound && !strings.Contains(body, `"error"`) {
				t.Fatalf("body = %s, want an error object", body)
			}
		})
	}
}

func TestHTTPWrongMethod(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/push")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /push = %d, want 405", resp.StatusCode)
	}
}

func TestHTTPHealth(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
}
