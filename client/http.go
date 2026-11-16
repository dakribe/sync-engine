package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dakribe/sync-engine/wire"
)

// HTTPTransport talks to a sync server over HTTP.
type HTTPTransport struct {
	// BaseURL is the server's address, like "http://localhost:8080".
	BaseURL string
	// Client sends the requests. Nil means http.DefaultClient.
	Client *http.Client
}

// StatusError is a non-200 answer from the server.
type StatusError struct {
	Code    int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("server answered %d: %s", e.Code, e.Message)
}

// Push sends mutations.
func (t *HTTPTransport) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	var resp wire.PushResponse
	err := t.post(ctx, "/push", req, &resp)
	return resp, err
}

// Pull fetches changes.
func (t *HTTPTransport) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	var resp wire.PullResponse
	err := t.post(ctx, "/pull", req, &resp)
	return resp, err
}

func (t *HTTPTransport) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(t.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	hc := t.Client
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if json.Unmarshal(b, &e) != nil || e.Error == "" {
			e.Error = strings.TrimSpace(string(b))
		}
		return &StatusError{Code: resp.StatusCode, Message: e.Error}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
