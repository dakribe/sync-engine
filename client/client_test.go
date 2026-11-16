package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/dakribe/sync-engine/server"
	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/wire"
)

var errOffline = errors.New("offline")

// flaky wraps a transport and can fail pushes or pulls on demand.
type flaky struct {
	Transport
	mu                 sync.Mutex
	failPush, failPull bool
	dropPushResponse   bool // apply the push but report an error, as if the reply was lost
	pushes, pulls      int
}

func (f *flaky) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	f.mu.Lock()
	failPush, drop := f.failPush, f.dropPushResponse
	f.pushes++
	f.mu.Unlock()
	if failPush {
		return wire.PushResponse{}, errOffline
	}
	resp, err := f.Transport.Push(ctx, req)
	if drop {
		return wire.PushResponse{}, errOffline
	}
	return resp, err
}

func (f *flaky) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	f.mu.Lock()
	failPull := f.failPull
	f.pulls++
	f.mu.Unlock()
	if failPull {
		return wire.PullResponse{}, errOffline
	}
	return f.Transport.Pull(ctx, req)
}

func (f *flaky) set(fn func(f *flaky)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func newEngine() *server.Engine { return server.NewEngine(memory.New()) }

func mustSync(t *testing.T, c *Client) {
	t.Helper()
	if err := c.Sync(t.Context()); err != nil {
		t.Fatalf("sync %s: %v", c.ID(), err)
	}
}

func mustPut(t *testing.T, c *Client, collection, id string, fields map[string]any) {
	t.Helper()
	if err := c.Put(collection, id, fields); err != nil {
		t.Fatalf("put: %v", err)
	}
}

func fieldOf(t *testing.T, c *Client, collection, id, name string) any {
	t.Helper()
	rec, ok := c.Get(collection, id)
	if !ok {
		return nil
	}
	var v any
	if raw, ok := rec.Fields[name]; ok {
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
	}
	return v
}

func TestWritesShowBeforeSync(t *testing.T) {
	c := New(&flaky{Transport: newEngine(), failPush: true, failPull: true}, Options{})
	mustPut(t, c, "todos", "1", map[string]any{"title": "milk"})

	if got := fieldOf(t, c, "todos", "1", "title"); got != "milk" {
		t.Fatalf("title = %v, want milk", got)
	}
	if err := c.Sync(t.Context()); !errors.Is(err, errOffline) {
		t.Fatalf("sync err = %v, want offline", err)
	}
	if c.Pending() != 1 || fieldOf(t, c, "todos", "1", "title") != "milk" {
		t.Fatal("a failed sync lost the local change")
	}
}

func TestSyncSharesChanges(t *testing.T) {
	e := newEngine()
	a, b := New(e, Options{}), New(e, Options{})

	mustPut(t, a, "todos", "1", map[string]any{"title": "milk"})
	mustSync(t, a)
	mustSync(t, b)

	if got := fieldOf(t, b, "todos", "1", "title"); got != "milk" {
		t.Fatalf("b sees title %v, want milk", got)
	}
	if a.Pending() != 0 {
		t.Fatalf("a has %d pending after sync", a.Pending())
	}

	if err := b.Delete("todos", "1"); err != nil {
		t.Fatal(err)
	}
	mustSync(t, b)
	mustSync(t, a)
	if _, ok := a.Get("todos", "1"); ok || len(a.List("todos")) != 0 {
		t.Fatal("delete didn't reach a")
	}
}

func TestOfflineEditsRebaseOntoServerChanges(t *testing.T) {
	e := newEngine()
	a, b := New(e, Options{}), New(e, Options{})
	mustPut(t, a, "todos", "1", map[string]any{"title": "milk", "done": false})
	mustSync(t, a)
	mustSync(t, b)

	// a edits the title while offline; meanwhile b marks it done and syncs.
	mustPut(t, a, "todos", "1", map[string]any{"title": "oat milk"})
	mustPut(t, b, "todos", "1", map[string]any{"done": true})
	mustSync(t, b)

	// a pulls b's change before pushing: its pending edit must stay on top.
	if err := a.pull(t.Context()); err != nil {
		t.Fatal(err)
	}
	if fieldOf(t, a, "todos", "1", "title") != "oat milk" || fieldOf(t, a, "todos", "1", "done") != true {
		t.Fatal("rebase lost a's pending edit or b's change")
	}

	mustSync(t, a)
	mustSync(t, b)
	for _, c := range []*Client{a, b} {
		if fieldOf(t, c, "todos", "1", "title") != "oat milk" || fieldOf(t, c, "todos", "1", "done") != true {
			t.Fatalf("%s didn't converge: %+v", c.ID(), c.List("todos"))
		}
	}
}

func TestServerOrderWinsOnTheSameField(t *testing.T) {
	e := newEngine()
	a, b := New(e, Options{}), New(e, Options{})
	mustPut(t, a, "todos", "1", map[string]any{"title": "from a"})
	mustPut(t, b, "todos", "1", map[string]any{"title": "from b"})

	mustSync(t, a) // a's edit reaches the server first
	mustSync(t, b) // so b's lands last and wins
	mustSync(t, a)

	for _, c := range []*Client{a, b} {
		if got := fieldOf(t, c, "todos", "1", "title"); got != "from b" {
			t.Fatalf("%s sees %v, want from b", c.ID(), got)
		}
	}
}

func TestChangeDoesNotFlickerBetweenPushAndPull(t *testing.T) {
	f := &flaky{Transport: newEngine()}
	c := New(f, Options{})
	mustPut(t, c, "todos", "1", map[string]any{"title": "milk"})

	f.set(func(f *flaky) { f.failPull = true })
	c.Sync(t.Context()) // push succeeds, pull fails
	if fieldOf(t, c, "todos", "1", "title") != "milk" {
		t.Fatal("pushed change vanished before the pull brought it back")
	}

	f.set(func(f *flaky) { f.failPull = false })
	mustSync(t, c)
	if c.Pending() != 0 || fieldOf(t, c, "todos", "1", "title") != "milk" {
		t.Fatal("change missing after a full sync")
	}
}

func TestLostPushResponseIsRetriedSafely(t *testing.T) {
	e := newEngine()
	f := &flaky{Transport: e, dropPushResponse: true}
	c := New(f, Options{})
	mustPut(t, c, "counters", "c", map[string]any{"n": 1})

	if err := c.Sync(t.Context()); err == nil {
		t.Fatal("sync succeeded with a lost response")
	}
	f.set(func(f *flaky) { f.dropPushResponse = false })
	mustSync(t, c)

	resp, _ := e.Pull(t.Context(), wire.PullRequest{ClientID: "x"})
	if resp.Version != 1 {
		t.Fatalf("server version = %d, want 1 (the retry was applied twice)", resp.Version)
	}
}

func TestPushesInBatches(t *testing.T) {
	f := &flaky{Transport: newEngine()}
	c := New(f, Options{PushBatch: 10})
	for i := range 25 {
		mustPut(t, c, "t", fmt.Sprint(i), map[string]any{"n": i})
	}
	mustSync(t, c)
	if f.pushes != 3 || c.Pending() != 0 || len(c.List("t")) != 25 {
		t.Fatalf("%d pushes, %d pending, %d records; want 3, 0, 25", f.pushes, c.Pending(), len(c.List("t")))
	}
}

func TestPullsInPages(t *testing.T) {
	e := newEngine()
	writer := New(e, Options{})
	for i := range 25 {
		mustPut(t, writer, "t", fmt.Sprint(i), map[string]any{"n": i})
	}
	mustSync(t, writer)

	f := &flaky{Transport: e}
	reader := New(f, Options{PullLimit: 10})
	mustSync(t, reader)
	if f.pulls != 3 || len(reader.List("t")) != 25 || reader.Cursor() != 25 {
		t.Fatalf("%d pulls, %d records, cursor %d; want 3, 25, 25", f.pulls, len(reader.List("t")), reader.Cursor())
	}
}

// rejecting rejects every mutation, standing in for server-side rules a client can't
// check, like permissions.
type rejecting struct{ Transport }

func (r rejecting) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	var resp wire.PushResponse
	for _, m := range req.Mutations {
		resp.Rejected = append(resp.Rejected, wire.Rejection{Seq: m.Seq, Reason: "not allowed"})
		resp.LastSeq = m.Seq
	}
	return resp, nil
}

func (r rejecting) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	return wire.PullResponse{LastSeq: 1, Records: []wire.Record{}}, nil
}

func TestRejectedMutationIsReportedAndUndone(t *testing.T) {
	var got []wire.Rejection
	c := New(rejecting{}, Options{OnReject: func(r wire.Rejection) { got = append(got, r) }})
	mustPut(t, c, "t", "1", map[string]any{"x": 1})
	mustSync(t, c)

	if len(got) != 1 || got[0].Reason != "not allowed" {
		t.Fatalf("rejections = %+v", got)
	}
	if _, ok := c.Get("t", "1"); ok || c.Pending() != 0 {
		t.Fatal("rejected change is still visible")
	}
}

func TestInvalidMutationIsRefusedLocally(t *testing.T) {
	c := New(newEngine(), Options{})
	if err := c.Put("", "1", map[string]any{"x": 1}); err == nil {
		t.Fatal("put with no collection succeeded")
	}
	if c.Pending() != 0 {
		t.Fatal("invalid mutation was queued")
	}
	// The next valid mutation still gets seq 1, so the server sees no gap.
	mustPut(t, c, "t", "1", map[string]any{"x": 1})
	mustSync(t, c)
	if c.Pending() != 0 {
		t.Fatal("valid mutation wasn't confirmed")
	}
}

func TestSubscribeFiresOnChange(t *testing.T) {
	e := newEngine()
	a, b := New(e, Options{}), New(e, Options{})
	changes, stop := b.Subscribe()
	defer stop()

	mustPut(t, a, "t", "1", map[string]any{"x": 1})
	mustSync(t, a)
	mustSync(t, b)
	select {
	case <-changes:
	default:
		t.Fatal("no change notification after a pull brought new data")
	}
}

func TestReturnedRecordsAreCopies(t *testing.T) {
	c := New(newEngine(), Options{})
	mustPut(t, c, "t", "1", map[string]any{"x": 1})
	rec, _ := c.Get("t", "1")
	rec.Fields["x"][0] = '9'
	if fieldOf(t, c, "t", "1", "x") != float64(1) {
		t.Fatal("editing a returned record changed the client's data")
	}
}

func TestOverHTTP(t *testing.T) {
	srv := httptest.NewServer(server.NewHandler(newEngine(), slog.New(slog.DiscardHandler)))
	defer srv.Close()

	a := New(&HTTPTransport{BaseURL: srv.URL}, Options{})
	b := New(&HTTPTransport{BaseURL: srv.URL}, Options{})
	mustPut(t, a, "todos", "1", map[string]any{"title": "milk"})
	mustSync(t, a)
	mustSync(t, b)
	if fieldOf(t, b, "todos", "1", "title") != "milk" {
		t.Fatal("change didn't travel over HTTP")
	}

	bad := &HTTPTransport{BaseURL: srv.URL}
	_, err := bad.Pull(t.Context(), wire.PullRequest{})
	if se, ok := errors.AsType[*StatusError](err); !ok || se.Code != 400 {
		t.Fatalf("pull with no client id: err = %v, want a 400 StatusError", err)
	}
}
