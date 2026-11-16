package server

import (
	"bufio"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/wire"
)

func TestSubscribeGetsNewVersions(t *testing.T) {
	e := NewEngine(memory.New())
	ch, stop := e.Subscribe()
	defer stop()

	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}})
	if v := <-ch; v != 1 {
		t.Fatalf("got version %d, want 1", v)
	}
}

func TestSlowSubscriberSkipsToLatest(t *testing.T) {
	e := NewEngine(memory.New())
	ch, stop := e.Subscribe()
	defer stop()

	for seq := uint64(1); seq <= 5; seq++ {
		push(t, e, "a", wire.Mutation{Seq: seq, Ops: []wire.Op{put("t", "1", map[string]any{"x": seq})}})
	}
	if v := <-ch; v != 5 {
		t.Fatalf("got version %d, want the latest, 5", v)
	}
	select {
	case v := <-ch:
		t.Fatalf("got a stale version %d after the latest", v)
	default:
	}
}

func TestPushThatChangesNothingIsNotAnnounced(t *testing.T) {
	e := NewEngine(memory.New())
	m := wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}}
	push(t, e, "a", m)

	ch, stop := e.Subscribe()
	defer stop()
	push(t, e, "a", m) // a retry: already applied
	select {
	case v := <-ch:
		t.Fatalf("a retried push announced version %d", v)
	default:
	}
}

func TestCloseEndsSubscriptions(t *testing.T) {
	e := NewEngine(memory.New())
	ch, _ := e.Subscribe()
	e.Close()
	if _, ok := <-ch; ok {
		t.Fatal("subscription still open after Close")
	}
	if late, _ := e.Subscribe(); late == nil {
		t.Fatal("nil channel")
	} else if _, ok := <-late; ok {
		t.Fatal("subscription after Close is open")
	}
}

// readEvent reads one server-sent event and returns its name and data.
func readEvent(t *testing.T, r *bufio.Reader) (string, string) {
	t.Helper()
	var event, data string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read event: %v", err)
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case line == "":
			if event != "" {
				return event, data
			}
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
}

func TestEventStream(t *testing.T) {
	old := HeartbeatInterval
	HeartbeatInterval = 10 * time.Millisecond
	defer func() { HeartbeatInterval = old }()

	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
	r := bufio.NewReader(resp.Body)

	if event, data := readEvent(t, r); event != "poke" || data != `{"version":0}` {
		t.Fatalf("first event = %s %s, want the current version", event, data)
	}

	time.Sleep(50 * time.Millisecond) // let heartbeats through; readEvent skips them
	post(t, srv, "/push", `{"clientId":"a","mutations":[{"seq":1,"ops":[{"kind":"put","collection":"t","id":"1","fields":{"x":1}}]}]}`)
	if event, data := readEvent(t, r); event != "poke" || data != `{"version":1}` {
		t.Fatalf("event after push = %s %s, want version 1", event, data)
	}
}
