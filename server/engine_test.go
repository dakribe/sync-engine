package server

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/dakribe/sync-engine/wire"
)

func put(collection, id string, fields map[string]any) wire.Op {
	raw := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		b, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}
		raw[name] = b
	}
	return wire.Op{Kind: wire.OpPut, Collection: collection, ID: id, Fields: raw}
}

func del(collection, id string) wire.Op {
	return wire.Op{Kind: wire.OpDelete, Collection: collection, ID: id}
}

func push(t *testing.T, e *Engine, client string, muts ...wire.Mutation) wire.PushResponse {
	t.Helper()
	resp, err := e.Push(wire.PushRequest{ClientID: client, Mutations: muts})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	return resp
}

func pull(t *testing.T, e *Engine, client string, since uint64) wire.PullResponse {
	t.Helper()
	resp, err := e.Pull(wire.PullRequest{ClientID: client, Since: since})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	return resp
}

func field(t *testing.T, rec wire.Record, name string) any {
	t.Helper()
	raw, ok := rec.Fields[name]
	if !ok {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("field %q: %v", name, err)
	}
	return v
}

func TestPushThenPull(t *testing.T) {
	e := NewEngine()
	resp := push(t, e, "a",
		wire.Mutation{Seq: 1, Ops: []wire.Op{put("todos", "1", map[string]any{"title": "milk"})}},
		wire.Mutation{Seq: 2, Ops: []wire.Op{put("todos", "2", map[string]any{"title": "eggs"})}},
	)
	if resp.LastSeq != 2 {
		t.Fatalf("LastSeq = %d, want 2", resp.LastSeq)
	}

	got := pull(t, e, "a", 0)
	if got.Version != 2 || got.LastSeq != 2 || len(got.Records) != 2 {
		t.Fatalf("pull = version %d, lastSeq %d, %d records; want 2, 2, 2", got.Version, got.LastSeq, len(got.Records))
	}
	if got.Records[0].ID != "1" || got.Records[1].ID != "2" {
		t.Fatalf("records out of order: %s, %s", got.Records[0].ID, got.Records[1].ID)
	}
	if title := field(t, got.Records[1], "title"); title != "eggs" {
		t.Fatalf("title = %v, want eggs", title)
	}
}

func TestPushIsIdempotent(t *testing.T) {
	e := NewEngine()
	m := wire.Mutation{Seq: 1, Ops: []wire.Op{put("counters", "c", map[string]any{"n": 1})}}
	push(t, e, "a", m)
	push(t, e, "a", m) // a retry after a lost response

	if got := pull(t, e, "a", 0); got.Version != 1 {
		t.Fatalf("version = %d after a retried push, want 1", got.Version)
	}
}

func TestPushStopsAtGap(t *testing.T) {
	e := NewEngine()
	resp := push(t, e, "a",
		wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}},
		wire.Mutation{Seq: 3, Ops: []wire.Op{put("t", "3", map[string]any{"x": 3})}},
	)
	if resp.LastSeq != 1 {
		t.Fatalf("LastSeq = %d, want 1 (seq 2 is missing)", resp.LastSeq)
	}
	if got := pull(t, e, "a", 0); len(got.Records) != 1 {
		t.Fatalf("%d records, want 1", len(got.Records))
	}
}

func TestSeqIsPerClient(t *testing.T) {
	e := NewEngine()
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "a", map[string]any{"x": 1})}})
	resp := push(t, e, "b", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "b", map[string]any{"x": 1})}})
	if resp.LastSeq != 1 {
		t.Fatalf("client b LastSeq = %d, want 1", resp.LastSeq)
	}
	if got := pull(t, e, "b", 0); got.Version != 2 {
		t.Fatalf("version = %d, want 2", got.Version)
	}
}

func TestInvalidMutationIsRejectedAndSkipped(t *testing.T) {
	e := NewEngine()
	resp := push(t, e, "a",
		wire.Mutation{Seq: 1, Ops: []wire.Op{{Kind: "rename", Collection: "t", ID: "1"}}},
		wire.Mutation{Seq: 2, Ops: []wire.Op{put("t", "2", map[string]any{"x": 2})}},
	)
	if resp.LastSeq != 2 {
		t.Fatalf("LastSeq = %d, want 2", resp.LastSeq)
	}
	if len(resp.Rejected) != 1 || resp.Rejected[0].Seq != 1 {
		t.Fatalf("rejected = %+v, want seq 1", resp.Rejected)
	}
	if got := pull(t, e, "a", 0); len(got.Records) != 1 || got.Records[0].ID != "2" {
		t.Fatalf("records = %+v, want only record 2", got.Records)
	}
}

func TestMutationIsAtomic(t *testing.T) {
	e := NewEngine()
	resp := push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{
		put("t", "1", map[string]any{"x": 1}),
		{Kind: wire.OpPut, Collection: "", ID: "2"},
	}})
	if len(resp.Rejected) != 1 {
		t.Fatalf("rejected = %+v, want 1", resp.Rejected)
	}
	if got := pull(t, e, "a", 0); len(got.Records) != 0 {
		t.Fatalf("half a mutation was applied: %+v", got.Records)
	}
}

func TestPutMergesFields(t *testing.T) {
	e := NewEngine()
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"title": "milk", "done": false})}})
	push(t, e, "b", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"done": true})}})
	push(t, e, "a", wire.Mutation{Seq: 2, Ops: []wire.Op{put("t", "1", map[string]any{"title": "oat milk"})}})

	rec := pull(t, e, "a", 0).Records[0]
	if field(t, rec, "title") != "oat milk" || field(t, rec, "done") != true {
		t.Fatalf("record = %s, want both edits kept", rec.Fields)
	}
}

func TestDeleteSendsTombstone(t *testing.T) {
	e := NewEngine()
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}})
	cursor := pull(t, e, "b", 0).Version
	push(t, e, "a", wire.Mutation{Seq: 2, Ops: []wire.Op{del("t", "1")}})

	got := pull(t, e, "b", cursor)
	if len(got.Records) != 1 || !got.Records[0].Deleted || got.Records[0].Fields != nil {
		t.Fatalf("records = %+v, want one tombstone", got.Records)
	}
}

func TestPutAfterDeleteStartsFresh(t *testing.T) {
	e := NewEngine()
	push(t, e, "a",
		wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"old": 1})}},
		wire.Mutation{Seq: 2, Ops: []wire.Op{del("t", "1")}},
		wire.Mutation{Seq: 3, Ops: []wire.Op{put("t", "1", map[string]any{"new": 1})}},
	)
	rec := pull(t, e, "a", 0).Records[0]
	if rec.Deleted || field(t, rec, "old") != nil || field(t, rec, "new") == nil {
		t.Fatalf("record = %+v, want only the new field", rec)
	}
}

func TestPullIsCompacted(t *testing.T) {
	e := NewEngine()
	for seq := uint64(1); seq <= 100; seq++ {
		push(t, e, "a", wire.Mutation{Seq: seq, Ops: []wire.Op{put("counters", "c", map[string]any{"n": seq})}})
	}
	got := pull(t, e, "b", 0)
	if got.Version != 100 || len(got.Records) != 1 || field(t, got.Records[0], "n") != float64(100) {
		t.Fatalf("pull = version %d, %d records, want 100 and 1 record at n=100", got.Version, len(got.Records))
	}
}

func TestPullFromCursorReturnsOnlyNewChanges(t *testing.T) {
	e := NewEngine()
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}})
	cursor := pull(t, e, "b", 0).Version
	push(t, e, "a", wire.Mutation{Seq: 2, Ops: []wire.Op{put("t", "2", map[string]any{"x": 2})}})

	got := pull(t, e, "b", cursor)
	if len(got.Records) != 1 || got.Records[0].ID != "2" {
		t.Fatalf("records = %+v, want only record 2", got.Records)
	}
	if again := pull(t, e, "b", got.Version); len(again.Records) != 0 {
		t.Fatalf("pull at the latest version returned %d records", len(again.Records))
	}
}

func TestPulledRecordsAreCopies(t *testing.T) {
	e := NewEngine()
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}})
	pull(t, e, "a", 0).Records[0].Fields["x"][0] = '9'

	if x := field(t, pull(t, e, "a", 0).Records[0], "x"); x != float64(1) {
		t.Fatalf("x = %v after editing a pulled copy, want 1", x)
	}
}

func TestRequestsNeedClientID(t *testing.T) {
	e := NewEngine()
	if _, err := e.Push(wire.PushRequest{}); err != ErrNoClientID {
		t.Fatalf("push err = %v, want ErrNoClientID", err)
	}
	if _, err := e.Pull(wire.PullRequest{}); err != ErrNoClientID {
		t.Fatalf("pull err = %v, want ErrNoClientID", err)
	}
}

func TestConcurrentPushes(t *testing.T) {
	e := NewEngine()
	const clients, perClient = 8, 50
	var wg sync.WaitGroup
	for c := range clients {
		wg.Go(func() {
			id := string(rune('a' + c))
			for seq := uint64(1); seq <= perClient; seq++ {
				push(t, e, id, wire.Mutation{Seq: seq, Ops: []wire.Op{put("t", id, map[string]any{"n": seq})}})
			}
		})
	}
	wg.Wait()

	got := pull(t, e, "reader", 0)
	if got.Version != clients*perClient || len(got.Records) != clients {
		t.Fatalf("version %d with %d records, want %d and %d", got.Version, len(got.Records), clients*perClient, clients)
	}
}
