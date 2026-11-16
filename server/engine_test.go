package server

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/store/sqlite"
	"github.com/dakribe/sync-engine/wire"
)

// stores are the store implementations every engine test runs against.
var stores = []struct {
	name string
	open func(t *testing.T) store.Store
}{
	{"memory", func(*testing.T) store.Store { return memory.New() }},
	{"sqlite", func(t *testing.T) store.Store {
		s, err := sqlite.Open(filepath.Join(t.TempDir(), "sync.db"))
		if err != nil {
			t.Fatalf("open sqlite: %v", err)
		}
		return s
	}},
}

var engineTests = []struct {
	name string
	run  func(t *testing.T, e *Engine)
}{
	{"PushThenPull", testPushThenPull},
	{"PushIsIdempotent", testPushIsIdempotent},
	{"PushStopsAtGap", testPushStopsAtGap},
	{"SeqIsPerClient", testSeqIsPerClient},
	{"InvalidMutationIsRejectedAndSkipped", testInvalidMutationIsRejectedAndSkipped},
	{"MutationIsAtomic", testMutationIsAtomic},
	{"PutMergesFields", testPutMergesFields},
	{"DeleteSendsTombstone", testDeleteSendsTombstone},
	{"PutAfterDeleteStartsFresh", testPutAfterDeleteStartsFresh},
	{"PullIsCompacted", testPullIsCompacted},
	{"PullFromCursorReturnsOnlyNewChanges", testPullFromCursorReturnsOnlyNewChanges},
	{"PulledRecordsAreCopies", testPulledRecordsAreCopies},
	{"RequestsNeedClientID", testRequestsNeedClientID},
	{"ConcurrentPushes", testConcurrentPushes},
	{"PullPagesNeverSplitAMutation", testPullPagesNeverSplitAMutation},
	{"PullSendsAMutationBiggerThanAPageWhole", testPullSendsAMutationBiggerThanAPageWhole},
	{"PullPagesByRecord", testPullPagesByRecord},
}

func TestEngine(t *testing.T) {
	for _, st := range stores {
		t.Run(st.name, func(t *testing.T) {
			for _, tt := range engineTests {
				t.Run(tt.name, func(t *testing.T) {
					s := st.open(t)
					t.Cleanup(func() { s.Close() })
					tt.run(t, NewEngine(s))
				})
			}
		})
	}
}

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
	resp, err := e.Push(t.Context(), wire.PushRequest{ClientID: client, Mutations: muts})
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	return resp
}

func pull(t *testing.T, e *Engine, client string, since uint64) wire.PullResponse {
	t.Helper()
	resp, err := e.Pull(t.Context(), wire.PullRequest{ClientID: client, Since: since})
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

func testPushThenPull(t *testing.T, e *Engine) {
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

func testPushIsIdempotent(t *testing.T, e *Engine) {
	m := wire.Mutation{Seq: 1, Ops: []wire.Op{put("counters", "c", map[string]any{"n": 1})}}
	push(t, e, "a", m)
	push(t, e, "a", m) // a retry after a lost response

	if got := pull(t, e, "a", 0); got.Version != 1 {
		t.Fatalf("version = %d after a retried push, want 1", got.Version)
	}
}

func testPushStopsAtGap(t *testing.T, e *Engine) {
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

func testSeqIsPerClient(t *testing.T, e *Engine) {
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "a", map[string]any{"x": 1})}})
	resp := push(t, e, "b", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "b", map[string]any{"x": 1})}})
	if resp.LastSeq != 1 {
		t.Fatalf("client b LastSeq = %d, want 1", resp.LastSeq)
	}
	if got := pull(t, e, "b", 0); got.Version != 2 {
		t.Fatalf("version = %d, want 2", got.Version)
	}
}

func testInvalidMutationIsRejectedAndSkipped(t *testing.T, e *Engine) {
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

func testMutationIsAtomic(t *testing.T, e *Engine) {
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

func testPutMergesFields(t *testing.T, e *Engine) {
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"title": "milk", "done": false})}})
	push(t, e, "b", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"done": true})}})
	push(t, e, "a", wire.Mutation{Seq: 2, Ops: []wire.Op{put("t", "1", map[string]any{"title": "oat milk"})}})

	rec := pull(t, e, "a", 0).Records[0]
	if field(t, rec, "title") != "oat milk" || field(t, rec, "done") != true {
		t.Fatalf("record = %s, want both edits kept", rec.Fields)
	}
}

func testDeleteSendsTombstone(t *testing.T, e *Engine) {
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}})
	cursor := pull(t, e, "b", 0).Version
	push(t, e, "a", wire.Mutation{Seq: 2, Ops: []wire.Op{del("t", "1")}})

	got := pull(t, e, "b", cursor)
	if len(got.Records) != 1 || !got.Records[0].Deleted || got.Records[0].Fields != nil {
		t.Fatalf("records = %+v, want one tombstone", got.Records)
	}
}

func testPutAfterDeleteStartsFresh(t *testing.T, e *Engine) {
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

func testPullIsCompacted(t *testing.T, e *Engine) {
	for seq := uint64(1); seq <= 100; seq++ {
		push(t, e, "a", wire.Mutation{Seq: seq, Ops: []wire.Op{put("counters", "c", map[string]any{"n": seq})}})
	}
	got := pull(t, e, "b", 0)
	if got.Version != 100 || len(got.Records) != 1 || field(t, got.Records[0], "n") != float64(100) {
		t.Fatalf("pull = version %d, %d records, want 100 and 1 record at n=100", got.Version, len(got.Records))
	}
}

func testPullFromCursorReturnsOnlyNewChanges(t *testing.T, e *Engine) {
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

func testPulledRecordsAreCopies(t *testing.T, e *Engine) {
	push(t, e, "a", wire.Mutation{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"x": 1})}})
	pull(t, e, "a", 0).Records[0].Fields["x"][0] = '9'

	if x := field(t, pull(t, e, "a", 0).Records[0], "x"); x != float64(1) {
		t.Fatalf("x = %v after editing a pulled copy, want 1", x)
	}
}

func testRequestsNeedClientID(t *testing.T, e *Engine) {
	if _, err := e.Push(t.Context(), wire.PushRequest{}); err != ErrNoClientID {
		t.Fatalf("push err = %v, want ErrNoClientID", err)
	}
	if _, err := e.Pull(t.Context(), wire.PullRequest{}); err != ErrNoClientID {
		t.Fatalf("pull err = %v, want ErrNoClientID", err)
	}
}

func testConcurrentPushes(t *testing.T, e *Engine) {
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

// pullAll follows pages until More is false and returns every page.
func pullAll(t *testing.T, e *Engine, since uint64, limit int) []wire.PullResponse {
	t.Helper()
	var pages []wire.PullResponse
	for {
		resp, err := e.Pull(t.Context(), wire.PullRequest{ClientID: "reader", Since: since, Limit: limit})
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		pages = append(pages, resp)
		if !resp.More {
			return pages
		}
		if resp.Version <= since {
			t.Fatalf("pull from %d made no progress", since)
		}
		since = resp.Version
	}
}

func testPullPagesNeverSplitAMutation(t *testing.T, e *Engine) {
	// 10 mutations of 3 records each.
	for seq := uint64(1); seq <= 10; seq++ {
		var ops []wire.Op
		for i := range 3 {
			ops = append(ops, put("t", fmt.Sprintf("%d-%d", seq, i), map[string]any{"n": i}))
		}
		push(t, e, "a", wire.Mutation{Seq: seq, Ops: ops})
	}

	pages := pullAll(t, e, 0, 4)
	seen := map[string]bool{}
	for i, page := range pages {
		if len(page.Records)%3 != 0 {
			t.Fatalf("page %d has %d records, which splits a mutation", i, len(page.Records))
		}
		for _, rec := range page.Records {
			if rec.Version > page.Version {
				t.Fatalf("page %d holds version %d past its cursor %d", i, rec.Version, page.Version)
			}
			seen[rec.ID] = true
		}
	}
	if len(seen) != 30 || len(pages) != 10 {
		t.Fatalf("%d pages with %d records, want 10 pages and 30 records", len(pages), len(seen))
	}
	if last := pages[len(pages)-1]; last.Version != 10 || last.More {
		t.Fatalf("last page = version %d, more %v", last.Version, last.More)
	}
}

func testPullSendsAMutationBiggerThanAPageWhole(t *testing.T, e *Engine) {
	var ops []wire.Op
	for i := range 5 {
		ops = append(ops, put("t", fmt.Sprint(i), map[string]any{"n": i}))
	}
	push(t, e, "a",
		wire.Mutation{Seq: 1, Ops: ops},
		wire.Mutation{Seq: 2, Ops: []wire.Op{put("t", "after", map[string]any{"n": 1})}},
	)

	pages := pullAll(t, e, 0, 2)
	if len(pages) != 2 || len(pages[0].Records) != 5 || pages[0].Version != 1 || len(pages[1].Records) != 1 {
		t.Fatalf("pages = %+v, want the 5-record mutation whole, then the last record", pages)
	}
}

func testPullPagesByRecord(t *testing.T, e *Engine) {
	for seq := uint64(1); seq <= 25; seq++ {
		push(t, e, "a", wire.Mutation{Seq: seq, Ops: []wire.Op{put("t", fmt.Sprint(seq), map[string]any{"n": seq})}})
	}
	pages := pullAll(t, e, 0, 10)
	if len(pages) != 3 || len(pages[0].Records) != 10 || len(pages[2].Records) != 5 {
		t.Fatalf("got %d pages, want 10, 10 and 5 records", len(pages))
	}
	if !pages[0].More || pages[0].Version != 10 {
		t.Fatalf("first page = version %d, more %v; want 10, true", pages[0].Version, pages[0].More)
	}
}
