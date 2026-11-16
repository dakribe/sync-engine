package server

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/dakribe/sync-engine/store/sqlite"
	"github.com/dakribe/sync-engine/wire"
)

func benchEngine(b *testing.B) *Engine {
	b.Helper()
	st, err := sqlite.Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { st.Close() })
	return NewEngine(st)
}

func benchMutation(seq uint64, id int) wire.Mutation {
	return wire.Mutation{Seq: seq, Ops: []wire.Op{{
		Kind: wire.OpPut, Collection: "todos", ID: fmt.Sprint(id),
		Fields: map[string]json.RawMessage{"title": json.RawMessage(`"buy milk"`), "done": json.RawMessage("false")},
	}}}
}

// load fills the engine with n records, 1,000 mutations per push.
func load(b *testing.B, e *Engine, n int) {
	b.Helper()
	for start := 0; start < n; start += 1000 {
		var muts []wire.Mutation
		for i := start; i < min(n, start+1000); i++ {
			muts = append(muts, benchMutation(uint64(i+1), i))
		}
		if _, err := e.Push(context.Background(), wire.PushRequest{ClientID: "loader", Mutations: muts}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPush pushes one mutation per request, as a client typing would.
func BenchmarkPush(b *testing.B) {
	e := benchEngine(b)
	var seq uint64
	for b.Loop() {
		seq++
		if _, err := e.Push(b.Context(), wire.PushRequest{ClientID: "a", Mutations: []wire.Mutation{benchMutation(seq, int(seq%1000))}}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "mutations/s")
}

// BenchmarkPushBatch pushes 100 mutations per request, as a client back online would.
func BenchmarkPushBatch(b *testing.B) {
	e := benchEngine(b)
	var seq uint64
	for b.Loop() {
		muts := make([]wire.Mutation, 100)
		for i := range muts {
			seq++
			muts[i] = benchMutation(seq, int(seq%1000))
		}
		if _, err := e.Push(b.Context(), wire.PushRequest{ClientID: "a", Mutations: muts}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "mutations/s")
}

// BenchmarkPushParallel has many clients pushing one mutation each at the same time.
func BenchmarkPushParallel(b *testing.B) {
	e := benchEngine(b)
	var clients atomic.Int64
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		id := fmt.Sprint("c", clients.Add(1))
		var seq uint64
		for pb.Next() {
			seq++
			if _, err := e.Push(context.Background(), wire.PushRequest{ClientID: id, Mutations: []wire.Mutation{benchMutation(seq, int(seq%1000))}}); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "mutations/s")
}

// BenchmarkPullPage reads a full 1,000-record page from the middle of 100,000 records.
func BenchmarkPullPage(b *testing.B) {
	e := benchEngine(b)
	load(b, e, 100_000)
	for b.Loop() {
		resp, err := e.Pull(b.Context(), wire.PullRequest{ClientID: "a", Since: 50_000})
		if err != nil || len(resp.Records) != DefaultPullLimit {
			b.Fatalf("pull: %v, %d records", err, len(resp.Records))
		}
	}
}

// BenchmarkPullRecent reads the last 10 changes out of 100,000 records: a client that's
// nearly caught up, the common case with live updates.
func BenchmarkPullRecent(b *testing.B) {
	e := benchEngine(b)
	load(b, e, 100_000)
	for b.Loop() {
		resp, err := e.Pull(b.Context(), wire.PullRequest{ClientID: "a", Since: 100_000 - 10})
		if err != nil || len(resp.Records) != 10 {
			b.Fatalf("pull: %v, %d records", err, len(resp.Records))
		}
	}
}
