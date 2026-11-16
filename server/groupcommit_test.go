package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/wire"
)

// gatedStore counts commits and holds the first one until released.
type gatedStore struct {
	store.Store
	updates atomic.Int32
	release chan struct{}
	fail    bool
}

func (g *gatedStore) Update(ctx context.Context, fn func(store.Tx) error) error {
	if g.updates.Add(1) == 1 {
		<-g.release
	}
	if g.fail {
		return errors.New("disk full")
	}
	return g.Store.Update(ctx, fn)
}

func queued(e *Engine) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.queue)
}

func waitQueued(t *testing.T, e *Engine, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for queued(e) < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d pushes queued, want %d", queued(e), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestConcurrentPushesShareACommit(t *testing.T) {
	gs := &gatedStore{Store: memory.New(), release: make(chan struct{})}
	e := NewEngine(gs)

	var wg sync.WaitGroup
	results := make([]wire.PushResponse, 11)
	for i := range results {
		wg.Go(func() {
			resp, err := e.Push(context.Background(), wire.PushRequest{ClientID: fmt.Sprint(i), Mutations: []wire.Mutation{
				{Seq: 1, Ops: []wire.Op{put("t", fmt.Sprint(i), map[string]any{"n": i})}},
			}})
			if err != nil {
				t.Error(err)
			}
			results[i] = resp
		})
		if i == 0 {
			// Wait for the first push to start committing, so the rest queue behind it.
			for gs.updates.Load() == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}
	waitQueued(t, e, 10)
	close(gs.release)
	wg.Wait()

	if n := gs.updates.Load(); n != 2 {
		t.Fatalf("%d commits, want 2: the first push, then the 10 that queued behind it", n)
	}
	for i, r := range results {
		if r.LastSeq != 1 {
			t.Fatalf("push %d: LastSeq %d, want 1", i, r.LastSeq)
		}
	}
	if v, _ := e.Version(context.Background()); v != 11 {
		t.Fatalf("version = %d, want 11", v)
	}
}

func TestFailedCommitFailsEveryPushInIt(t *testing.T) {
	gs := &gatedStore{Store: memory.New(), release: make(chan struct{}), fail: true}
	e := NewEngine(gs)

	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := range 4 {
		wg.Go(func() {
			_, err := e.Push(context.Background(), wire.PushRequest{ClientID: fmt.Sprint(i), Mutations: []wire.Mutation{
				{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"n": i})}},
			}})
			if err != nil {
				failures.Add(1)
			}
		})
		if i == 0 {
			for gs.updates.Load() == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}
	waitQueued(t, e, 3)
	close(gs.release)
	wg.Wait()
	if failures.Load() != 4 {
		t.Fatalf("%d of 4 pushes failed, want all", failures.Load())
	}
}

func TestCanceledLeaderStillCommitsTheBatch(t *testing.T) {
	gs := &gatedStore{Store: memory.New(), release: make(chan struct{})}
	e := NewEngine(gs)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		e.Push(ctx, wire.PushRequest{ClientID: "leader", Mutations: []wire.Mutation{{Seq: 1, Ops: []wire.Op{put("t", "1", map[string]any{"n": 1})}}}})
		close(done)
	}()
	for gs.updates.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	close(gs.release)
	<-done

	if v, _ := e.Version(context.Background()); v != 1 {
		t.Fatalf("version = %d, want 1: the leader's cancel undid the commit", v)
	}
}
