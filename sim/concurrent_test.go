package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/dakribe/sync-engine/client"
	"github.com/dakribe/sync-engine/server"
	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/wire"
)

// unreliable fails a share of requests at random and passes live updates through. It's
// safe for concurrent use.
type unreliable struct {
	engine *server.Engine
	mu     sync.Mutex
	rng    *rand.Rand
	failP  float64
}

func (u *unreliable) fail() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.rng.Float64() < u.failP
}

func (u *unreliable) heal() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failP = 0
}

func (u *unreliable) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	if u.fail() {
		return wire.PushResponse{}, errNetwork
	}
	resp, err := u.engine.Push(ctx, req)
	if u.fail() { // applied, but the response is lost
		return wire.PushResponse{}, errNetwork
	}
	return resp, err
}

func (u *unreliable) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	if u.fail() {
		return wire.PullResponse{}, errNetwork
	}
	return u.engine.Pull(ctx, req)
}

func (u *unreliable) Watch(ctx context.Context, fn func(uint64)) error {
	return u.engine.Watch(ctx, fn)
}

// TestConcurrentClientsConverge runs clients in real goroutines, each writing as fast as
// it can while its background loop syncs over a network that fails a quarter of
// requests. Run it with -race. After the network heals, every client must reach the
// server's state on its own, through live updates.
func TestConcurrentClientsConverge(t *testing.T) {
	const clients, writes = 6, 200
	engine := server.NewEngine(memory.New())
	defer engine.Close()
	net := &unreliable{engine: engine, rng: rand.New(rand.NewPCG(1, 2)), failP: 0.25}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var cs []*client.Client
	for range clients {
		c := client.New(net, client.Options{PushBatch: 5, PullLimit: 10, MinBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond})
		cs = append(cs, c)
		go c.Run(ctx)
	}

	var wg sync.WaitGroup
	for i, c := range cs {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(i), 99))
			for n := range writes {
				id := fmt.Sprint(rng.IntN(20))
				var err error
				if rng.IntN(6) == 0 {
					err = c.Delete("todos", id)
				} else {
					err = c.Mutate(wire.Op{Kind: wire.OpPut, Collection: "todos", ID: id, Fields: map[string]json.RawMessage{
						fmt.Sprintf("by%d", i): json.RawMessage(fmt.Sprint(n)),
					}})
				}
				if err != nil {
					t.Errorf("client %d write %d: %v", i, n, err)
					return
				}
			}
		})
	}
	wg.Wait()
	net.heal()

	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := engine.Pull(t.Context(), wire.PullRequest{ClientID: "checker", Limit: server.MaxPullLimit})
		if err != nil {
			t.Fatal(err)
		}
		want := newSnapshot()
		for _, rec := range resp.Records {
			want.PutRecord(rec)
		}
		converged := true
		for _, c := range cs {
			if c.Pending() != 0 || c.Cursor() != resp.Version || !maps.Equal(clientState(c), want.state()) {
				converged = false
				break
			}
		}
		if converged {
			if resp.Version < clients*writes/10 {
				t.Fatalf("server only reached version %d", resp.Version)
			}
			t.Logf("converged at version %d", resp.Version)
			return
		}
		if time.Now().After(deadline) {
			for i, c := range cs {
				t.Logf("client %d: %d pending, cursor %d of %d", i, c.Pending(), c.Cursor(), resp.Version)
			}
			t.Fatal("clients didn't converge after the network healed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
