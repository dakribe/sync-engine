// Package sim tests the whole system under a seeded, deterministic simulation: several
// clients make random edits over a network that drops, duplicates and replays requests,
// go offline and restart, and must still agree with the server in the end.
//
// The server is wrapped in a recorder that replays every mutation it applies onto a
// reference model, in the server's order, keeping the model's state at every version.
// After every step, each client's view must equal the model at the client's cursor with
// the client's own pending mutations replayed on top. At the end the network heals, and
// every client, the server and the model must hold the same data, with every mutation
// applied exactly once.
//
// A failing seed reproduces exactly: go test ./sim -run TestSimulation -seed N -v
package sim

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dakribe/sync-engine/client"
	"github.com/dakribe/sync-engine/server"
	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/store/memory"
	"github.com/dakribe/sync-engine/store/sqlite"
	"github.com/dakribe/sync-engine/wire"
)

var (
	seedFlag  = flag.Uint64("seed", 0, "run only this seed")
	seedsFlag = flag.Int("seeds", 100, "number of seeds to run")
	stepsFlag = flag.Int("steps", 400, "steps per seed")
)

var collections = []string{"todos", "notes"}

func TestSimulation(t *testing.T) {
	seeds, steps := *seedsFlag, *stepsFlag
	if testing.Short() {
		seeds = 30
	}
	run := func(seed uint64, newStore func(t *testing.T) store.Store) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			s := newSim(t, seed, newStore(t))
			s.run(steps)
		})
	}

	if *seedFlag != 0 {
		run(*seedFlag, memoryStore)
		return
	}
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		run(seed, memoryStore)
	}
}

func TestSimulationSQLite(t *testing.T) {
	seeds := 20
	if testing.Short() {
		seeds = 3
	}
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			s := newSim(t, seed, sqliteStore(t))
			s.run(*stepsFlag / 2)
		})
	}
}

func memoryStore(*testing.T) store.Store { return memory.New() }

func sqliteStore(t *testing.T) store.Store {
	st, err := sqlite.Open(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

type sim struct {
	t       *testing.T
	rng     *rand.Rand
	engine  *server.Engine
	model   *model
	clients []*simClient
	steps   int
}

// simClient is one replica plus what the test knows about it.
type simClient struct {
	c       *client.Client
	link    *link
	path    string          // SQLite file, or "" for an in-memory client
	created []wire.Mutation // every mutation it made, in order
}

func newSim(t *testing.T, seed uint64, st store.Store) *sim {
	s := &sim{t: t, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	s.engine = server.NewEngine(st)
	s.model = newModel()

	dir := t.TempDir()
	for i := range 4 {
		path := ""
		if i%2 == 1 { // half the clients keep their state in a file and can restart
			path = filepath.Join(dir, fmt.Sprintf("client%d.db", i))
		}
		sc := &simClient{path: path, link: &link{rng: s.rng, server: &recorder{engine: s.engine, model: s.model}}}
		sc.c = s.open(sc, fmt.Sprintf("c%d", i))
		s.clients = append(s.clients, sc)
	}
	t.Cleanup(func() {
		for _, sc := range s.clients {
			sc.c.Close()
		}
	})
	return s
}

func (s *sim) open(sc *simClient, id string) *client.Client {
	// Small batches and pages so pushes and pulls span several requests.
	opts := client.Options{ID: id, PushBatch: 3, PullLimit: 4}
	if sc.path == "" {
		return client.New(sc.link, opts)
	}
	c, err := client.Open(sc.path, sc.link, opts)
	if err != nil {
		s.t.Fatalf("open %s: %v", id, err)
	}
	return c
}

func (s *sim) run(steps int) {
	for range steps {
		s.steps++
		s.step()
		s.checkViews()
	}
	s.heal()
	s.checkConverged()
}

// step does one random thing.
func (s *sim) step() {
	sc := s.clients[s.rng.IntN(len(s.clients))]
	switch r := s.rng.IntN(100); {
	case r < 45:
		s.mutate(sc)
	case r < 80:
		sc.c.Sync(context.Background()) // errors are expected on a faulty network
	case r < 88:
		sc.link.offline = !sc.link.offline
	case r < 95:
		sc.link.faulty = !sc.link.faulty
	default:
		if sc.path != "" {
			if err := sc.c.Close(); err != nil {
				s.t.Fatal(err)
			}
			sc.c = s.open(sc, sc.c.ID())
		}
	}
}

func (s *sim) mutate(sc *simClient) {
	var ops []wire.Op
	for range 1 + s.rng.IntN(3) {
		col := collections[s.rng.IntN(len(collections))]
		id := fmt.Sprint(s.rng.IntN(6))
		if s.rng.IntN(5) == 0 {
			ops = append(ops, wire.Op{Kind: wire.OpDelete, Collection: col, ID: id})
			continue
		}
		fields := map[string]json.RawMessage{}
		for _, name := range []string{"title", "done", "rank"} {
			if s.rng.IntN(2) == 0 {
				fields[name] = json.RawMessage(fmt.Sprint(s.rng.IntN(100)))
			}
		}
		if len(fields) == 0 {
			fields["title"] = json.RawMessage(`0`)
		}
		ops = append(ops, wire.Op{Kind: wire.OpPut, Collection: col, ID: id, Fields: fields})
	}
	if err := sc.c.Mutate(ops...); err != nil {
		s.t.Fatalf("step %d: mutate: %v", s.steps, err)
	}
	sc.created = append(sc.created, wire.Mutation{Seq: uint64(len(sc.created) + 1), Ops: ops})
}

// checkViews checks every client shows the server's state at its cursor with its own
// pending mutations on top.
func (s *sim) checkViews() {
	for _, sc := range s.clients {
		want := s.model.at(sc.c.Cursor())
		for _, m := range sc.created[len(sc.created)-sc.c.Pending():] {
			wire.Apply(want, 0, m)
		}
		if got, want := clientState(sc.c), want.state(); !maps.Equal(got, want) {
			s.t.Fatalf("step %d: %s at cursor %d with %d pending shows\n  %v\nwant\n  %v",
				s.steps, sc.c.ID(), sc.c.Cursor(), sc.c.Pending(), got, want)
		}
	}
}

// heal brings every client online on a clean network and syncs until nothing changes.
func (s *sim) heal() {
	for _, sc := range s.clients {
		sc.link.offline, sc.link.faulty = false, false
	}
	// Two rounds: the first pushes everything, the second pulls what others pushed.
	for range 2 {
		for _, sc := range s.clients {
			if err := sc.c.Sync(context.Background()); err != nil {
				s.t.Fatalf("sync %s on a healed network: %v", sc.c.ID(), err)
			}
		}
	}
}

func (s *sim) checkConverged() {
	server := s.serverState()
	if model := s.model.at(s.model.version()).state(); !maps.Equal(server, model) {
		s.t.Fatalf("server and model differ:\n  server %v\n  model  %v", server, model)
	}
	for _, sc := range s.clients {
		if sc.c.Pending() != 0 {
			s.t.Fatalf("%s still has %d pending after healing", sc.c.ID(), sc.c.Pending())
		}
		if got := clientState(sc.c); !maps.Equal(got, server) {
			s.t.Fatalf("%s didn't converge:\n  client %v\n  server %v", sc.c.ID(), got, server)
		}
		applied := s.model.applied[sc.c.ID()]
		if len(applied) != len(sc.created) {
			s.t.Fatalf("%s made %d mutations, the server applied %d", sc.c.ID(), len(sc.created), len(applied))
		}
		for i, seq := range applied {
			if seq != uint64(i+1) {
				s.t.Fatalf("%s: server applied seqs %v, want 1..%d once each, in order", sc.c.ID(), applied, len(sc.created))
			}
		}
	}
}

func (s *sim) serverState() map[string]string {
	resp, err := s.engine.Pull(context.Background(), wire.PullRequest{ClientID: "checker", Limit: server.MaxPullLimit})
	if err != nil || resp.More {
		s.t.Fatalf("full pull: %v (more: %v)", err, resp.More)
	}
	if resp.Version != s.model.version() {
		s.t.Fatalf("server at version %d, model at %d", resp.Version, s.model.version())
	}
	st := newSnapshot()
	for _, rec := range resp.Records {
		st.PutRecord(rec)
	}
	return st.state()
}

func clientState(c *client.Client) map[string]string {
	st := newSnapshot()
	for _, col := range collections {
		for _, rec := range c.List(col) {
			st.PutRecord(rec)
		}
	}
	return st.state()
}

// link is one client's connection to the server. Offline, every request fails. Faulty,
// requests are randomly dropped before reaching the server, have their response lost
// after the server applied them, are delivered twice, or are preceded by a stale copy of
// an earlier push arriving late.
type link struct {
	rng     *rand.Rand
	server  *recorder
	offline bool
	faulty  bool
	sent    []wire.PushRequest
}

var errNetwork = errors.New("simulated network failure")

func (l *link) fault(p float64) bool { return l.faulty && l.rng.Float64() < p }

func (l *link) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	if l.offline || l.fault(0.2) {
		return wire.PushResponse{}, errNetwork
	}
	if len(l.sent) > 0 && l.fault(0.2) {
		l.server.Push(ctx, l.sent[l.rng.IntN(len(l.sent))])
	}
	l.sent = append(l.sent, req)
	resp, err := l.server.Push(ctx, req)
	if err == nil && l.fault(0.2) {
		resp, err = l.server.Push(ctx, req)
	}
	if l.fault(0.2) {
		return wire.PushResponse{}, errNetwork
	}
	return resp, err
}

func (l *link) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	if l.offline || l.fault(0.3) {
		return wire.PullResponse{}, errNetwork
	}
	return l.server.Pull(ctx, req)
}

// recorder passes requests to the engine and replays each newly applied mutation onto
// the model. The simulation runs one request at a time, so the order it sees is the
// server's order.
type recorder struct {
	engine *server.Engine
	model  *model
}

func (r *recorder) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	before := uint64(len(r.model.applied[req.ClientID]))
	resp, err := r.engine.Push(ctx, req)
	if err != nil {
		return resp, err
	}
	rejected := map[uint64]bool{}
	for _, rj := range resp.Rejected {
		rejected[rj.Seq] = true
	}
	for _, m := range req.Mutations {
		if m.Seq > before && m.Seq <= resp.LastSeq {
			r.model.applied[req.ClientID] = append(r.model.applied[req.ClientID], m.Seq)
			if !rejected[m.Seq] {
				r.model.apply(m)
			}
		}
	}
	return resp, nil
}

func (r *recorder) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	return r.engine.Pull(ctx, req)
}

// model is the reference: the server's state after every version.
type model struct {
	history []*snapshot         // history[v] is the state at version v
	applied map[string][]uint64 // per client, the seqs the server applied, in order
}

func newModel() *model {
	return &model{history: []*snapshot{newSnapshot()}, applied: map[string][]uint64{}}
}

func (m *model) version() uint64 { return uint64(len(m.history) - 1) }

func (m *model) apply(mut wire.Mutation) {
	next := m.at(m.version())
	wire.Apply(next, m.version()+1, mut)
	m.history = append(m.history, next)
}

// at returns a copy of the state at version v.
func (m *model) at(v uint64) *snapshot {
	return &snapshot{recs: maps.Clone(m.history[v].recs)}
}

// snapshot is a set of records that wire.Apply can write to.
type snapshot struct {
	recs map[string]wire.Record
}

func newSnapshot() *snapshot { return &snapshot{recs: map[string]wire.Record{}} }

func (s *snapshot) Record(collection, id string) (wire.Record, bool, error) {
	rec, ok := s.recs[collection+"/"+id]
	return rec.Clone(), ok, nil
}

func (s *snapshot) PutRecord(rec wire.Record) error {
	s.recs[rec.Collection+"/"+rec.ID] = rec.Clone()
	return nil
}

// state renders live records as "collection/id" -> fields JSON, for comparing.
func (s *snapshot) state() map[string]string {
	out := map[string]string{}
	for _, k := range slices.Sorted(maps.Keys(s.recs)) {
		rec := s.recs[k]
		if rec.Deleted {
			continue
		}
		b, _ := json.Marshal(rec.Fields)
		out[k] = string(b)
	}
	return out
}
