// Package client keeps a local copy of the server's data that can be read and written
// offline.
//
// The client holds two layers. The base is the server's data as of the last pull. On top
// of it sit the pending mutations: changes made locally that the server hasn't confirmed
// yet. Reads see the base with the pending mutations applied, so a change shows the moment
// it's made. Syncing pushes the pending mutations, pulls what changed on the server, drops
// the mutations the server confirmed and replays the rest on top of the new base (a
// rebase). The rebase only touches records the pending mutations touch, so it costs the
// same with 10 records or 10 million.
package client

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/dakribe/sync-engine/wire"
)

// Transport carries requests to the server. *server.Engine is a Transport, which is
// handy in tests; HTTPTransport talks to a remote server.
type Transport interface {
	Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error)
	Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error)
}

// Options configure a client. The zero value works.
type Options struct {
	// ID identifies this client's queue of mutations to the server. Two clients must never
	// share an ID. Empty means a new random ID.
	ID string
	// PushBatch caps the mutations sent in one push. 0 means 100.
	PushBatch int
	// PullLimit is the page size to ask for. 0 means the server's default.
	PullLimit int
	// OnReject is called when the server rejects a mutation. The mutation's effect is
	// removed from local data at the next pull.
	OnReject func(wire.Rejection)

	// PollInterval is how often Run syncs when nothing else prompts it. 0 means
	// DefaultPollInterval.
	PollInterval time.Duration
	// MinBackoff and MaxBackoff bound how long Run waits after failed syncs. 0 means
	// DefaultMinBackoff and DefaultMaxBackoff.
	MinBackoff, MaxBackoff time.Duration
	// OnSyncError is called when a sync in Run fails, for example while offline.
	OnSyncError func(error)
}

// Client is a local replica. Its methods are safe to call from many goroutines.
type Client struct {
	id        string
	transport Transport
	opts      Options

	syncMu sync.Mutex // one sync at a time

	mu      sync.Mutex
	base    *table // server data as of cursor
	view    *layer // base plus pending mutations
	pending []wire.Mutation
	nextSeq uint64
	cursor  uint64
	subs    map[chan struct{}]struct{}

	wake chan struct{} // asks Run to sync
}

// New returns an empty client that syncs through t.
func New(t Transport, opts Options) *Client {
	if opts.ID == "" {
		opts.ID = rand.Text()
	}
	if opts.PushBatch <= 0 {
		opts.PushBatch = 100
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.MinBackoff <= 0 {
		opts.MinBackoff = DefaultMinBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = DefaultMaxBackoff
	}
	c := &Client{
		id:        opts.ID,
		transport: t,
		opts:      opts,
		base:      newTable(),
		nextSeq:   1,
		subs:      map[chan struct{}]struct{}{},
		wake:      make(chan struct{}, 1),
	}
	c.view = newLayer(c.base)
	return c
}

// ID returns the client's ID.
func (c *Client) ID() string { return c.id }

// Mutate applies ops locally as one mutation and queues it for the server. The change is
// visible to reads as soon as Mutate returns.
func (c *Client) Mutate(ops ...wire.Op) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := wire.Mutation{Seq: c.nextSeq, Ops: slices.Clone(ops)}
	if err := m.Validate(); err != nil {
		return err
	}
	if err := wire.Apply(c.view, 0, m); err != nil {
		return err
	}
	c.nextSeq++
	c.pending = append(c.pending, m)
	c.notify()
	c.Poke()
	return nil
}

// Put sets fields on a record, creating it if needed. Each field value is encoded as JSON.
func (c *Client) Put(collection, id string, fields map[string]any) error {
	raw := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		b, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
		raw[name] = b
	}
	return c.Mutate(wire.Op{Kind: wire.OpPut, Collection: collection, ID: id, Fields: raw})
}

// Delete removes a record.
func (c *Client) Delete(collection, id string) error {
	return c.Mutate(wire.Op{Kind: wire.OpDelete, Collection: collection, ID: id})
}

// Get returns a record as the client currently sees it.
func (c *Client) Get(collection, id string) (wire.Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok, _ := c.view.Record(collection, id)
	if !ok || rec.Deleted {
		return wire.Record{}, false
	}
	return rec, true
}

// List returns the records in a collection, ordered by id.
func (c *Client) List(collection string) []wire.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.view.list(collection)
}

// Pending returns the number of mutations the server hasn't confirmed.
func (c *Client) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Cursor returns the server version the client's base is at.
func (c *Client) Cursor() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

// Subscribe returns a channel that receives a value after local data changes. Changes
// that land while a value is waiting are folded into it. Call the returned function to
// stop.
func (c *Client) Subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.subs[ch] = struct{}{}
	c.mu.Unlock()
	return ch, func() {
		c.mu.Lock()
		delete(c.subs, ch)
		c.mu.Unlock()
	}
}

// notify wakes subscribers. The caller holds c.mu.
func (c *Client) notify() {
	for ch := range c.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Sync pushes pending mutations, then pulls until the client has caught up with the
// server. It's safe to call at any time, including while offline: on a network error
// nothing is lost, and the next Sync picks up where this one stopped.
func (c *Client) Sync(ctx context.Context) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if err := c.push(ctx); err != nil {
		return fmt.Errorf("push: %w", err)
	}
	if err := c.pull(ctx); err != nil {
		return fmt.Errorf("pull: %w", err)
	}
	return nil
}

// errNoProgress means the server stopped accepting this client's mutations, which happens
// if it lost data or another client used the same ID.
var errNoProgress = errors.New("server did not accept the next mutation")

// push sends pending mutations in batches. It doesn't drop them: their effects aren't in
// the base until the next pull, and dropping them early would make the changes flicker
// out of view.
func (c *Client) push(ctx context.Context) error {
	var sent uint64
	for {
		c.mu.Lock()
		i, _ := slices.BinarySearchFunc(c.pending, sent+1, func(m wire.Mutation, seq uint64) int { return cmp.Compare(m.Seq, seq) })
		batch := slices.Clone(c.pending[i:min(len(c.pending), i+c.opts.PushBatch)])
		c.mu.Unlock()
		if len(batch) == 0 {
			return nil
		}

		resp, err := c.transport.Push(ctx, wire.PushRequest{ClientID: c.id, Mutations: batch})
		if err != nil {
			return err
		}
		for _, r := range resp.Rejected {
			if c.opts.OnReject != nil {
				c.opts.OnReject(r)
			}
		}
		if resp.LastSeq < batch[0].Seq {
			return fmt.Errorf("%w: sent %d, server is at %d", errNoProgress, batch[0].Seq, resp.LastSeq)
		}
		sent = resp.LastSeq
	}
}

// pull fetches pages until the server says there's no more, applying each one.
func (c *Client) pull(ctx context.Context) error {
	for {
		c.mu.Lock()
		since := c.cursor
		c.mu.Unlock()

		resp, err := c.transport.Pull(ctx, wire.PullRequest{ClientID: c.id, Since: since, Limit: c.opts.PullLimit})
		if err != nil {
			return err
		}
		c.apply(resp)
		if !resp.More {
			return nil
		}
	}
}

// apply moves the base forward by one pulled page and rebases pending mutations onto it.
func (c *Client) apply(resp wire.PullResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, rec := range resp.Records {
		if rec.Deleted {
			c.base.delete(rec.Collection, rec.ID)
		} else {
			c.base.put(rec)
		}
	}
	c.cursor = resp.Version
	if !resp.More {
		// LastSeq describes the data only on the last page.
		c.pending = slices.DeleteFunc(c.pending, func(m wire.Mutation) bool { return m.Seq <= resp.LastSeq })
	}
	c.rebase()
	if len(resp.Records) > 0 {
		c.notify()
	}
}

// rebase replays pending mutations on a fresh layer over the base. The caller holds c.mu.
func (c *Client) rebase() {
	c.view = newLayer(c.base)
	for _, m := range c.pending {
		// A pending mutation passed Validate, and the layer's methods can't fail.
		_ = wire.Apply(c.view, 0, m)
	}
}

type key struct{ collection, id string }

// table holds records by collection, then id. It never holds tombstones.
type table struct {
	collections map[string]map[string]wire.Record
}

func newTable() *table { return &table{collections: map[string]map[string]wire.Record{}} }

func (t *table) get(collection, id string) (wire.Record, bool) {
	rec, ok := t.collections[collection][id]
	return rec, ok
}

func (t *table) put(rec wire.Record) {
	recs := t.collections[rec.Collection]
	if recs == nil {
		recs = map[string]wire.Record{}
		t.collections[rec.Collection] = recs
	}
	recs[rec.ID] = rec.Clone()
}

func (t *table) delete(collection, id string) {
	delete(t.collections[collection], id)
}

// layer is a copy-on-write view over a table: reads fall through to the table, and writes
// stay in the layer, tombstones included.
type layer struct {
	base    *table
	changed map[key]wire.Record
}

func newLayer(base *table) *layer {
	return &layer{base: base, changed: map[key]wire.Record{}}
}

func (l *layer) Record(collection, id string) (wire.Record, bool, error) {
	if rec, ok := l.changed[key{collection, id}]; ok {
		return rec.Clone(), true, nil
	}
	rec, ok := l.base.get(collection, id)
	return rec.Clone(), ok, nil
}

func (l *layer) PutRecord(rec wire.Record) error {
	l.changed[key{rec.Collection, rec.ID}] = rec.Clone()
	return nil
}

func (l *layer) list(collection string) []wire.Record {
	var out []wire.Record
	for id, rec := range l.base.collections[collection] {
		if _, ok := l.changed[key{collection, id}]; !ok {
			out = append(out, rec.Clone())
		}
	}
	for k, rec := range l.changed {
		if k.collection == collection && !rec.Deleted {
			out = append(out, rec.Clone())
		}
	}
	slices.SortFunc(out, func(a, b wire.Record) int { return cmp.Compare(a.ID, b.ID) })
	return out
}
