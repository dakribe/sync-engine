// Package memory is a store that keeps everything in memory. It's for tests and for
// running the server without a database; data is lost when the process exits.
package memory

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/wire"
)

type key struct{ collection, id string }

// Store is an in-memory store.Store.
type Store struct {
	mu      sync.RWMutex
	version uint64
	lastSeq map[string]uint64
	records map[key]wire.Record
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{lastSeq: map[string]uint64{}, records: map[key]wire.Record{}}
}

// Update runs fn with writes staged in the transaction, and keeps them only if fn
// succeeds.
func (s *Store) Update(ctx context.Context, fn func(store.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := &tx{s: s, writable: true, version: s.version, lastSeq: map[string]uint64{}, records: map[key]wire.Record{}}
	if err := fn(tx); err != nil {
		return err
	}
	s.version = tx.version
	for client, seq := range tx.lastSeq {
		s.lastSeq[client] = seq
	}
	for k, rec := range tx.records {
		s.records[k] = rec
	}
	return nil
}

// View runs fn under a read lock, so it sees no partial Update.
func (s *Store) View(ctx context.Context, fn func(store.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(&tx{s: s, version: s.version})
}

// Close does nothing.
func (s *Store) Close() error { return nil }

// tx reads through its staged writes to the store underneath.
type tx struct {
	s        *Store
	writable bool
	version  uint64
	lastSeq  map[string]uint64
	records  map[key]wire.Record
}

func (t *tx) Version() (uint64, error) { return t.version, nil }

func (t *tx) SetVersion(v uint64) error {
	if !t.writable {
		return errReadOnly
	}
	t.version = v
	return nil
}

func (t *tx) LastSeq(clientID string) (uint64, error) {
	if seq, ok := t.lastSeq[clientID]; ok {
		return seq, nil
	}
	return t.s.lastSeq[clientID], nil
}

func (t *tx) SetLastSeq(clientID string, seq uint64) error {
	if !t.writable {
		return errReadOnly
	}
	t.lastSeq[clientID] = seq
	return nil
}

func (t *tx) Record(collection, id string) (wire.Record, bool, error) {
	k := key{collection, id}
	rec, ok := t.records[k]
	if !ok {
		rec, ok = t.s.records[k]
	}
	if !ok {
		return wire.Record{}, false, nil
	}
	return clone(rec), true, nil
}

func (t *tx) PutRecord(rec wire.Record) error {
	if !t.writable {
		return errReadOnly
	}
	t.records[key{rec.Collection, rec.ID}] = clone(rec)
	return nil
}

func (t *tx) ChangedSince(v uint64) ([]wire.Record, error) {
	out := []wire.Record{}
	for k, rec := range t.s.records {
		if _, staged := t.records[k]; !staged && rec.Version > v {
			out = append(out, clone(rec))
		}
	}
	for _, rec := range t.records {
		if rec.Version > v {
			out = append(out, clone(rec))
		}
	}
	slices.SortFunc(out, func(a, b wire.Record) int {
		return cmp.Or(
			cmp.Compare(a.Version, b.Version),
			cmp.Compare(a.Collection, b.Collection),
			cmp.Compare(a.ID, b.ID),
		)
	})
	return out, nil
}

var errReadOnly = errors.New("memory: write in a read-only transaction")

func clone(rec wire.Record) wire.Record {
	if rec.Fields != nil {
		fields := make(map[string]json.RawMessage, len(rec.Fields))
		for name, value := range rec.Fields {
			fields[name] = slices.Clone(value)
		}
		rec.Fields = fields
	}
	return rec
}
