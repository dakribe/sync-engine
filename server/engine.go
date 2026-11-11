// Package server applies client mutations in one global order and serves the result.
package server

import (
	"cmp"
	"encoding/json"
	"errors"
	"slices"
	"sync"

	"github.com/dakribe/sync-engine/wire"
)

// ErrNoClientID is returned when a request doesn't say which client sent it.
var ErrNoClientID = errors.New("request has no client id")

type recordKey struct {
	collection string
	id         string
}

// Engine holds the authoritative data. Every applied mutation gets the next version
// number, and each record remembers the version that last changed it, so a pull only has
// to find records newer than the client's cursor.
type Engine struct {
	mu      sync.Mutex
	version uint64
	records map[recordKey]*wire.Record
	lastSeq map[string]uint64 // per client: the last mutation applied
}

// NewEngine returns an empty engine at version 0.
func NewEngine() *Engine {
	return &Engine{
		records: make(map[recordKey]*wire.Record),
		lastSeq: make(map[string]uint64),
	}
}

// Push applies a client's mutations in order. Mutations the server has already applied
// are skipped, so retrying a push is safe. Processing stops at the first gap in sequence
// numbers, because applying a later mutation before an earlier one would break the
// client's order; the client resends from LastSeq+1. Invalid mutations are rejected but
// still count as processed.
func (e *Engine) Push(req wire.PushRequest) (wire.PushResponse, error) {
	if req.ClientID == "" {
		return wire.PushResponse{}, ErrNoClientID
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	var resp wire.PushResponse
	for _, m := range req.Mutations {
		last := e.lastSeq[req.ClientID]
		if m.Seq <= last {
			continue // already applied
		}
		if m.Seq != last+1 {
			break // gap: wait for the missing mutation
		}
		if err := m.Validate(); err != nil {
			resp.Rejected = append(resp.Rejected, wire.Rejection{Seq: m.Seq, Reason: err.Error()})
		} else {
			e.apply(m)
		}
		e.lastSeq[req.ClientID] = m.Seq
	}
	resp.LastSeq = e.lastSeq[req.ClientID]
	return resp, nil
}

// apply runs a valid mutation's ops under one new version. The caller holds e.mu.
func (e *Engine) apply(m wire.Mutation) {
	e.version++
	for _, op := range m.Ops {
		key := recordKey{op.Collection, op.ID}
		rec := e.records[key]
		switch op.Kind {
		case wire.OpPut:
			if rec == nil || rec.Deleted {
				// A put on a missing or deleted record starts it fresh.
				rec = &wire.Record{Collection: op.Collection, ID: op.ID, Fields: map[string]json.RawMessage{}}
				e.records[key] = rec
			}
			for name, value := range op.Fields {
				rec.Fields[name] = slices.Clone(value)
			}
		case wire.OpDelete:
			if rec == nil || rec.Deleted {
				continue
			}
			rec.Deleted = true
			rec.Fields = nil
		}
		rec.Version = e.version
	}
}

// Pull returns the current state of every record changed after req.Since, oldest change
// first. A record changed many times appears once, so a client that was offline for a
// long time downloads the data it needs, not its history.
func (e *Engine) Pull(req wire.PullRequest) (wire.PullResponse, error) {
	if req.ClientID == "" {
		return wire.PullResponse{}, ErrNoClientID
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	resp := wire.PullResponse{
		Version: e.version,
		LastSeq: e.lastSeq[req.ClientID],
		Records: []wire.Record{},
	}
	for _, rec := range e.records {
		if rec.Version > req.Since {
			resp.Records = append(resp.Records, cloneRecord(rec))
		}
	}
	slices.SortFunc(resp.Records, func(a, b wire.Record) int {
		return cmp.Or(
			cmp.Compare(a.Version, b.Version),
			cmp.Compare(a.Collection, b.Collection),
			cmp.Compare(a.ID, b.ID),
		)
	})
	return resp, nil
}

// cloneRecord copies a record so callers can't change the engine's data.
func cloneRecord(rec *wire.Record) wire.Record {
	out := *rec
	if rec.Fields != nil {
		out.Fields = make(map[string]json.RawMessage, len(rec.Fields))
		for name, value := range rec.Fields {
			out.Fields[name] = slices.Clone(value)
		}
	}
	return out
}
