// Package server applies client mutations in one global order and serves the result.
package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/wire"
)

// ErrNoClientID is returned when a request doesn't say which client sent it.
var ErrNoClientID = errors.New("request has no client id")

// Engine holds the authoritative data. Every applied mutation gets the next version
// number, and each record remembers the version that last changed it, so a pull only has
// to find records newer than the client's cursor.
type Engine struct {
	store store.Store
}

// NewEngine returns an engine that keeps its data in s.
func NewEngine(s store.Store) *Engine {
	return &Engine{store: s}
}

// Push applies a client's mutations in order, in one transaction. Mutations the server
// has already applied are skipped, so retrying a push is safe. Processing stops at the
// first gap in sequence numbers, because applying a later mutation before an earlier one
// would break the client's order; the client resends from LastSeq+1. Invalid mutations are
// rejected but still count as processed.
func (e *Engine) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	if req.ClientID == "" {
		return wire.PushResponse{}, ErrNoClientID
	}

	var resp wire.PushResponse
	err := e.store.Update(ctx, func(tx store.Tx) error {
		resp = wire.PushResponse{}
		last, err := tx.LastSeq(req.ClientID)
		if err != nil {
			return err
		}
		version, err := tx.Version()
		if err != nil {
			return err
		}

		start := last
		for _, m := range req.Mutations {
			if m.Seq <= last {
				continue // already applied
			}
			if m.Seq != last+1 {
				break // gap: wait for the missing mutation
			}
			if err := m.Validate(); err != nil {
				resp.Rejected = append(resp.Rejected, wire.Rejection{Seq: m.Seq, Reason: err.Error()})
			} else {
				version++
				if err := apply(tx, version, m); err != nil {
					return err
				}
			}
			last = m.Seq
		}
		resp.LastSeq = last

		if last == start {
			return nil // nothing new
		}
		if err := tx.SetVersion(version); err != nil {
			return err
		}
		return tx.SetLastSeq(req.ClientID, last)
	})
	if err != nil {
		return wire.PushResponse{}, err
	}
	return resp, nil
}

// apply runs a valid mutation's ops under the given version.
func apply(tx store.Tx, version uint64, m wire.Mutation) error {
	for _, op := range m.Ops {
		rec, ok, err := tx.Record(op.Collection, op.ID)
		if err != nil {
			return err
		}
		switch op.Kind {
		case wire.OpPut:
			if !ok || rec.Deleted {
				// A put on a missing or deleted record starts it fresh.
				rec = wire.Record{Collection: op.Collection, ID: op.ID, Fields: map[string]json.RawMessage{}}
			}
			for name, value := range op.Fields {
				rec.Fields[name] = value
			}
		case wire.OpDelete:
			if !ok || rec.Deleted {
				continue
			}
			rec.Deleted = true
			rec.Fields = nil
		}
		rec.Version = version
		if err := tx.PutRecord(rec); err != nil {
			return err
		}
	}
	return nil
}

// Pull returns the current state of every record changed after req.Since, oldest change
// first. A record changed many times appears once, so a client that was offline for a
// long time downloads the data it needs, not its history.
func (e *Engine) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	if req.ClientID == "" {
		return wire.PullResponse{}, ErrNoClientID
	}

	var resp wire.PullResponse
	err := e.store.View(ctx, func(tx store.Tx) error {
		var err error
		if resp.Version, err = tx.Version(); err != nil {
			return err
		}
		if resp.LastSeq, err = tx.LastSeq(req.ClientID); err != nil {
			return err
		}
		resp.Records, err = tx.ChangedSince(req.Since)
		return err
	})
	if err != nil {
		return wire.PullResponse{}, err
	}
	return resp, nil
}
