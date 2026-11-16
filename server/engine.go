// Package server applies client mutations in one global order and serves the result.
package server

import (
	"cmp"
	"context"
	"errors"
	"slices"

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
	watch *broadcaster
}

// NewEngine returns an engine that keeps its data in s.
func NewEngine(s store.Store) *Engine {
	return &Engine{store: s, watch: newBroadcaster()}
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

	var (
		resp       wire.PushResponse
		newVersion uint64 // set if this push changed data
	)
	err := e.store.Update(ctx, func(tx store.Tx) error {
		resp, newVersion = wire.PushResponse{}, 0
		last, err := tx.LastSeq(req.ClientID)
		if err != nil {
			return err
		}
		version, err := tx.Version()
		if err != nil {
			return err
		}

		start, startVersion := last, version
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
				if err := wire.Apply(tx, version, m); err != nil {
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
		if version != startVersion {
			newVersion = version
		}
		return tx.SetLastSeq(req.ClientID, last)
	})
	if err != nil {
		return wire.PushResponse{}, err
	}
	if newVersion != 0 {
		e.watch.publish(newVersion)
	}
	return resp, nil
}

// Page sizes for Pull.
const (
	DefaultPullLimit = 1000
	MaxPullLimit     = 10000
)

// Pull returns the current state of the records changed after req.Since, oldest change
// first. A record changed many times appears once, so a client that was offline for a
// long time downloads the data it needs, not its history.
//
// Large results come in pages. A page never splits a mutation: it ends just before the
// mutation that would cross the limit, or, if a single mutation is bigger than a page,
// holds all of that mutation. So every page leaves the client with a state the server
// actually passed through.
func (e *Engine) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	if req.ClientID == "" {
		return wire.PullResponse{}, ErrNoClientID
	}
	limit := req.Limit
	if limit <= 0 {
		limit = DefaultPullLimit
	}
	limit = min(limit, MaxPullLimit)

	var resp wire.PullResponse
	err := e.store.View(ctx, func(tx store.Tx) error {
		current, err := tx.Version()
		if err != nil {
			return err
		}
		resp = wire.PullResponse{Version: current}
		if resp.LastSeq, err = tx.LastSeq(req.ClientID); err != nil {
			return err
		}
		if req.Since >= current {
			resp.Records = []wire.Record{}
			return nil
		}

		// Read one extra record to learn whether the page ends inside a mutation.
		recs, err := tx.Changes(req.Since, current, limit+1)
		if err != nil {
			return err
		}
		if len(recs) <= limit {
			resp.Records = recs
			return nil
		}

		last := recs[limit-1].Version
		if recs[limit].Version != last {
			resp.Records, resp.Version = recs[:limit], last
		} else if i := firstAtVersion(recs, last); i > 0 {
			// Stop before the mutation that crosses the limit.
			resp.Records, resp.Version = recs[:i], recs[i-1].Version
		} else {
			// One mutation is bigger than a page: send all of it.
			if resp.Records, err = tx.Changes(req.Since, last, 0); err != nil {
				return err
			}
			resp.Version = last
		}
		resp.More = resp.Version < current
		return nil
	})
	if err != nil {
		return wire.PullResponse{}, err
	}
	return resp, nil
}

// firstAtVersion returns the index of the first record with version v. recs is sorted by
// version and contains v.
func firstAtVersion(recs []wire.Record, v uint64) int {
	i, _ := slices.BinarySearchFunc(recs, v, func(r wire.Record, v uint64) int { return cmp.Compare(r.Version, v) })
	return i
}
