// Package server applies client mutations in one global order and serves the result.
package server

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"

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

	mu         sync.Mutex
	queue      []*pushCall // pushes waiting for the next commit
	committing bool        // a leader is committing a batch
}

// NewEngine returns an engine that keeps its data in s.
func NewEngine(s store.Store) *Engine {
	return &Engine{store: s, watch: newBroadcaster()}
}

// pushCall is one Push waiting in the queue.
type pushCall struct {
	req  wire.PushRequest
	resp wire.PushResponse
	err  error
	lead bool          // set before wake is closed if this call should commit the next batch
	wake chan struct{} // closed when resp and err are ready, or when lead is set
}

// Push applies a client's mutations in order. Mutations the server has already applied
// are skipped, so retrying a push is safe. Processing stops at the first gap in sequence
// numbers, because applying a later mutation before an earlier one would break the
// client's order; the client resends from LastSeq+1. Invalid mutations are rejected but
// still count as processed.
//
// Pushes that arrive together share one transaction (group commit). Every commit waits
// for the disk, so committing pushes one at a time caps throughput at one disk sync per
// push. Instead, the first push in becomes the leader and commits; pushes that arrive
// meanwhile queue up, and when the commit finishes the first of them leads the next
// commit for the whole queue. Under load, each commit carries many pushes for the price
// of one sync, and no caller waits for more than the commit in progress and its own.
func (e *Engine) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	if req.ClientID == "" {
		return wire.PushResponse{}, ErrNoClientID
	}

	call := &pushCall{req: req, wake: make(chan struct{})}
	e.mu.Lock()
	e.queue = append(e.queue, call)
	if e.committing {
		e.mu.Unlock()
		<-call.wake
		if !call.lead {
			return call.resp, call.err
		}
		e.mu.Lock()
	}
	e.committing = true
	batch := e.queue
	e.queue = nil
	e.mu.Unlock()

	// The batch holds other callers' pushes, so the leader going away mustn't cancel it.
	e.commit(context.WithoutCancel(ctx), batch)

	e.mu.Lock()
	if len(e.queue) > 0 {
		next := e.queue[0]
		next.lead = true
		close(next.wake)
	} else {
		e.committing = false
	}
	e.mu.Unlock()

	for _, c := range batch {
		if c != call {
			close(c.wake)
		}
	}
	return call.resp, call.err
}

// commit applies a batch of pushes in one transaction, in queue order, and fills in each
// call's result.
func (e *Engine) commit(ctx context.Context, batch []*pushCall) {
	var startVersion, endVersion uint64
	err := e.store.Update(ctx, func(tx store.Tx) error {
		version, err := tx.Version()
		if err != nil {
			return err
		}
		startVersion = version
		for _, c := range batch {
			if c.resp, version, err = pushTx(tx, c.req, version); err != nil {
				return err
			}
		}
		endVersion = version
		if version != startVersion {
			return tx.SetVersion(version)
		}
		return nil
	})
	if err != nil {
		for _, c := range batch {
			c.resp, c.err = wire.PushResponse{}, err
		}
		return
	}
	if endVersion != startVersion {
		e.watch.publish(endVersion)
	}
}

// pushTx applies one push inside a transaction, starting from version, and returns the
// version after it.
func pushTx(tx store.Tx, req wire.PushRequest, version uint64) (wire.PushResponse, uint64, error) {
	var resp wire.PushResponse
	last, err := tx.LastSeq(req.ClientID)
	if err != nil {
		return resp, version, err
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
			if err := wire.Apply(tx, version, m); err != nil {
				return resp, version, err
			}
		}
		last = m.Seq
	}
	resp.LastSeq = last
	if last != start {
		if err := tx.SetLastSeq(req.ClientID, last); err != nil {
			return resp, version, err
		}
	}
	return resp, version, nil
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
// holds all of that mutation. So the version a page ends at is an exact cursor: every
// change up to it has been sent. Clients must apply a pull's pages together, though: a
// record that changes again while a client is paging moves to a later page.
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
