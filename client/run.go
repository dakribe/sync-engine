package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// Defaults for Run.
const (
	DefaultPollInterval = 30 * time.Second
	DefaultMinBackoff   = 250 * time.Millisecond
	DefaultMaxBackoff   = 30 * time.Second
)

// Watcher is a Transport that can also announce new server versions as they happen.
// HTTPTransport and *server.Engine are Watchers.
type Watcher interface {
	Watch(ctx context.Context, fn func(version uint64)) error
}

// Run syncs in the background until ctx is done: right after a local change, when the
// server announces new data, and every PollInterval. If the transport is a Watcher, Run
// keeps a live connection open and pulls as soon as another client's change lands,
// reconnecting with backoff when the connection drops.
//
// While syncs fail it waits longer between tries, doubling from MinBackoff up to
// MaxBackoff with random jitter, so a server coming back from an outage isn't hit by every
// client at once.
//
// Changes made during a sync are folded into one more sync right after it, so a burst of
// writes costs a couple of round trips, not one each.
func (c *Client) Run(ctx context.Context) error {
	if w, ok := c.transport.(Watcher); ok {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go c.watch(ctx, w)
	}

	var failures int
	for {
		err := c.Sync(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var wait time.Duration
		if err != nil {
			failures++
			wait = c.backoff(failures)
			if c.opts.OnSyncError != nil {
				c.opts.OnSyncError(err)
			}
		} else {
			failures = 0
			wait = c.opts.PollInterval
		}

		timer := time.NewTimer(wait)
		// While syncs fail, local changes don't cut the wait short, so a client offline
		// doesn't retry on every keystroke. A poke does: it means the server is back.
		local := c.wake
		if err != nil {
			local = nil
		}
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		case <-local:
			timer.Stop()
		case <-c.poke:
			timer.Stop()
		}
	}
}

// Poke asks Run to sync now, for example because the server said it has new data.
func (c *Client) Poke() { signal(c.poke) }

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default: // a sync is already due
	}
}

// watch holds the live connection open, poking Run when the server is ahead of the
// client's cursor.
func (c *Client) watch(ctx context.Context, w Watcher) {
	var failures int
	for {
		err := w.Watch(ctx, func(version uint64) {
			failures = 0 // connected
			if version > c.Cursor() {
				c.Poke()
			}
		})
		if ctx.Err() != nil {
			return
		}
		failures++
		if c.opts.OnSyncError != nil && err != nil {
			c.opts.OnSyncError(fmt.Errorf("watch: %w", err))
		}
		timer := time.NewTimer(c.backoff(failures))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// backoff returns the wait after the given number of failures in a row: half the capped
// exponential delay, plus a random part up to the other half.
func (c *Client) backoff(failures int) time.Duration {
	d := c.opts.MinBackoff
	for i := 1; i < failures && d < c.opts.MaxBackoff; i++ {
		d *= 2
	}
	d = min(d, c.opts.MaxBackoff)
	return d/2 + rand.N(d/2+1)
}
