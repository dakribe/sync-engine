package client

import (
	"context"
	"math/rand/v2"
	"time"
)

// Defaults for Run.
const (
	DefaultPollInterval = 30 * time.Second
	DefaultMinBackoff   = 250 * time.Millisecond
	DefaultMaxBackoff   = 30 * time.Second
)

// Run syncs in the background until ctx is done: right after a local change, when Poke is
// called, and every PollInterval. While syncs fail it waits longer between tries, doubling
// from MinBackoff up to MaxBackoff with random jitter, so a server coming back from an
// outage isn't hit by every client at once.
//
// Changes made during a sync are folded into one more sync right after it, so a burst of
// writes costs a couple of round trips, not one each.
func (c *Client) Run(ctx context.Context) error {
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
		if err != nil {
			// Don't retry on every keystroke while the server is unreachable.
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		case <-c.wake:
			timer.Stop()
		}
	}
}

// Poke asks Run to sync soon, for example because the server said it has new data.
func (c *Client) Poke() {
	select {
	case c.wake <- struct{}{}:
	default: // a sync is already due
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
