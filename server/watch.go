package server

import (
	"context"
	"errors"
	"sync"

	"github.com/dakribe/sync-engine/store"
)

// ErrClosed is returned by Watch after the engine is closed.
var ErrClosed = errors.New("engine closed")

// Version returns the version of the last applied mutation.
func (e *Engine) Version(ctx context.Context) (uint64, error) {
	var v uint64
	err := e.store.View(ctx, func(tx store.Tx) error {
		var err error
		v, err = tx.Version()
		return err
	})
	return v, err
}

// Subscribe returns a channel that receives the new version after each push that changes
// data. A slow reader doesn't hold up pushes: it skips to the latest version, which is all
// it needs to know to pull. The channel is closed when the engine closes. Call the
// returned function to stop.
func (e *Engine) Subscribe() (<-chan uint64, func()) {
	return e.watch.subscribe()
}

// Watch calls fn with the current version, then with each new one, until ctx is done or
// the engine closes. It lets an in-process client get live updates the same way a remote
// one does over HTTP.
func (e *Engine) Watch(ctx context.Context, fn func(version uint64)) error {
	ch, stop := e.Subscribe()
	defer stop()

	v, err := e.Version(ctx)
	if err != nil {
		return err
	}
	fn(v)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return ErrClosed
			}
			fn(v)
		}
	}
}

// Close ends every subscription, so open event streams finish and a server can shut
// down. The engine still serves pushes and pulls.
func (e *Engine) Close() {
	e.watch.close()
}

// broadcaster sends the latest version to every subscriber without blocking.
type broadcaster struct {
	mu     sync.Mutex
	subs   map[chan uint64]struct{}
	closed bool
}

func newBroadcaster() *broadcaster {
	return &broadcaster{subs: map[chan uint64]struct{}{}}
}

func (b *broadcaster) subscribe() (<-chan uint64, func()) {
	ch := make(chan uint64, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		close(ch)
		return ch, func() {}
	}
	b.subs[ch] = struct{}{}
	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
	}
}

func (b *broadcaster) publish(v uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		// Replace an unread version with the newer one. Versions can arrive out of order
		// from concurrent pushes, so never replace a newer one with an older one.
		send := v
		select {
		case old := <-ch:
			send = max(send, old)
		default:
		}
		ch <- send
	}
}

func (b *broadcaster) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for ch := range b.subs {
		delete(b.subs, ch)
		close(ch)
	}
}
