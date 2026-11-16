package client

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunSyncsAfterLocalChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine()
		a, b := New(e, Options{}), New(e, Options{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go a.Run(ctx)
		synctest.Wait() // a's first sync is done and it's waiting

		mustPut(t, a, "t", "1", map[string]any{"x": 1})
		synctest.Wait()
		if a.Pending() != 0 {
			t.Fatal("Run didn't push the change")
		}
		mustSync(t, b)
		if _, ok := b.Get("t", "1"); !ok {
			t.Fatal("change didn't reach the server")
		}
	})
}

func TestRunPollsForRemoteChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine()
		// flaky hides the engine's Watch, so a has no live updates and must poll.
		a, b := New(&flaky{Transport: e}, Options{PollInterval: time.Minute}), New(e, Options{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go a.Run(ctx)
		synctest.Wait()

		mustPut(t, b, "t", "1", map[string]any{"x": 1})
		mustSync(t, b)
		time.Sleep(59 * time.Second)
		synctest.Wait()
		if _, ok := a.Get("t", "1"); ok {
			t.Fatal("a pulled before the poll interval")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if _, ok := a.Get("t", "1"); !ok {
			t.Fatal("a didn't pull after the poll interval")
		}
	})
}

func TestRunBacksOffWhileOffline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &flaky{Transport: newEngine(), failPush: true}
		var errs int
		c := New(f, Options{MinBackoff: time.Second, MaxBackoff: 8 * time.Second, OnSyncError: func(error) { errs++ }})
		mustPut(t, c, "t", "1", map[string]any{"x": 1})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go c.Run(ctx)

		// Waits are about 1, 2, 4, 8, 8... seconds, each at least half that.
		time.Sleep(60 * time.Second)
		synctest.Wait()
		if errs < 8 || errs > 16 {
			t.Fatalf("%d tries in a minute offline, want 8 to 16", errs)
		}

		// Local changes while offline don't cause extra tries.
		before := f.pushes
		for range 50 {
			mustPut(t, c, "t", "1", map[string]any{"x": 2})
		}
		synctest.Wait()
		if f.pushes != before {
			t.Fatalf("writes while offline caused %d extra pushes", f.pushes-before)
		}

		f.set(func(f *flaky) { f.failPush = false })
		time.Sleep(8 * time.Second)
		synctest.Wait()
		if c.Pending() != 0 {
			t.Fatalf("%d pending after coming back online", c.Pending())
		}
	})
}

func TestRunStopsWithContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New(newEngine(), Options{})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error)
		go func() { done <- c.Run(ctx) }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	})
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	c := New(newEngine(), Options{MinBackoff: 100 * time.Millisecond, MaxBackoff: time.Second})
	for failures, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 4: 800 * time.Millisecond, 10: time.Second} {
		for range 100 {
			if got := c.backoff(failures); got < want/2 || got > want {
				t.Fatalf("backoff(%d) = %v, want between %v and %v", failures, got, want/2, want)
			}
		}
	}
}

func TestRunPullsAsSoonAsServerAnnouncesAChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine()
		a, b := New(e, Options{PollInterval: time.Hour}), New(e, Options{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go a.Run(ctx)
		synctest.Wait()

		mustPut(t, b, "t", "1", map[string]any{"x": 1})
		mustSync(t, b)
		synctest.Wait() // no time passes: only the announcement can wake a
		if _, ok := a.Get("t", "1"); !ok {
			t.Fatal("a didn't pull after the server announced a change")
		}
	})
}

func TestWatchCatchesUpAfterReconnecting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine()
		a := New(e, Options{PollInterval: time.Hour, MinBackoff: time.Second})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go a.Run(ctx)
		synctest.Wait()

		// Closing the engine drops a's connection, like a server restart. b's change
		// lands while a is disconnected, so no announcement reaches a.
		e.Close()
		synctest.Wait()
		b := New(e, Options{})
		mustPut(t, b, "t", "1", map[string]any{"x": 1})
		mustSync(t, b)
		synctest.Wait()
		if _, ok := a.Get("t", "1"); ok {
			t.Fatal("a learned of the change while disconnected")
		}

		// On reconnecting, the first thing a hears is the current version, so it catches up.
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if _, ok := a.Get("t", "1"); !ok {
			t.Fatal("a didn't catch up after reconnecting")
		}
	})
}
