package client

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dakribe/sync-engine/wire"
)

// withBase returns an offline client whose base holds n records.
func withBase(b *testing.B, n int) *Client {
	b.Helper()
	c := New(&flaky{Transport: newEngine(), failPush: true, failPull: true}, Options{})
	recs := make([]wire.Record, n)
	for i := range recs {
		recs[i] = wire.Record{Collection: "todos", ID: fmt.Sprint(i), Version: uint64(i + 1),
			Fields: map[string]json.RawMessage{"title": json.RawMessage(`"buy milk"`)}}
	}
	if err := c.apply(recs, uint64(n), 0); err != nil {
		b.Fatal(err)
	}
	return c
}

func BenchmarkMutate(b *testing.B) {
	c := withBase(b, 1000)
	fields := map[string]any{"done": true}
	i := 0
	for b.Loop() {
		if err := c.Put("todos", fmt.Sprint(i%1000), fields); err != nil {
			b.Fatal(err)
		}
		i++
		if i%1000 == 0 { // keep the pending queue from growing without bound
			c.pending = c.pending[:0]
			c.rebase()
		}
	}
}

func BenchmarkMutateOnDisk(b *testing.B) {
	c, err := Open(filepath.Join(b.TempDir(), "client.db"), &flaky{Transport: newEngine(), failPush: true, failPull: true}, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	fields := map[string]any{"done": true}
	i := 0
	for b.Loop() {
		if err := c.Put("todos", fmt.Sprint(i%1000), fields); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkGet(b *testing.B) {
	c := withBase(b, 100_000)
	i := 0
	for b.Loop() {
		if _, ok := c.Get("todos", fmt.Sprint(i%100_000)); !ok {
			b.Fatal("missing")
		}
		i++
	}
}

// BenchmarkRebase replays 10 pending mutations after a pull. The cost should not depend
// on how many records the client holds.
func BenchmarkRebase(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("records=%d", n), func(b *testing.B) {
			c := withBase(b, n)
			for i := range 10 {
				if err := c.Put("todos", fmt.Sprint(i), map[string]any{"done": true}); err != nil {
					b.Fatal(err)
				}
			}
			for b.Loop() {
				c.mu.Lock()
				c.rebase()
				c.mu.Unlock()
			}
		})
	}
}
