package client

import (
	"path/filepath"
	"strings"
	"testing"
)

func openClient(t *testing.T, path string, tr Transport, opts Options) *Client {
	t.Helper()
	c, err := Open(path, tr, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestOfflineChangeSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.db")
	e := newEngine()

	c := openClient(t, path, &flaky{Transport: e, failPush: true}, Options{})
	id := c.ID()
	mustPut(t, c, "todos", "1", map[string]any{"title": "milk"})
	c.Close()

	c = openClient(t, path, e, Options{})
	if c.ID() != id {
		t.Fatalf("id = %q after restart, want %q", c.ID(), id)
	}
	if c.Pending() != 1 || fieldOf(t, c, "todos", "1", "title") != "milk" {
		t.Fatal("offline change was lost on restart")
	}
	mustSync(t, c)
	if c.Pending() != 0 {
		t.Fatal("restored change wasn't confirmed")
	}
}

func TestSyncedDataSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.db")
	e := newEngine()
	writer := New(e, Options{})
	mustPut(t, writer, "todos", "1", map[string]any{"title": "milk"})
	mustPut(t, writer, "todos", "2", map[string]any{"title": "eggs"})
	mustSync(t, writer)

	c := openClient(t, path, e, Options{})
	mustSync(t, c)
	c.Close()

	// The writer deletes one record while c is closed.
	if err := writer.Delete("todos", "2"); err != nil {
		t.Fatal(err)
	}
	mustSync(t, writer)

	f := &flaky{Transport: e, failPull: true}
	c = openClient(t, path, f, Options{})
	if c.Cursor() != 2 || len(c.List("todos")) != 2 {
		t.Fatalf("cursor %d with %d records after restart, want 2 and 2", c.Cursor(), len(c.List("todos")))
	}
	f.set(func(f *flaky) { f.failPull = false })
	mustSync(t, c)
	if got := c.List("todos"); len(got) != 1 || got[0].ID != "1" {
		t.Fatalf("records = %+v, want only todo 1", got)
	}

	// The delete is on disk too.
	c.Close()
	c = openClient(t, path, &flaky{Transport: e, failPull: true}, Options{})
	if len(c.List("todos")) != 1 {
		t.Fatal("delete wasn't saved")
	}
}

func TestSeqContinuesAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.db")
	e := newEngine()

	c := openClient(t, path, e, Options{})
	mustPut(t, c, "t", "1", map[string]any{"x": 1})
	mustSync(t, c) // seq 1 confirmed; nothing pending
	c.Close()

	// If the restarted client reused seq 1, the server would skip this as a duplicate.
	c = openClient(t, path, e, Options{})
	mustPut(t, c, "t", "2", map[string]any{"x": 2})
	mustSync(t, c)
	other := New(e, Options{})
	mustSync(t, other)
	if _, ok := other.Get("t", "2"); !ok {
		t.Fatal("the server skipped the first mutation after a restart")
	}
}

func TestOpenRefusesAnotherClientsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.db")
	openClient(t, path, newEngine(), Options{ID: "a"}).Close()

	if _, err := Open(path, newEngine(), Options{ID: "b"}); err == nil || !strings.Contains(err.Error(), `"a"`) {
		t.Fatalf("open with another id: err = %v", err)
	}
	if c, err := Open(path, newEngine(), Options{ID: "a"}); err != nil {
		t.Fatalf("open with the same id: %v", err)
	} else {
		c.Close()
	}
}
