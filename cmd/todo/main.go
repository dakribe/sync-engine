// Command todo is a small terminal to-do list built on the sync engine. Run it in two
// terminals against one syncd and changes show in both as they happen. Go offline in
// one, keep editing, and watch the edits merge when it comes back.
//
//	syncd &
//	todo -db alice.db
//	todo -db bob.db
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dakribe/sync-engine/client"
	"github.com/dakribe/sync-engine/wire"
)

const collection = "todos"

func main() {
	serverURL := flag.String("server", "http://localhost:8080", "sync server")
	dbPath := flag.String("db", "todo.db", "local database file")
	flag.Parse()

	if err := run(*serverURL, *dbPath); err != nil {
		fmt.Fprintln(os.Stderr, "todo:", err)
		os.Exit(1)
	}
}

type app struct {
	c       *client.Client
	link    *switchable
	printMu sync.Mutex
}

func run(serverURL, dbPath string) error {
	link := &switchable{Transport: &client.HTTPTransport{BaseURL: serverURL}}
	c, err := client.Open(dbPath, link, client.Options{
		MaxBackoff:  2 * time.Second, // come back quickly after "online"
		OnSyncError: func(error) {},  // the status line shows when we're offline
	})
	if err != nil {
		return err
	}
	defer c.Close()

	a := &app{c: c, link: link}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	// Redraw when data changes, including changes from other clients.
	changes, stop := c.Subscribe()
	defer stop()
	go func() {
		for range changes {
			a.render()
		}
	}()

	fmt.Println("Commands: add <title>, done <n>, rename <n> <title>, rm <n>, offline, online, quit")
	a.render()

	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		quit, err := a.command(strings.TrimSpace(in.Text()))
		if err != nil {
			a.print("error: %v\n", err)
		}
		if quit {
			break
		}
	}
	return in.Err()
}

func (a *app) command(line string) (quit bool, err error) {
	verb, rest, _ := strings.Cut(line, " ")
	switch verb {
	case "":
		a.render()
	case "add":
		if rest == "" {
			return false, errors.New("usage: add <title>")
		}
		return false, a.c.Put(collection, newID(), map[string]any{"title": rest, "done": false})
	case "done":
		rec, err := a.pick(rest)
		if err != nil {
			return false, err
		}
		var done bool
		json.Unmarshal(rec.Fields["done"], &done)
		return false, a.c.Put(collection, rec.ID, map[string]any{"done": !done})
	case "rename":
		n, title, _ := strings.Cut(rest, " ")
		rec, err := a.pick(n)
		if err != nil {
			return false, err
		}
		if title == "" {
			return false, errors.New("usage: rename <n> <title>")
		}
		return false, a.c.Put(collection, rec.ID, map[string]any{"title": title})
	case "rm":
		rec, err := a.pick(rest)
		if err != nil {
			return false, err
		}
		return false, a.c.Delete(collection, rec.ID)
	case "offline":
		a.link.offline.Store(true)
		a.render()
	case "online":
		a.link.offline.Store(false)
		a.c.Poke()
		a.render()
	case "quit", "exit":
		return true, nil
	default:
		return false, fmt.Errorf("unknown command %q", verb)
	}
	return false, nil
}

// pick returns the to-do shown as number n.
func (a *app) pick(n string) (wire.Record, error) {
	i, err := strconv.Atoi(strings.TrimSpace(n))
	todos := a.c.List(collection)
	if err != nil || i < 1 || i > len(todos) {
		return wire.Record{}, fmt.Errorf("no to-do %q", n)
	}
	return todos[i-1], nil
}

func (a *app) render() {
	var b strings.Builder
	todos := a.c.List(collection)
	b.WriteString("\n")
	if len(todos) == 0 {
		b.WriteString("  (nothing to do)\n")
	}
	for i, rec := range todos {
		var title string
		var done bool
		json.Unmarshal(rec.Fields["title"], &title)
		json.Unmarshal(rec.Fields["done"], &done)
		box := "[ ]"
		if done {
			box = "[x]"
		}
		fmt.Fprintf(&b, "  %d. %s %s\n", i+1, box, title)
	}

	status := "online"
	if a.link.offline.Load() {
		status = "offline"
	}
	if n := a.c.Pending(); n > 0 {
		status += fmt.Sprintf(", %d change(s) not yet synced", n)
	} else {
		status += ", synced"
	}
	fmt.Fprintf(&b, "  -- %s --\n> ", status)
	a.print("%s", b.String())
}

func (a *app) print(format string, args ...any) {
	a.printMu.Lock()
	defer a.printMu.Unlock()
	fmt.Printf(format, args...)
}

// newID returns an id that sorts by creation time, so the list keeps its order.
func newID() string {
	return fmt.Sprintf("%016x-%s", time.Now().UnixNano(), rand.Text()[:6])
}

// switchable is a transport that can be switched off to simulate losing the network.
type switchable struct {
	client.Transport
	offline atomic.Bool
}

var errOffline = errors.New("offline")

func (s *switchable) Push(ctx context.Context, req wire.PushRequest) (wire.PushResponse, error) {
	if s.offline.Load() {
		return wire.PushResponse{}, errOffline
	}
	return s.Transport.Push(ctx, req)
}

func (s *switchable) Pull(ctx context.Context, req wire.PullRequest) (wire.PullResponse, error) {
	if s.offline.Load() {
		return wire.PullResponse{}, errOffline
	}
	return s.Transport.Pull(ctx, req)
}

// Watch holds the event stream open while online, and drops it on going offline.
func (s *switchable) Watch(ctx context.Context, fn func(uint64)) error {
	if s.offline.Load() {
		return errOffline
	}
	w, ok := s.Transport.(client.Watcher)
	if !ok {
		return errors.New("transport has no live updates")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for !s.offline.Load() && ctx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
		}
		cancel()
	}()
	return w.Watch(ctx, fn)
}
