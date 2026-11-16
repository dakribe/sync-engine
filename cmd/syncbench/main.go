// Command syncbench measures the whole system over real HTTP: how long one client's write
// takes to show on every other client, and how many writes a second the server sustains
// when every client writes at once.
//
// By default it starts a server in-process on a temporary SQLite file. Pass -url to
// measure a running syncd instead.
//
//	syncbench -clients 50 -writes 200
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dakribe/sync-engine/client"
	"github.com/dakribe/sync-engine/server"
	"github.com/dakribe/sync-engine/store/sqlite"
	"github.com/dakribe/sync-engine/wire"
)

func main() {
	url := flag.String("url", "", "server to measure; empty starts one in-process")
	clients := flag.Int("clients", 50, "number of clients")
	writes := flag.Int("writes", 200, "writes for the latency test")
	burst := flag.Int("burst", 100, "writes per client for the throughput test")
	flag.Parse()

	if err := run(*url, *clients, *writes, *burst); err != nil {
		fmt.Fprintln(os.Stderr, "syncbench:", err)
		os.Exit(1)
	}
}

func run(url string, n, writes, burst int) error {
	if n < 2 {
		return fmt.Errorf("need at least 2 clients")
	}
	if url == "" {
		var stop func()
		var err error
		if url, stop, err = startServer(); err != nil {
			return err
		}
		defer stop()
	}

	// Every client holds an event stream open, so allow plenty of connections.
	hc := &http.Client{Transport: &http.Transport{MaxIdleConns: 4 * n, MaxIdleConnsPerHost: 4 * n}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := make([]*client.Client, n)
	for i := range cs {
		cs[i] = client.New(&client.HTTPTransport{BaseURL: url, Client: hc}, client.Options{PollInterval: time.Hour})
		go cs[i].Run(ctx)
	}
	time.Sleep(500 * time.Millisecond) // let event streams connect

	fmt.Printf("%d clients, server %s\n\n", n, url)
	if err := latency(cs, writes); err != nil {
		return err
	}
	fmt.Println()
	return throughput(cs, burst)
}

// latency has client 0 write a counter, one write at a time, and times how long each
// write takes to show on every other client.
func latency(cs []*client.Client, writes int) error {
	var (
		mu      sync.Mutex
		sentAt  = map[int]time.Time{}
		samples []time.Duration
		seen    = make([]atomic.Int64, len(cs))
	)
	for i, c := range cs[1:] {
		changes, stop := c.Subscribe()
		defer stop()
		go func() {
			for range changes {
				rec, ok := c.Get("bench", "probe")
				if !ok {
					continue
				}
				var v int
				json.Unmarshal(rec.Fields["n"], &v)
				if prev := seen[i+1].Load(); int64(v) > prev {
					seen[i+1].Store(int64(v))
					mu.Lock()
					samples = append(samples, time.Since(sentAt[v]))
					mu.Unlock()
				}
			}
		}()
	}

	for v := 1; v <= writes; v++ {
		mu.Lock()
		sentAt[v] = time.Now()
		mu.Unlock()
		if err := cs[0].Put("bench", "probe", map[string]any{"n": v}); err != nil {
			return err
		}
		err := waitFor(10*time.Second, func() bool {
			for i := 1; i < len(cs); i++ {
				if seen[i].Load() < int64(v) {
					return false
				}
			}
			return true
		})
		if err != nil {
			return fmt.Errorf("write %d didn't reach every client: %w", v, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	slices.Sort(samples)
	pct := func(p float64) time.Duration { return samples[int(p*float64(len(samples)-1))] }
	fmt.Printf("Write on one client to visible on all %d others (%d writes, %d samples)\n", len(cs)-1, writes, len(samples))
	fmt.Printf("  p50 %v   p95 %v   p99 %v   max %v\n", round(pct(0.5)), round(pct(0.95)), round(pct(0.99)), round(samples[len(samples)-1]))
	return nil
}

// throughput has every client write burst records at once, then waits until every client
// holds every record.
func throughput(cs []*client.Client, burst int) error {
	start := time.Now()
	var wg sync.WaitGroup
	for i, c := range cs {
		wg.Go(func() {
			for j := range burst {
				c.Mutate(wire.Op{Kind: wire.OpPut, Collection: "load", ID: fmt.Sprintf("%d-%d", i, j),
					Fields: map[string]json.RawMessage{"n": json.RawMessage(fmt.Sprint(j))}})
			}
		})
	}
	wg.Wait()

	total := len(cs) * burst
	var pushed time.Duration
	err := waitFor(60*time.Second, func() bool {
		for _, c := range cs {
			if c.Pending() != 0 {
				return false
			}
		}
		if pushed == 0 {
			pushed = time.Since(start)
		}
		for _, c := range cs {
			if len(c.List("load")) != total {
				return false
			}
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("clients didn't converge: %w", err)
	}
	converged := time.Since(start)
	fmt.Printf("All %d clients write %d records each at once (%d writes)\n", len(cs), burst, total)
	fmt.Printf("  every write confirmed in %v (%.0f writes/s)\n", round(pushed), float64(total)/pushed.Seconds())
	fmt.Printf("  every client holds all %d records after %v\n", total, round(converged))
	return nil
}

func startServer() (string, func(), error) {
	dir, err := os.MkdirTemp("", "syncbench")
	if err != nil {
		return "", nil, err
	}
	st, err := sqlite.Open(filepath.Join(dir, "sync.db"))
	if err != nil {
		return "", nil, err
	}
	engine := server.NewEngine(st)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: server.NewHandler(engine, slog.New(slog.DiscardHandler))}
	srv.RegisterOnShutdown(engine.Close)
	go srv.Serve(ln)
	return "http://" + ln.Addr().String(), func() {
		srv.Shutdown(context.Background())
		st.Close()
		os.RemoveAll(dir)
	}, nil
}

func waitFor(timeout time.Duration, done func() bool) error {
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %v", timeout)
		}
		time.Sleep(200 * time.Microsecond)
	}
	return nil
}

func round(d time.Duration) time.Duration {
	switch {
	case d > time.Second:
		return d.Round(time.Millisecond)
	case d > time.Millisecond:
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}
