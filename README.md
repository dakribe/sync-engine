# sync-engine

A local-first sync engine in Go. Clients keep a local copy of the data and write to it
directly, so changes show up instantly and work offline. Changes get queued and pushed to a
server, which orders everyone's changes and sends them back out.

Mostly modeled on how Replicache works.

## How it works

- Each client numbers its mutations 1, 2, 3... and pushes them to the server. The server
  remembers the last one it applied per client, so retries don't apply anything twice.
- Pulls return the current state of every record that changed since the client's cursor
  (not the full history). Deletes come back as tombstones. Big pulls are paged.
- The client keeps the server's data as a base layer, with its unconfirmed mutations
  applied on top. After a pull it drops whatever the server confirmed and replays the rest.
- Conflicts: server order wins, per field. A put only touches the fields it names.
- The server sends a small "poke" over server-sent events when data changes, and clients
  pull when they get one.
- Server storage is SQLite. Concurrent pushes get batched into one transaction (group commit).
- The client can optionally save its state to a SQLite file so offline changes survive a restart.

## Running it

```sh
go run ./cmd/syncd -db ""        # server, in-memory
go run ./cmd/todo -db alice.db   # demo todo app, run a couple of these
go run ./cmd/todo -db bob.db
```

In the todo app: `add <title>`, `done <n>`, `rename <n> <title>`, `rm <n>`, `offline`, `online`.

Using the client:

```go
c, err := client.Open("todos.db", &client.HTTPTransport{BaseURL: "http://localhost:8080"}, client.Options{})
c.Put("todos", "1", map[string]any{"title": "milk"})
c.List("todos")
go c.Run(ctx) // syncs in the background
```

## Tests

```sh
go test ./...
go test -race ./...
go test ./sim -run 'TestSimulation$' -seeds 2000   # longer run
go test ./sim -run 'TestSimulation$' -seed 42 -v   # replay a seed
```

`sim/` has a deterministic simulation: a few clients make random edits over a fake network
that drops, duplicates and replays requests, and after every step each client's view is
checked against a reference model. It caught a bug where a pull that failed halfway through
could leave a client showing half of a mutation.

## Numbers

On my M1 Pro, everything on one machine, SQLite with full fsync:

- write on one client showing up on 49 others: ~2ms median, ~5ms p99 (`go run ./cmd/syncbench`)
- pushes: ~8k/s one at a time, ~28k/s with 16 clients pushing at once (was ~6.7k before group commit)
- client rebase takes ~6µs whether it holds 1k or 100k records

## TODO

- auth / per-user data
- clean up old tombstones
- client loads everything into memory, which won't scale past a lot of records
- detect when the server db gets restored from an older backup
