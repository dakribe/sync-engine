# sync-engine

A local-first sync engine in Go.

Clients read and write a local copy of the data, so every change shows instantly and works
offline. Changes are queued and pushed to a server, which puts every client's changes in one
order and sends the result back out. Clients then replay their own unconfirmed changes on top,
so everyone ends up with the same data.

## How it works

- **Mutations.** A client groups its changes into a mutation: one or more puts or deletes on
  records, applied together. Each client numbers its mutations 1, 2, 3...
- **Push.** The client sends its queued mutations. The server applies them in order and
  remembers the last number it applied for that client, so a mutation sent twice (after a
  timeout, say) is only applied once.
- **Pull.** The client asks for everything that changed since its last version. The server
  answers with the current state of each changed record, not every step that led there, plus
  the last mutation it applied for that client.
- **Pages.** Large pulls come in pages of up to 1,000 records (the client can ask for up to
  10,000). A page never splits a mutation, so each page ends at an exact cursor. The client
  keeps pulling while `more` is set, then applies all the pages at once: a record that
  changes again mid-pull moves to a later page, so stopping early could show half of a
  mutation. (The simulation tests found this.)
- **Live updates.** Clients keep an event stream open. When a push changes data, the
  server sends each client a tiny poke with the new version, and clients that are behind
  pull. A slow client skips straight to the latest version instead of queueing pokes.
- **Rebase.** The client drops the mutations the server confirmed and replays the rest on top
  of the server's data.
- **Conflicts.** The server's order decides. A put changes only the fields it names, so two
  clients editing different fields of the same record both keep their change.

## Client

```go
c, err := client.Open("todos.db", &client.HTTPTransport{BaseURL: "http://localhost:8080"}, client.Options{})

c.Put("todos", "1", map[string]any{"title": "milk", "done": false}) // visible at once
c.Get("todos", "1")
c.List("todos")
c.Sync(ctx) // push, pull, rebase; safe to call offline
go c.Run(ctx) // or let it sync in the background
```

- **Two layers.** The client holds the server's data as of its last pull (the base) and,
  on top, a copy-on-write layer with its unconfirmed mutations applied. Reads go through
  the layer, so a change is visible the moment it's made.
- **Same rules on both sides.** Client and server apply mutations with the same function,
  `wire.Apply`, so the client's prediction matches what the server will do.
- **Rebase costs what's pending, not what's stored.** After a pull, the client throws away
  the layer and replays its pending mutations on a fresh one. Only records those
  mutations touch are copied, so a rebase costs the same over 10 records or 10 million.
- **No flicker.** Pushed mutations stay in the queue until a pull brings back data that
  includes them, so a change never blinks out between push and pull.
- **Background sync.** `Run` syncs right after a local change, when poked, and on an
  interval. Writes made during a sync are folded into one more, so a burst of edits costs a
  couple of round trips. While offline it backs off exponentially with jitter (250 ms up to
  30 s), and doesn't retry on every keystroke. Its tests run on a fake clock
  (`testing/synctest`), so they're instant and deterministic.
- **Survives restarts.** `client.Open(path, ...)` keeps the client's state in a SQLite
  file: its ID, cursor, synced data and pending mutations. A change made offline is pushed
  whenever the app next runs, and a restarted client never reuses a mutation number (the
  server would skip it as a duplicate). Each change is written to disk before it's applied
  in memory, and reads are served from memory.
- **Live.** `Run` holds the event stream open and pulls the moment the server announces
  a version newer than its own, reconnecting with backoff if the stream drops. In the
  end-to-end test, a write on one client shows on another over local HTTP in about 2 ms,
  with polling switched off. While offline, local writes don't trigger retries but a poke
  does, since it means the server is back.
- **Offline is the normal case.** A failed push or pull loses nothing; the next sync
  continues. A push whose reply was lost is resent and applied once.

## API

JSON over HTTP. Errors are objects with an `"error"` field, and bodies are capped at 8 MB.

| Endpoint | Request | Response |
|---|---|---|
| `POST /push` | `{"clientId", "mutations": [{"seq", "ops": [...]}]}` | `{"lastSeq", "rejected"}` |
| `POST /pull` | `{"clientId", "since", "limit"}` | `{"version", "lastSeq", "records", "more"}` |
| `GET /events` | | server-sent events: `event: poke`, `data: {"version"}` |
| `GET /healthz` | | `{"status": "ok"}` |

An op is `{"kind": "put" | "delete", "collection", "id", "fields"}`.

`/events` sends the current version as soon as you connect, so a client that reconnects
after a drop catches up at once, then a poke after each change, and a comment every 15 s
so proxies don't close an idle stream.

**Why server-sent events and not WebSocket?** Pokes only go from server to client; writes
already go over plain HTTP. SSE does this with the standard library, passes through
ordinary HTTP proxies, and needs no extra protocol or dependency.

```sh
curl -X POST localhost:8080/push -d '{"clientId":"a","mutations":[{"seq":1,"ops":[
  {"kind":"put","collection":"todos","id":"1","fields":{"title":"milk"}}]}]}'
curl -X POST localhost:8080/pull -d '{"clientId":"b","since":0}'
```

## Storage

The engine runs on a small `Store` interface with two implementations, and every engine
test runs against both:

- **SQLite** (`store/sqlite`), pure Go with no C compiler needed. Writes go through one
  connection, so pushes never fight over SQLite's write lock; reads use a separate pool and,
  in WAL mode, never block the writer. Commits are fully synced to disk, because once the
  server confirms a mutation the client forgets it, so a lost commit would lose the change.
  Pulls are a range scan over a `(version, collection, id)` index. The schema is upgraded by
  numbered migrations.
- **Memory** (`store/memory`), for tests and running without a database.

## Testing

Besides unit tests for each package, `sim/` tests the whole system:

- **Deterministic simulation.** Four clients, two of them saved to disk, make random edits
  while a seeded random network drops requests, loses responses after the server applied
  them, delivers requests twice and replays stale pushes late. Clients go offline and
  restart. The server is wrapped in a recorder that replays every mutation it applies onto
  a reference model, keeping the state at every version. **After every step**, each
  client's view must equal the model at the client's cursor with its own pending
  mutations replayed on top. At the end the network heals, and every client, the server
  and the model must hold the same data, with every mutation applied exactly once and in
  order. A failing seed reproduces exactly with `-seed N`.
- **Concurrent clients.** Six clients in real goroutines write as fast as they can while
  their background loops sync over a network that fails a quarter of requests. After it
  heals, they must converge on their own through live updates. Run with `-race`.

The simulation found a bug on its first run: a pull interrupted between pages left a
client showing half of a mutation. It's fixed (the client now applies a pull's pages
together), with a unit test that reproduces it.

```sh
go test ./...                                   # 100 seeds × 400 steps, plus SQLite seeds
go test ./sim -run 'TestSimulation$' -seeds 2000  # longer sweep (about 3 minutes)
go test ./sim -run 'TestSimulation$' -seed 42 -v  # replay one seed
go test -race ./...
```

## Layout

```
wire/           Messages between clients and the server, and the rules for applying them
client/         The local replica: optimistic writes, sync, rebase
server/         The engine: push and pull
store/          The storage interface
store/sqlite/   SQLite storage
store/memory/   In-memory storage
internal/       Shared SQLite setup and migrations
sim/            Whole-system simulation tests
cmd/syncd/      The server program
```

## Roadmap

- [x] Server engine: ordered, idempotent push and compacted pull
- [x] SQLite storage
- [x] HTTP API
- [x] Client with offline queue, optimistic writes and rebase
- [x] Live updates over server-sent events
- [x] Simulation tests with many clients over an unreliable network
- [ ] Benchmarks
- [ ] Demo app

## Build and test

Requires Go 1.26.

```sh
go test ./...
go run ./cmd/syncd                  # listens on :8080, data in sync.db
go run ./cmd/syncd -db ""           # keep data in memory
```
