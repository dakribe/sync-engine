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
- **Rebase.** The client drops the mutations the server confirmed and replays the rest on top
  of the server's data.
- **Conflicts.** The server's order decides. A put changes only the fields it names, so two
  clients editing different fields of the same record both keep their change.

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

## Layout

```
wire/           Messages between clients and the server
server/         The engine: push and pull
store/          The storage interface
store/sqlite/   SQLite storage
store/memory/   In-memory storage
cmd/syncd/      The server program
```

## Roadmap

- [x] Server engine: ordered, idempotent push and compacted pull
- [x] SQLite storage
- [ ] HTTP API
- [ ] Client with offline queue, optimistic writes and rebase
- [ ] Live updates over WebSocket
- [ ] Simulation tests with many clients over an unreliable network
- [ ] Benchmarks
- [ ] Demo app

## Build and test

Requires Go 1.26.

```sh
go test ./...
go run ./cmd/syncd
```
