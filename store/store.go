// Package store defines the storage the server engine runs on.
package store

import (
	"context"

	"github.com/dakribe/sync-engine/wire"
)

// Store keeps the server's data. Update runs read-write transactions one at a time; View
// runs read-only transactions that see a consistent snapshot and may run alongside an
// Update. If fn returns an error, nothing it wrote is kept.
type Store interface {
	Update(ctx context.Context, fn func(Tx) error) error
	View(ctx context.Context, fn func(Tx) error) error
	Close() error
}

// Tx is a transaction. Records passed in and returned are copies.
type Tx interface {
	// Version is the version of the last applied mutation, or 0.
	Version() (uint64, error)
	SetVersion(v uint64) error

	// LastSeq is the last mutation applied for a client, or 0 for a new client.
	LastSeq(clientID string) (uint64, error)
	SetLastSeq(clientID string, seq uint64) error

	// Record returns a record, including a deleted one, and whether it exists.
	Record(collection, id string) (wire.Record, bool, error)
	PutRecord(rec wire.Record) error

	// ChangedSince returns every record with a version above v, ordered by version, then
	// collection, then id.
	ChangedSince(v uint64) ([]wire.Record, error)
}
