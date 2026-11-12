// Package sqlite is a store backed by a SQLite database file.
//
// It opens the file twice: one connection for writes, so Update transactions run one at a
// time and never fight over the write lock, and a pool of read-only connections for View.
// The database is in WAL mode, so reads see a consistent snapshot and don't block the
// writer. Commits are fully synced to disk, because once the server tells a client a
// mutation was applied, the client forgets it, so losing it would lose the change.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"runtime"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/wire"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Store is a SQLite store.Store.
type Store struct {
	writer *sql.DB
	reader *sql.DB
}

var _ store.Store = (*Store)(nil)

// migrations upgrade the schema one step each. PRAGMA user_version records how many have
// run.
var migrations = []string{
	`CREATE TABLE meta (
		key   TEXT PRIMARY KEY,
		value INTEGER NOT NULL
	) STRICT;

	CREATE TABLE clients (
		id       TEXT PRIMARY KEY,
		last_seq INTEGER NOT NULL
	) STRICT;

	-- fields is a JSON object, or NULL for a deleted record.
	CREATE TABLE records (
		collection TEXT NOT NULL,
		id         TEXT NOT NULL,
		fields     TEXT,
		deleted    INTEGER NOT NULL,
		version    INTEGER NOT NULL,
		PRIMARY KEY (collection, id)
	) STRICT, WITHOUT ROWID;

	-- Pulls read records in this order, so they're a range scan over this index.
	CREATE INDEX records_by_version ON records (version, collection, id);`,
}

// Open opens or creates the database at path and brings its schema up to date.
func Open(path string) (*Store, error) {
	pragmas := url.Values{}
	for _, p := range []string{"journal_mode(WAL)", "synchronous(FULL)", "busy_timeout(5000)"} {
		pragmas.Add("_pragma", p)
	}

	writer, err := sql.Open("sqlite", "file:"+path+"?"+pragmas.Encode())
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(1)
	if err := migrate(writer); err != nil {
		writer.Close()
		return nil, err
	}

	pragmas.Add("_pragma", "query_only(1)")
	reader, err := sql.Open("sqlite", "file:"+path+"?"+pragmas.Encode())
	if err != nil {
		writer.Close()
		return nil, err
	}
	reader.SetMaxOpenConns(max(4, runtime.NumCPU()))

	return &Store{writer: writer, reader: reader}, nil
}

func migrate(db *sql.DB) error {
	var current int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&current); err != nil {
		return fmt.Errorf("sqlite: read schema version: %w", err)
	}
	if current > len(migrations) {
		return fmt.Errorf("sqlite: database schema version %d is newer than this program (%d)", current, len(migrations))
	}
	for i := current; i < len(migrations); i++ {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[i]); err != nil {
			tx.Rollback()
			return fmt.Errorf("sqlite: migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, i+1)); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// Update runs fn in a write transaction and commits it if fn succeeds.
func (s *Store) Update(ctx context.Context, fn func(store.Tx) error) error {
	return run(ctx, s.writer, fn)
}

// View runs fn in a read transaction.
func (s *Store) View(ctx context.Context, fn func(store.Tx) error) error {
	return run(ctx, s.reader, fn)
}

func run(ctx context.Context, db *sql.DB, fn func(store.Tx) error) error {
	sqlTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(&tx{ctx: ctx, tx: sqlTx}); err != nil {
		sqlTx.Rollback()
		return err
	}
	return sqlTx.Commit()
}

// Close closes both connection pools.
func (s *Store) Close() error {
	return errors.Join(s.reader.Close(), s.writer.Close())
}

type tx struct {
	ctx context.Context
	tx  *sql.Tx
}

func (t *tx) Version() (uint64, error) {
	var v uint64
	err := t.tx.QueryRowContext(t.ctx, `SELECT value FROM meta WHERE key = 'version'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

func (t *tx) SetVersion(v uint64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO meta (key, value) VALUES ('version', ?)
		 ON CONFLICT (key) DO UPDATE SET value = excluded.value`, v)
	return err
}

func (t *tx) LastSeq(clientID string) (uint64, error) {
	var seq uint64
	err := t.tx.QueryRowContext(t.ctx, `SELECT last_seq FROM clients WHERE id = ?`, clientID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (t *tx) SetLastSeq(clientID string, seq uint64) error {
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO clients (id, last_seq) VALUES (?, ?)
		 ON CONFLICT (id) DO UPDATE SET last_seq = excluded.last_seq`, clientID, seq)
	return err
}

func (t *tx) Record(collection, id string) (wire.Record, bool, error) {
	row := t.tx.QueryRowContext(t.ctx,
		`SELECT collection, id, fields, deleted, version FROM records WHERE collection = ? AND id = ?`,
		collection, id)
	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return wire.Record{}, false, nil
	}
	if err != nil {
		return wire.Record{}, false, err
	}
	return rec, true, nil
}

func (t *tx) PutRecord(rec wire.Record) error {
	var fields []byte
	if !rec.Deleted {
		var err error
		if fields, err = json.Marshal(rec.Fields); err != nil {
			return err
		}
	}
	_, err := t.tx.ExecContext(t.ctx,
		`INSERT INTO records (collection, id, fields, deleted, version) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (collection, id) DO UPDATE
		 SET fields = excluded.fields, deleted = excluded.deleted, version = excluded.version`,
		rec.Collection, rec.ID, nullable(fields), rec.Deleted, rec.Version)
	return err
}

func (t *tx) ChangedSince(v uint64) ([]wire.Record, error) {
	rows, err := t.tx.QueryContext(t.ctx,
		`SELECT collection, id, fields, deleted, version FROM records
		 WHERE version > ? ORDER BY version, collection, id`, v)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []wire.Record{}
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func scanRecord(row interface{ Scan(...any) error }) (wire.Record, error) {
	var (
		rec    wire.Record
		fields sql.NullString
	)
	if err := row.Scan(&rec.Collection, &rec.ID, &fields, &rec.Deleted, &rec.Version); err != nil {
		return wire.Record{}, err
	}
	if fields.Valid {
		if err := json.Unmarshal([]byte(fields.String), &rec.Fields); err != nil {
			return wire.Record{}, fmt.Errorf("sqlite: record %s/%s: %w", rec.Collection, rec.ID, err)
		}
	}
	return rec, nil
}

func nullable(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}
