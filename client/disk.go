package client

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/dakribe/sync-engine/internal/sqlitedb"
	"github.com/dakribe/sync-engine/wire"
)

var clientMigrations = []string{
	`CREATE TABLE meta (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	) STRICT;

	-- The base: server data as of the cursor. Deleted records are removed, not kept.
	CREATE TABLE records (
		collection TEXT NOT NULL,
		id         TEXT NOT NULL,
		fields     TEXT NOT NULL,
		version    INTEGER NOT NULL,
		PRIMARY KEY (collection, id)
	) STRICT, WITHOUT ROWID;

	-- Mutations the server hasn't confirmed, as JSON arrays of ops.
	CREATE TABLE pending (
		seq INTEGER PRIMARY KEY,
		ops TEXT NOT NULL
	) STRICT;`,
}

// disk keeps a client's state in a SQLite file, so pending mutations survive a restart.
// The client still serves reads from memory; disk is written before memory changes, so
// memory never holds anything the file doesn't.
type disk struct {
	db *sql.DB
}

// state is everything a client loads at startup.
type state struct {
	id      string
	cursor  uint64
	nextSeq uint64
	base    *table
	pending []wire.Mutation
}

func openDisk(path string) (*disk, error) {
	db, err := sqlitedb.Open(path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := sqlitedb.Migrate(db, clientMigrations); err != nil {
		db.Close()
		return nil, err
	}
	return &disk{db: db}, nil
}

// load reads the saved state. A new file has an empty id.
func (d *disk) load() (state, error) {
	st := state{base: newTable(), nextSeq: 1}

	rows, err := d.db.Query(`SELECT key, value FROM meta`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return st, err
		}
		switch k {
		case "client_id":
			st.id = v
		case "cursor":
			st.cursor, err = strconv.ParseUint(v, 10, 64)
		case "next_seq":
			st.nextSeq, err = strconv.ParseUint(v, 10, 64)
		}
		if err != nil {
			rows.Close()
			return st, fmt.Errorf("client: meta %s: %w", k, err)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	rows, err = d.db.Query(`SELECT collection, id, fields, version FROM records`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var rec wire.Record
		var fields string
		if err := rows.Scan(&rec.Collection, &rec.ID, &fields, &rec.Version); err != nil {
			rows.Close()
			return st, err
		}
		if err := json.Unmarshal([]byte(fields), &rec.Fields); err != nil {
			rows.Close()
			return st, fmt.Errorf("client: record %s/%s: %w", rec.Collection, rec.ID, err)
		}
		st.base.put(rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	rows, err = d.db.Query(`SELECT seq, ops FROM pending ORDER BY seq`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var m wire.Mutation
		var ops string
		if err := rows.Scan(&m.Seq, &ops); err != nil {
			return st, err
		}
		if err := json.Unmarshal([]byte(ops), &m.Ops); err != nil {
			return st, fmt.Errorf("client: pending mutation %d: %w", m.Seq, err)
		}
		st.pending = append(st.pending, m)
	}
	return st, rows.Err()
}

// setID records which client this file belongs to.
func (d *disk) setID(id string) error {
	_, err := d.db.Exec(`INSERT INTO meta (key, value) VALUES ('client_id', ?)`, id)
	return err
}

// addPending saves a new local mutation.
func (d *disk) addPending(m wire.Mutation) error {
	ops, err := json.Marshal(m.Ops)
	if err != nil {
		return err
	}
	return d.inTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO pending (seq, ops) VALUES (?, ?)`, m.Seq, string(ops)); err != nil {
			return err
		}
		return setMeta(tx, "next_seq", m.Seq+1)
	})
}

// applyPull saves a finished pull in one transaction: the records, the new cursor, and
// the confirmed mutations to drop.
func (d *disk) applyPull(recs []wire.Record, cursor, confirmedThrough uint64) error {
	return d.inTx(func(tx *sql.Tx) error {
		for _, rec := range recs {
			var err error
			if rec.Deleted {
				_, err = tx.Exec(`DELETE FROM records WHERE collection = ? AND id = ?`, rec.Collection, rec.ID)
			} else {
				var fields []byte
				if fields, err = json.Marshal(rec.Fields); err != nil {
					return err
				}
				_, err = tx.Exec(
					`INSERT INTO records (collection, id, fields, version) VALUES (?, ?, ?, ?)
					 ON CONFLICT (collection, id) DO UPDATE SET fields = excluded.fields, version = excluded.version`,
					rec.Collection, rec.ID, string(fields), rec.Version)
			}
			if err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`DELETE FROM pending WHERE seq <= ?`, confirmedThrough); err != nil {
			return err
		}
		return setMeta(tx, "cursor", cursor)
	})
}

func (d *disk) inTx(fn func(*sql.Tx) error) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}

func setMeta(tx *sql.Tx, key string, value uint64) error {
	_, err := tx.Exec(
		`INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
		key, strconv.FormatUint(value, 10))
	return err
}

func (d *disk) close() error { return d.db.Close() }
