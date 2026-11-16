// Package sqlitedb opens SQLite databases the way this project uses them: WAL mode, full
// sync on commit, and schemas upgraded by numbered migrations.
package sqlitedb

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Open opens or creates the database at path. extra are more pragmas, like
// "query_only(1)".
//
// Commits are fully synced to disk. In WAL mode the cheaper synchronous=NORMAL survives
// a crashed process but can lose the last commits on power loss, and in a sync engine a
// lost commit can lose a change the other side already forgot.
func Open(path string, extra ...string) (*sql.DB, error) {
	pragmas := url.Values{}
	for _, p := range append([]string{"journal_mode(WAL)", "synchronous(FULL)", "busy_timeout(5000)"}, extra...) {
		pragmas.Add("_pragma", p)
	}
	return sql.Open("sqlite", "file:"+path+"?"+pragmas.Encode())
}

// Migrate runs the migrations the database hasn't run yet, each in its own transaction.
// PRAGMA user_version records how many have run.
func Migrate(db *sql.DB, migrations []string) error {
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
