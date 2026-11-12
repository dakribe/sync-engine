package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/wire"
)

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s
}

func TestDataSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s := open(t, path)
	err := s.Update(t.Context(), func(tx store.Tx) error {
		return errors.Join(
			tx.SetVersion(7),
			tx.SetLastSeq("a", 3),
			tx.PutRecord(wire.Record{Collection: "t", ID: "1", Version: 7,
				Fields: map[string]json.RawMessage{"title": json.RawMessage(`"milk"`)}}),
		)
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s = open(t, path)
	defer s.Close()
	err = s.View(t.Context(), func(tx store.Tx) error {
		version, _ := tx.Version()
		seq, _ := tx.LastSeq("a")
		rec, ok, err := tx.Record("t", "1")
		if err != nil {
			return err
		}
		if version != 7 || seq != 3 || !ok || string(rec.Fields["title"]) != `"milk"` {
			t.Errorf("after reopen: version %d, seq %d, record %+v", version, seq, rec)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestUpdateRollsBackOnError(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "sync.db"))
	defer s.Close()

	boom := errors.New("boom")
	err := s.Update(t.Context(), func(tx store.Tx) error {
		if err := tx.SetVersion(1); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("update err = %v, want boom", err)
	}
	s.View(t.Context(), func(tx store.Tx) error {
		if v, _ := tx.Version(); v != 0 {
			t.Errorf("version = %d after a failed update, want 0", v)
		}
		return nil
	})
}

func TestViewIsReadOnly(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "sync.db"))
	defer s.Close()

	err := s.View(t.Context(), func(tx store.Tx) error { return tx.SetVersion(1) })
	if err == nil {
		t.Fatal("write in a view succeeded")
	}
}

func TestDeletedRecordHasNoFields(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "sync.db"))
	defer s.Close()

	s.Update(t.Context(), func(tx store.Tx) error {
		return tx.PutRecord(wire.Record{Collection: "t", ID: "1", Deleted: true, Version: 1})
	})
	s.View(t.Context(), func(tx store.Tx) error {
		rec, ok, err := tx.Record("t", "1")
		if err != nil || !ok || !rec.Deleted || rec.Fields != nil {
			t.Errorf("record = %+v, %v, %v; want a tombstone", rec, ok, err)
		}
		return nil
	})
}

func TestRefusesNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	open(t, path).Close()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("open err = %v, want a newer-schema error", err)
	}
}
