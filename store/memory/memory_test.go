package memory

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/dakribe/sync-engine/store"
	"github.com/dakribe/sync-engine/wire"
)

func TestUpdateRollsBackOnError(t *testing.T) {
	s := New()
	boom := errors.New("boom")
	err := s.Update(t.Context(), func(tx store.Tx) error {
		tx.SetVersion(1)
		tx.SetLastSeq("a", 1)
		tx.PutRecord(wire.Record{Collection: "t", ID: "1", Version: 1, Fields: map[string]json.RawMessage{}})
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("update err = %v, want boom", err)
	}
	s.View(t.Context(), func(tx store.Tx) error {
		v, _ := tx.Version()
		seq, _ := tx.LastSeq("a")
		_, ok, _ := tx.Record("t", "1")
		if v != 0 || seq != 0 || ok {
			t.Errorf("failed update left version %d, seq %d, record %v", v, seq, ok)
		}
		return nil
	})
}

func TestUpdateSeesItsOwnWrites(t *testing.T) {
	s := New()
	s.Update(t.Context(), func(tx store.Tx) error {
		tx.PutRecord(wire.Record{Collection: "t", ID: "1", Version: 1, Fields: map[string]json.RawMessage{}})
		if _, ok, _ := tx.Record("t", "1"); !ok {
			t.Error("record written in this transaction is missing")
		}
		if recs, _ := tx.ChangedSince(0); len(recs) != 1 {
			t.Errorf("ChangedSince = %d records, want 1", len(recs))
		}
		return nil
	})
}

func TestViewIsReadOnly(t *testing.T) {
	s := New()
	if err := s.View(t.Context(), func(tx store.Tx) error { return tx.SetVersion(1) }); err == nil {
		t.Fatal("write in a view succeeded")
	}
}
