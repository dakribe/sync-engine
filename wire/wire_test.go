package wire

import (
	"encoding/json"
	"testing"
)

func TestValidate(t *testing.T) {
	ok := Op{Kind: OpPut, Collection: "t", ID: "1", Fields: map[string]json.RawMessage{"x": json.RawMessage("1")}}
	tests := []struct {
		name  string
		m     Mutation
		valid bool
	}{
		{"put", Mutation{Seq: 1, Ops: []Op{ok}}, true},
		{"delete", Mutation{Seq: 1, Ops: []Op{{Kind: OpDelete, Collection: "t", ID: "1"}}}, true},
		{"no ops", Mutation{Seq: 1}, false},
		{"unknown kind", Mutation{Seq: 1, Ops: []Op{{Kind: "rename", Collection: "t", ID: "1"}}}, false},
		{"no collection", Mutation{Seq: 1, Ops: []Op{{Kind: OpPut, ID: "1"}}}, false},
		{"no id", Mutation{Seq: 1, Ops: []Op{{Kind: OpPut, Collection: "t"}}}, false},
		{"delete with fields", Mutation{Seq: 1, Ops: []Op{{Kind: OpDelete, Collection: "t", ID: "1", Fields: ok.Fields}}}, false},
		{"empty field name", Mutation{Seq: 1, Ops: []Op{{Kind: OpPut, Collection: "t", ID: "1", Fields: map[string]json.RawMessage{"": json.RawMessage("1")}}}}, false},
		{"bad JSON", Mutation{Seq: 1, Ops: []Op{{Kind: OpPut, Collection: "t", ID: "1", Fields: map[string]json.RawMessage{"x": json.RawMessage("{")}}}}, false},
		{"one bad op spoils the mutation", Mutation{Seq: 1, Ops: []Op{ok, {Kind: OpPut}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.m.Validate(); (err == nil) != tt.valid {
				t.Fatalf("Validate() = %v, want valid %v", err, tt.valid)
			}
		})
	}
}

type mapStore map[string]Record

func (m mapStore) Record(collection, id string) (Record, bool, error) {
	rec, ok := m[collection+"/"+id]
	return rec.Clone(), ok, nil
}

func (m mapStore) PutRecord(rec Record) error {
	m[rec.Collection+"/"+rec.ID] = rec.Clone()
	return nil
}

func put(id string, fields map[string]string) Op {
	raw := map[string]json.RawMessage{}
	for k, v := range fields {
		raw[k] = json.RawMessage(v)
	}
	return Op{Kind: OpPut, Collection: "t", ID: id, Fields: raw}
}

func TestApply(t *testing.T) {
	s := mapStore{}
	Apply(s, 1, Mutation{Ops: []Op{put("1", map[string]string{"a": "1", "b": "1"})}})
	Apply(s, 2, Mutation{Ops: []Op{put("1", map[string]string{"b": "2"})}})
	if rec := s["t/1"]; string(rec.Fields["a"]) != "1" || string(rec.Fields["b"]) != "2" || rec.Version != 2 {
		t.Fatalf("after merging puts: %+v", rec)
	}

	Apply(s, 3, Mutation{Ops: []Op{{Kind: OpDelete, Collection: "t", ID: "1"}}})
	if rec := s["t/1"]; !rec.Deleted || rec.Fields != nil || rec.Version != 3 {
		t.Fatalf("after delete: %+v", rec)
	}

	// Deleting again changes nothing, not even the version.
	Apply(s, 4, Mutation{Ops: []Op{{Kind: OpDelete, Collection: "t", ID: "1"}}})
	if s["t/1"].Version != 3 {
		t.Fatal("deleting a deleted record bumped its version")
	}
	Apply(s, 4, Mutation{Ops: []Op{{Kind: OpDelete, Collection: "t", ID: "missing"}}})
	if _, ok := s["t/missing"]; ok {
		t.Fatal("deleting a missing record created a tombstone")
	}

	// A put after a delete starts fresh: the old fields don't come back.
	Apply(s, 5, Mutation{Ops: []Op{put("1", map[string]string{"c": "1"})}})
	if rec := s["t/1"]; rec.Deleted || len(rec.Fields) != 1 || string(rec.Fields["c"]) != "1" {
		t.Fatalf("after put on a deleted record: %+v", rec)
	}

	// Ops in one mutation see each other.
	Apply(s, 6, Mutation{Ops: []Op{put("2", map[string]string{"a": "1"}), put("2", map[string]string{"b": "1"})}})
	if rec := s["t/2"]; len(rec.Fields) != 2 {
		t.Fatalf("second op didn't build on the first: %+v", rec)
	}
}

func TestCloneIsDeep(t *testing.T) {
	rec := Record{Fields: map[string]json.RawMessage{"x": json.RawMessage("1")}}
	c := rec.Clone()
	c.Fields["x"][0] = '9'
	c.Fields["y"] = json.RawMessage("2")
	if string(rec.Fields["x"]) != "1" || len(rec.Fields) != 1 {
		t.Fatal("changing a clone changed the original")
	}
}
