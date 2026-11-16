package wire

import "encoding/json"

// RecordStore is what Apply reads and writes. Record returns a copy, including a deleted
// record, and whether it exists.
type RecordStore interface {
	Record(collection, id string) (Record, bool, error)
	PutRecord(rec Record) error
}

// Apply runs a valid mutation's ops, stamping changed records with version. The server
// uses it to apply mutations for real and clients use it to predict the result, so both
// sides follow the same rules:
//
//   - A put sets only the fields it names. On a missing or deleted record it starts fresh.
//   - A delete leaves a tombstone. Deleting a missing or deleted record does nothing.
func Apply(rs RecordStore, version uint64, m Mutation) error {
	for _, op := range m.Ops {
		rec, ok, err := rs.Record(op.Collection, op.ID)
		if err != nil {
			return err
		}
		switch op.Kind {
		case OpPut:
			if !ok || rec.Deleted {
				rec = Record{Collection: op.Collection, ID: op.ID, Fields: map[string]json.RawMessage{}}
			}
			for name, value := range op.Fields {
				rec.Fields[name] = value
			}
		case OpDelete:
			if !ok || rec.Deleted {
				continue
			}
			rec.Deleted = true
			rec.Fields = nil
		}
		rec.Version = version
		if err := rs.PutRecord(rec); err != nil {
			return err
		}
	}
	return nil
}
