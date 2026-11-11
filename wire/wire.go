// Package wire defines the messages clients and the server exchange.
package wire

import (
	"encoding/json"
	"errors"
	"fmt"
)

// OpKind says what an Op does to its record.
type OpKind string

const (
	// OpPut creates the record, or sets the named fields on an existing one.
	OpPut OpKind = "put"
	// OpDelete removes the record.
	OpDelete OpKind = "delete"
)

// Op is one change to one record.
type Op struct {
	Kind       OpKind                     `json:"kind"`
	Collection string                     `json:"collection"`
	ID         string                     `json:"id"`
	Fields     map[string]json.RawMessage `json:"fields,omitempty"`
}

// Mutation is a group of ops that a client applies together. Seq numbers a client's
// mutations 1, 2, 3... with no gaps.
type Mutation struct {
	Seq uint64 `json:"seq"`
	Ops []Op   `json:"ops"`
}

// PushRequest sends a client's queued mutations, oldest first.
type PushRequest struct {
	ClientID  string     `json:"clientId"`
	Mutations []Mutation `json:"mutations"`
}

// Rejection says why the server skipped a mutation. A rejected mutation still counts as
// processed, so the client drops it instead of retrying forever.
type Rejection struct {
	Seq    uint64 `json:"seq"`
	Reason string `json:"reason"`
}

// PushResponse reports how far the server got through a client's mutations.
type PushResponse struct {
	LastSeq  uint64      `json:"lastSeq"`
	Rejected []Rejection `json:"rejected,omitempty"`
}

// PullRequest asks for every change after version Since.
type PullRequest struct {
	ClientID string `json:"clientId"`
	Since    uint64 `json:"since"`
}

// Record is the current state of one record. Deleted records are sent as tombstones so
// clients know to remove them.
type Record struct {
	Collection string                     `json:"collection"`
	ID         string                     `json:"id"`
	Fields     map[string]json.RawMessage `json:"fields,omitempty"`
	Deleted    bool                       `json:"deleted,omitempty"`
	Version    uint64                     `json:"version"`
}

// PullResponse holds every record that changed after the requested version, oldest change
// first. Version is the cursor for the next pull, and LastSeq is the last of this client's
// mutations the data includes.
type PullResponse struct {
	Version uint64   `json:"version"`
	LastSeq uint64   `json:"lastSeq"`
	Records []Record `json:"records"`
}

// Validate reports whether the op is well formed.
func (op Op) Validate() error {
	switch op.Kind {
	case OpPut, OpDelete:
	default:
		return fmt.Errorf("unknown op kind %q", op.Kind)
	}
	if op.Collection == "" {
		return errors.New("op has no collection")
	}
	if op.ID == "" {
		return errors.New("op has no id")
	}
	if op.Kind == OpDelete && len(op.Fields) > 0 {
		return errors.New("delete op has fields")
	}
	for name, value := range op.Fields {
		if name == "" {
			return errors.New("op has a field with no name")
		}
		if !json.Valid(value) {
			return fmt.Errorf("field %q is not valid JSON", name)
		}
	}
	return nil
}

// Validate reports whether every op in the mutation is well formed.
func (m Mutation) Validate() error {
	if len(m.Ops) == 0 {
		return errors.New("mutation has no ops")
	}
	for i, op := range m.Ops {
		if err := op.Validate(); err != nil {
			return fmt.Errorf("op %d: %w", i, err)
		}
	}
	return nil
}
