package repl

import (
	"encoding/json"
	"time"
)

// StateMachine applies committed log entries to the application state.
type StateMachine interface {
	Apply(entry LogEntry) error
	Snapshot() (Snapshot, error)
	Restore(data []byte) error
}

// Snapshot represents a point-in-time snapshot of the state machine.
type Snapshot struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// Command represents a state machine command.
type Command struct {
	Op    string `json:"op"`    // "put", "delete", "batch"
	Key   []byte `json:"key"`   // for put/delete
	Value []byte `json:"value"` // for put
	// Batch operations
	Ops []Command `json:"ops,omitempty"` // for batch
}

// MammothStateMachine applies BSON commands to the engine.
type MammothStateMachine struct {
	engine EngineInterface
}

// NewMammothStateMachine creates a state machine backed by an engine.
func NewMammothStateMachine(eng EngineInterface) *MammothStateMachine {
	return &MammothStateMachine{engine: eng}
}

// Apply applies a log entry to the state machine.
func (m *MammothStateMachine) Apply(entry LogEntry) error {
	if entry.Type == 1 {
		// Config change — not handled by state machine
		return nil
	}

	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case "put":
		return m.engine.Put(cmd.Key, cmd.Value)
	case "delete":
		return m.engine.Delete(cmd.Key)
	case "batch":
		for _, op := range cmd.Ops {
			switch op.Op {
			case "put":
				if err := m.engine.Put(op.Key, op.Value); err != nil {
					return err
				}
			case "delete":
				if err := m.engine.Delete(op.Key); err != nil {
					return err
				}
			}
		}
		return nil
	case "oplog":
		// Apply oplog entry
		var oplogCmd OplogCommand
		if err := json.Unmarshal(cmd.Value, &oplogCmd); err != nil {
			return err
		}
		return m.applyOplogCommand(oplogCmd)
	}
	return nil
}

// applyOplogCommand applies an oplog command to the state machine.
func (m *MammothStateMachine) applyOplogCommand(cmd OplogCommand) error {
	// Convert back to documents
	obj := mapToDoc(cmd.Object)
	obj2 := mapToDoc(cmd.Object2)

	oplogEntry := &OplogEntry{
		Timestamp: cmd.Timestamp,
		Term:      cmd.Term,
		Hash:      cmd.Hash,
		Operation: cmd.Op,
		Namespace: cmd.Namespace,
		Object:    obj,
		Object2:   obj2,
		WallTime:  time.Now().UTC(),
	}

	// Apply based on operation type
	applier := NewOplogApplier(m.engine)
	return applier.Apply(oplogEntry)
}

// Snapshot creates a full snapshot of the engine state.
func (m *MammothStateMachine) Snapshot() ([]byte, error) {
	var data struct {
		Entries []kvEntry `json:"entries"`
	}
	m.engine.Scan(nil, func(key, value []byte) bool {
		data.Entries = append(data.Entries, kvEntry{
			Key:   key,
			Value: value,
		})
		return true
	})
	return json.Marshal(data)
}

// Restore restores the engine from snapshot data.
func (m *MammothStateMachine) Restore(data []byte) error {
	var snapshot struct {
		Entries []kvEntry `json:"entries"`
	}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	// Clear existing data by scanning and deleting
	var keys [][]byte
	m.engine.Scan(nil, func(key, _ []byte) bool {
		keys = append(keys, append([]byte{}, key...))
		return true
	})
	for _, k := range keys {
		m.engine.Delete(k)
	}

	// Restore from snapshot
	if batcher, ok := m.engine.(interface{ NewBatch() interface{ Put([]byte, []byte); Delete([]byte); Commit() error } }); ok {
		batch := batcher.NewBatch()
		for _, e := range snapshot.Entries {
			batch.Put(e.Key, e.Value)
		}
		return batch.Commit()
	}
	// Fallback: sequential puts
	for _, e := range snapshot.Entries {
		if err := m.engine.Put(e.Key, e.Value); err != nil {
			return err
		}
	}
	return nil
}

type kvEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
