package repl

import (
	"encoding/binary"
	"encoding/json"
)

const raftLogPrefix = "raft_log:"

// LogEngine is the interface RaftLog needs from the storage engine.
type LogEngine interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(prefix []byte, fn func(key, value []byte) bool) error
	NewBatch() LogBatch
}

// LogBatch is the batch interface for RaftLog.
type LogBatch interface {
	Put(key, value []byte)
	Delete(key []byte)
	Commit() error
}

// RaftLog stores log entries in the KV engine.
type RaftLog struct {
	engine LogEngine
}

// NewRaftLog creates a RaftLog backed by any LogEngine.
func NewRaftLog(eng LogEngine) *RaftLog {
	return &RaftLog{engine: eng}
}

// Append adds an entry to the log.
func (l *RaftLog) Append(entry LogEntry) error {
	key := logKey(entry.Index)
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return l.engine.Put(key, data)
}

// Entry returns the log entry at the given index.
func (l *RaftLog) Entry(index uint64) (LogEntry, error) {
	key := logKey(index)
	data, err := l.engine.Get(key)
	if err != nil {
		return LogEntry{}, err
	}
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return LogEntry{}, err
	}
	return entry, nil
}

// LastIndex returns the index of the last entry.
func (l *RaftLog) LastIndex() (uint64, error) {
	var last uint64
	prefix := []byte(raftLogPrefix)
	l.engine.Scan(prefix, func(key, _ []byte) bool {
		// Extract index from key
		idx := bytesToUint64(key[len(prefix):])
		if idx > last {
			last = idx
		}
		return true
	})
	return last, nil
}

// TruncateAfter removes all entries with index > index.
func (l *RaftLog) TruncateAfter(index uint64) error {
	prefix := []byte(raftLogPrefix)
	var keys [][]byte
	l.engine.Scan(prefix, func(key, _ []byte) bool {
		idx := bytesToUint64(key[len(prefix):])
		if idx > index {
			keys = append(keys, append([]byte{}, key...))
		}
		return true
	})
	for _, k := range keys {
		l.engine.Delete(k)
	}
	return nil
}

// EntriesFrom returns entries starting from the given index.
func (l *RaftLog) EntriesFrom(startIndex uint64) ([]LogEntry, error) {
	var entries []LogEntry
	key := logKey(startIndex)
	prefix := []byte(raftLogPrefix)

	l.engine.Scan(prefix, func(k, data []byte) bool {
		idx := bytesToUint64(k[len(prefix):])
		if idx >= startIndex {
			var entry LogEntry
			if err := json.Unmarshal(data, &entry); err == nil {
				entries = append(entries, entry)
			}
		}
		return true
	})
	_ = key
	return entries, nil
}

// AppendBatch atomically appends multiple entries.
func (l *RaftLog) AppendBatch(entries []LogEntry) error {
	batch := l.engine.NewBatch()
	for _, entry := range entries {
		key := logKey(entry.Index)
		data, _ := json.Marshal(entry)
		batch.Put(key, data)
	}
	return batch.Commit()
}

func logKey(index uint64) []byte {
	key := make([]byte, len(raftLogPrefix)+8)
	copy(key, raftLogPrefix)
	binary.BigEndian.PutUint64(key[len(raftLogPrefix):], index)
	return key
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
