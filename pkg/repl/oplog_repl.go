package repl

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// OplogReplicator manages oplog-based replication.
// It bridges the Oplog (operation log) with the Raft replication system.
type OplogReplicator struct {
	mu       sync.RWMutex
	oplog    *Oplog
	rs       *ReplicaSet
	rsm      *ReplicaSetManager
	applier  *OplogApplier

	// Replication state
	lastAppliedHash int64
	lastAppliedTime time.Time
	replicating     bool
	stopCh          chan struct{}

	// Tailing clients
	tailClients map[string]*TailClient
}

// TailClient represents a client tailing the oplog.
type TailClient struct {
	ID          string
	Since       time.Time
	Filter      map[string]interface{}
	Ch          chan *OplogEntry
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewOplogReplicator creates a new oplog replicator.
func NewOplogReplicator(oplog *Oplog, rs *ReplicaSet, rsm *ReplicaSetManager) *OplogReplicator {
	return &OplogReplicator{
		oplog:       oplog,
		rs:          rs,
		rsm:         rsm,
		applier:     NewOplogApplier(rs.sm.engine),
		tailClients: make(map[string]*TailClient),
		stopCh:      make(chan struct{}),
	}
}

// Start begins the replication loop.
func (or *OplogReplicator) Start() error {
	or.mu.Lock()
	defer or.mu.Unlock()

	if or.replicating {
		return fmt.Errorf("already replicating")
	}

	or.replicating = true
	or.stopCh = make(chan struct{})

	// Start replication loop based on role
	go or.replicationLoop()

	return nil
}

// Stop halts the replication loop.
func (or *OplogReplicator) Stop() {
	or.mu.Lock()
	defer or.mu.Unlock()

	if !or.replicating {
		return
	}

	close(or.stopCh)
	or.replicating = false

	// Close all tail clients
	for _, client := range or.tailClients {
		client.cancel()
		close(client.Ch)
	}
	or.tailClients = make(map[string]*TailClient)
}

// replicationLoop handles the replication based on node role.
func (or *OplogReplicator) replicationLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-or.stopCh:
			return
		case <-ticker.C:
			if or.rs.IsLeader() {
				or.processPrimaryTasks()
			} else {
				or.processSecondaryTasks()
			}
		}
	}
}

// processPrimaryTasks handles tasks when this node is the primary.
func (or *OplogReplicator) processPrimaryTasks() {
	// Clean up stale tail clients
	or.cleanupStaleClients()

	// Update replication lag for secondaries
	or.updateReplicationLag()
}

// processSecondaryTasks handles tasks when this node is a secondary.
func (or *OplogReplicator) processSecondaryTasks() {
	// Apply any new entries from the oplog
	// In a real implementation, this would fetch from the primary
	or.applyLocalOplog()
}

// LogOperation logs an operation to the oplog and replicates via Raft.
// This should be called by the primary for every write operation.
func (or *OplogReplicator) LogOperation(op OpType, ns string, obj, obj2 *bson.Document) (*OplogEntry, error) {
	if !or.rs.IsLeader() {
		return nil, ErrNotLeader
	}

	// Append to local oplog
	entry, err := or.oplog.Append(op, ns, obj, obj2)
	if err != nil {
		return nil, fmt.Errorf("failed to append to oplog: %w", err)
	}

	// Replicate via Raft
	cmd := OplogCommand{
		Op:        op,
		Namespace: ns,
		Timestamp: entry.Timestamp,
		Term:      entry.Term,
		Hash:      entry.Hash,
		Object:    docToMap(obj),
		Object2:   docToMap(obj2),
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	// Propose to Raft cluster
	_, _, err = or.rs.Propose(Command{
		Op:    "oplog",
		Key:   []byte(fmt.Sprintf("oplog_%d", entry.Hash)),
		Value: data,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to replicate oplog entry: %w", err)
	}

	// Notify tail clients
	or.notifyTailClients(entry)

	return entry, nil
}

// OplogCommand represents an oplog entry as a Raft command.
type OplogCommand struct {
	Op        OpType                 `json:"op"`
	Namespace string                 `json:"ns"`
	Timestamp time.Time              `json:"ts"`
	Term      int64                  `json:"term"`
	Hash      int64                  `json:"hash"`
	Object    map[string]interface{} `json:"o,omitempty"`
	Object2   map[string]interface{} `json:"o2,omitempty"`
}

// ApplyOplogEntry applies an oplog entry from replication.
func (or *OplogReplicator) ApplyOplogEntry(cmd OplogCommand) error {
	// Convert back to documents
	obj := mapToDoc(cmd.Object)
	obj2 := mapToDoc(cmd.Object2)

	entry := &OplogEntry{
		Timestamp: cmd.Timestamp,
		Term:      cmd.Term,
		Hash:      cmd.Hash,
		Operation: cmd.Op,
		Namespace: cmd.Namespace,
		Object:    obj,
		Object2:   obj2,
		WallTime:  time.Now().UTC(),
	}

	// Apply to local state
	if err := or.applier.Apply(entry); err != nil {
		return fmt.Errorf("failed to apply oplog entry: %w", err)
	}

	// Append to local oplog for tailing
	return or.oplog.AppendEntry(entry)
}

// Tail starts tailing the oplog from a given timestamp.
func (or *OplogReplicator) Tail(ctx context.Context, since time.Time, filter map[string]interface{}) (*TailClient, error) {
	or.mu.Lock()
	defer or.mu.Unlock()

	clientCtx, cancel := context.WithCancel(ctx)
	client := &TailClient{
		ID:     generateClientID(),
		Since:  since,
		Filter: filter,
		Ch:     make(chan *OplogEntry, 100),
		ctx:    clientCtx,
		cancel: cancel,
	}

	or.tailClients[client.ID] = client

	// Send initial batch of existing entries
	go or.sendInitialEntries(client, since)

	return client, nil
}

// StopTail stops tailing the oplog for a client.
func (or *OplogReplicator) StopTail(clientID string) {
	or.mu.Lock()
	defer or.mu.Unlock()

	if client, ok := or.tailClients[clientID]; ok {
		client.cancel()
		delete(or.tailClients, clientID)
	}
}

// sendInitialEntries sends existing oplog entries to a new tail client.
func (or *OplogReplicator) sendInitialEntries(client *TailClient, since time.Time) {
	entries, err := or.oplog.GetSince(since, 1000)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if or.matchesFilter(entry, client.Filter) {
			select {
			case client.Ch <- entry:
			case <-client.ctx.Done():
				return
			case <-or.stopCh:
				return
			}
		}
	}
}

// notifyTailClients notifies all tail clients of a new oplog entry.
func (or *OplogReplicator) notifyTailClients(entry *OplogEntry) {
	or.mu.RLock()
	clients := make([]*TailClient, 0, len(or.tailClients))
	for _, c := range or.tailClients {
		clients = append(clients, c)
	}
	or.mu.RUnlock()

	for _, client := range clients {
		if or.matchesFilter(entry, client.Filter) {
			select {
			case client.Ch <- entry:
			default:
				// Client channel full, drop entry
			}
		}
	}
}

// matchesFilter checks if an entry matches the tail filter.
func (or *OplogReplicator) matchesFilter(entry *OplogEntry, filter map[string]interface{}) bool {
	if filter == nil {
		return true
	}

	// Check namespace filter
	if ns, ok := filter["ns"]; ok {
		if entry.Namespace != ns.(string) {
			return false
		}
	}

	// Check operation type filter
	if op, ok := filter["op"]; ok {
		if string(entry.Operation) != op.(string) {
			return false
		}
	}

	return true
}

// cleanupStaleClients removes clients that have been disconnected.
func (or *OplogReplicator) cleanupStaleClients() {
	or.mu.Lock()
	defer or.mu.Unlock()

	for id, client := range or.tailClients {
		select {
		case <-client.ctx.Done():
			delete(or.tailClients, id)
			close(client.Ch)
		default:
		}
	}
}

// updateReplicationLag updates the lag for secondary members.
func (or *OplogReplicator) updateReplicationLag() {
	latest := or.oplog.GetLatestTimestamp()

	or.mu.RLock()
	defer or.mu.RUnlock()

	for id, client := range or.tailClients {
		// Calculate lag based on client's last ack
		lag := time.Since(client.Since)
		if lag < 0 {
			lag = 0
		}

		// Convert client ID to member ID if applicable
		memberID := uint64(0)
		fmt.Sscanf(id, "member_%d", &memberID)
		if memberID > 0 {
			or.rsm.UpdateReplicationLag(memberID, lag)
		}
	}

	_ = latest
}

// applyLocalOplog applies any unapplied oplog entries on secondaries.
func (or *OplogReplicator) applyLocalOplog() error {
	entries, err := or.oplog.GetSince(or.lastAppliedTime, 100)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.Hash <= or.lastAppliedHash {
			continue
		}

		if err := or.applier.Apply(entry); err != nil {
			return err
		}

		or.lastAppliedHash = entry.Hash
		or.lastAppliedTime = entry.Timestamp
	}

	return nil
}

// GetReplicationStatus returns the current replication status.
func (or *OplogReplicator) GetReplicationStatus() *ReplicationStatus {
	or.mu.RLock()
	defer or.mu.RUnlock()

	return &ReplicationStatus{
		IsPrimary:       or.rs.IsLeader(),
		LastAppliedHash: or.lastAppliedHash,
		LastAppliedTime: or.lastAppliedTime,
		OplogLatest:     or.oplog.GetLatestTimestamp(),
	}
}

// ReplicationStatus holds replication statistics.
type ReplicationStatus struct {
	IsPrimary       bool      `json:"isPrimary"`
	LastAppliedHash int64     `json:"lastAppliedHash"`
	LastAppliedTime time.Time `json:"lastAppliedTime"`
	OplogLatest     time.Time `json:"oplogLatest"`
}

// Helper functions

func docToMap(doc *bson.Document) map[string]interface{} {
	if doc == nil {
		return nil
	}
	result := make(map[string]interface{})
	for _, elem := range doc.Elements() {
		result[elem.Key] = bsonValueToGo(elem.Value)
	}
	return result
}

func mapToDoc(m map[string]interface{}) *bson.Document {
	if m == nil {
		return nil
	}
	doc := bson.NewDocument()
	for k, v := range m {
		doc.Set(k, goValueToBSON(v))
	}
	return doc
}

func bsonValueToGo(v bson.Value) interface{} {
	switch v.Type {
	case bson.TypeString:
		return v.String()
	case bson.TypeInt32:
		return v.Int32()
	case bson.TypeInt64:
		return v.Int64()
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeObjectID:
		oid := v.ObjectID()
		return oid.String()
	case bson.TypeDocument:
		doc := v.DocumentValue()
		return docToMap(doc)
	case bson.TypeArray:
		arr := v.ArrayValue()
		result := make([]interface{}, 0, len(arr))
		for _, elem := range arr {
			result = append(result, bsonValueToGo(elem))
		}
		return result
	default:
		return v.String()
	}
}

func goValueToBSON(v interface{}) bson.Value {
	switch val := v.(type) {
	case string:
		return bson.VString(val)
	case int:
		return bson.VInt64(int64(val))
	case int32:
		return bson.VInt32(val)
	case int64:
		return bson.VInt64(val)
	case float64:
		return bson.VDouble(val)
	case bool:
		return bson.VBool(val)
	case map[string]interface{}:
		return bson.VDoc(mapToDoc(val))
	case []interface{}:
		docs := make([]bson.Value, len(val))
		for i, elem := range val {
			docs[i] = goValueToBSON(elem)
		}
		return bson.VArray(docs)
	default:
		return bson.VString(fmt.Sprintf("%v", v))
	}
}

func generateClientID() string {
	return fmt.Sprintf("client_%d", time.Now().UnixNano())
}
