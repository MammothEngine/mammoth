package repl

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ReplicaSet manages a Raft cluster.
type ReplicaSet struct {
	mu       sync.Mutex
	raft     *Raft
	sm       *MammothStateMachine
	transport Transport
	config   *ClusterConfig
	id       uint64

	// Applied entries tracking
	appliedEntries []LogEntry
	applyDone      chan struct{}
}

// ReplicaSetConfig configures a replica set node.
type ReplicaSetConfig struct {
	ID              uint64
	Config          *ClusterConfig
	Engine          EngineInterface
	Transport       Transport // optional; defaults to in-memory
	ElectionTimeout time.Duration
	HeartbeatInterval time.Duration
}

// NewReplicaSet creates a new replica set node.
func NewReplicaSet(cfg ReplicaSetConfig) *ReplicaSet {
	// If no transport provided, create an in-memory one for testing
	transport := cfg.Transport
	if transport == nil {
		transport = NewMemTransport()
	}

	sm := NewMammothStateMachine(cfg.Engine)
	raft := NewRaft(RaftConfig{
		ID:              cfg.ID,
		Config:          cfg.Config,
		Engine:          cfg.Engine,
		Transport:       transport,
		ElectionTimeout: cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
	})

	// Register in mem transport if it supports it
	if reg, ok := transport.(interface{ Register(uint64, *Raft) }); ok {
		reg.Register(cfg.ID, raft)
	}

	rs := &ReplicaSet{
		raft:      raft,
		sm:        sm,
		transport: transport,
		config:    cfg.Config,
		id:        cfg.ID,
	}

	// Start applying committed entries
	go rs.applyLoop()

	return rs
}

// Start starts the replica set node.
func (rs *ReplicaSet) Start() {
	rs.raft.Start()
}

// Stop stops the replica set node.
func (rs *ReplicaSet) Stop() {
	rs.raft.Stop()
}

// IsLeader returns true if this node is the leader.
func (rs *ReplicaSet) IsLeader() bool {
	return rs.raft.IsLeader()
}

// LeaderID returns the current leader ID.
func (rs *ReplicaSet) LeaderID() uint64 {
	return rs.raft.LeaderID()
}

// State returns the current Raft state.
func (rs *ReplicaSet) State() NodeState {
	return rs.raft.State()
}

// Term returns the current term.
func (rs *ReplicaSet) Term() uint64 {
	return rs.raft.Term()
}

// ID returns this node's ID.
func (rs *ReplicaSet) ID() uint64 {
	return rs.id
}

// Propose submits a command to the cluster.
func (rs *ReplicaSet) Propose(cmd Command) (uint64, uint64, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return 0, 0, err
	}
	return rs.raft.Propose(data)
}

// Put proposes a put operation through Raft.
func (rs *ReplicaSet) Put(key, value []byte) (uint64, uint64, error) {
	return rs.Propose(Command{Op: "put", Key: key, Value: value})
}

// Delete proposes a delete operation through Raft.
func (rs *ReplicaSet) Delete(key []byte) (uint64, uint64, error) {
	return rs.Propose(Command{Op: "delete", Key: key})
}

// Get reads directly from the local engine (stale reads possible).
func (rs *ReplicaSet) Get(key []byte) ([]byte, error) {
	return rs.sm.engine.Get(key)
}

// Transport returns the transport for inter-node registration.
func (rs *ReplicaSet) Transport() Transport {
	return rs.transport
}

// RaftNode returns the underlying Raft node.
func (rs *ReplicaSet) RaftNode() *Raft {
	return rs.raft
}

// Status returns the replica set status.
func (rs *ReplicaSet) Status() *ReplicaSetStatus {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	stateStr := "follower"
	switch rs.raft.State() {
	case StateLeader:
		stateStr = "leader"
	case StateCandidate:
		stateStr = "candidate"
	}

	return &ReplicaSetStatus{
		ID:          rs.id,
		State:       stateStr,
		Term:        rs.raft.Term(),
		LeaderID:    rs.raft.LeaderID(),
		CommitIndex: rs.raft.CommitIndex(),
	}
}

// Config returns the cluster configuration.
func (rs *ReplicaSet) Config() *ClusterConfig {
	return rs.config
}

func (rs *ReplicaSet) applyLoop() {
	ch := rs.raft.ApplyCh()
	for entry := range ch {
		if err := rs.sm.Apply(entry); err != nil {
			continue
		}
		rs.mu.Lock()
		rs.appliedEntries = append(rs.appliedEntries, entry)
		rs.mu.Unlock()
	}
}

// --- Admin types ---

// ReplicaSetStatus is the status of a replica set node.
type ReplicaSetStatus struct {
	ID          uint64 `json:"id"`
	State       string `json:"state"`
	Term        uint64 `json:"term"`
	LeaderID    uint64 `json:"leader_id"`
	CommitIndex uint64 `json:"commit_index"`
}

// ReplSetInitiateRequest is the request to initiate a replica set.
type ReplSetInitiateRequest struct {
	Config *ClusterConfig `json:"config"`
}

// ReplSetInitiate initiates a new replica set on the given node.
func ReplSetInitiate(eng EngineInterface, cfg *ClusterConfig, nodeID uint64) (*ReplicaSet, error) {
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:     nodeID,
		Config: cfg,
		Engine: eng,
	})
	rs.Start()
	return rs, nil
}

// memTransport is an in-memory transport for testing and local use.
type memTransport struct {
	mu    sync.Mutex
	nodes map[uint64]*Raft
}

// NewMemTransport creates an in-memory transport for testing.
func NewMemTransport() *memTransport {
	return &memTransport{nodes: make(map[uint64]*Raft)}
}

func (t *memTransport) Register(id uint64, r *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = r
}

func (t *memTransport) SendRPC(to uint64, req *RPCRequest) (*RPCResponse, error) {
	t.mu.Lock()
	node, ok := t.nodes[to]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("node %d not found", to)
	}
	return node.HandleRPC(req)
}
