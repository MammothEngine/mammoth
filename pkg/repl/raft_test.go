package repl

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- In-memory engine for testing ---

type memEngine struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemEngine() *memEngine {
	return &memEngine{data: make(map[string][]byte)}
}

func (m *memEngine) Get(key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[string(key)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return v, nil
}

func (m *memEngine) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = value
	return nil
}

func (m *memEngine) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

func (m *memEngine) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == string(prefix) {
			if !fn([]byte(k), v) {
				return nil
			}
		}
	}
	return nil
}

func (m *memEngine) NewBatch() BatchInterface {
	return &memBatch{engine: m}
}

type memBatch struct {
	engine *memEngine
	puts   map[string][]byte
	dels   map[string]bool
}

func (b *memBatch) Put(key, value []byte) {
	if b.puts == nil {
		b.puts = make(map[string][]byte)
	}
	b.puts[string(key)] = value
}

func (b *memBatch) Delete(key []byte) {
	if b.dels == nil {
		b.dels = make(map[string]bool)
	}
	b.dels[string(key)] = true
}

func (b *memBatch) Commit() error {
	b.engine.mu.Lock()
	defer b.engine.mu.Unlock()
	for k, v := range b.puts {
		b.engine.data[k] = v
	}
	for k := range b.dels {
		delete(b.engine.data, k)
	}
	return nil
}

// --- Helpers ---

func testClusterConfig(n int) *ClusterConfig {
	cfg := &ClusterConfig{}
	for i := 1; i <= n; i++ {
		cfg.Nodes = append(cfg.Nodes, NodeConfig{
			ID:      uint64(i),
			Address: fmt.Sprintf("127.0.0.1:%d", 9000+i),
			Voter:   true,
		})
	}
	return cfg
}

func createCluster(t *testing.T, n int, electionTimeout time.Duration) ([]*Raft, *memTransport) {
	t.Helper()
	transport := NewMemTransport()
	cfg := testClusterConfig(n)
	nodes := make([]*Raft, n)

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		eng := newMemEngine()
		r := NewRaft(RaftConfig{
			ID:              id,
			Config:          cfg,
			Engine:          eng,
			Transport:       transport,
			ElectionTimeout: electionTimeout,
			HeartbeatInterval: 20 * time.Millisecond,
		})
		nodes[i] = r
		transport.Register(id, r)
	}

	return nodes, transport
}

func startAll(t *testing.T, nodes []*Raft) {
	t.Helper()
	for _, r := range nodes {
		r.Start()
	}
}

func stopAll(t *testing.T, nodes []*Raft) {
	t.Helper()
	for _, r := range nodes {
		r.Stop()
	}
}

func waitForLeader(t *testing.T, nodes []*Raft, timeout time.Duration) *Raft {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range nodes {
			if r.IsLeader() {
				return r
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// --- Tests ---

func TestRaftLeaderElection(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("expected a leader to be elected")
	}

	// Only one leader should exist
	leaderCount := 0
	for _, r := range nodes {
		if r.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}

func TestRaftLogReplication(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	// Propose a value
	idx, term, err := leader.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	if idx == 0 {
		t.Fatal("expected non-zero index")
	}
	if term == 0 {
		t.Fatal("expected non-zero term")
	}

	// Wait for replication to followers
	time.Sleep(200 * time.Millisecond)

	// Leader should have it committed
	if leader.CommitIndex() < idx {
		t.Fatalf("expected commitIndex >= %d, got %d", idx, leader.CommitIndex())
	}
}

func TestRaftMultipleProposals(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("entry-%d", i))
		_, _, err := leader.Propose(data)
		if err != nil {
			t.Fatalf("Propose(%d) failed: %v", i, err)
		}
	}

	// Wait for replication
	time.Sleep(300 * time.Millisecond)

	if leader.CommitIndex() < 5 {
		t.Fatalf("expected commitIndex >= 5, got %d", leader.CommitIndex())
	}
}

func TestRaftRejectProposalFromNonLeader(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	// Find a follower
	var follower *Raft
	for _, r := range nodes {
		if r != leader {
			follower = r
			break
		}
	}

	_, _, err := follower.Propose([]byte("should fail"))
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
}

func TestRaftAppendEntriesRejectLowerTerm(t *testing.T) {
	eng := newMemEngine()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "a", Voter: true},
			{ID: 2, Address: "b", Voter: true},
		},
	}
	r := NewRaft(RaftConfig{
		ID:     1,
		Config: cfg,
		Engine: eng,
		Transport: NewMemTransport(),
		ElectionTimeout: 500 * time.Millisecond,
	})

	// Manually set a high term
	r.mu.Lock()
	r.currentTerm = 10
	r.mu.Unlock()

	// Send AppendEntries with lower term
	resp, err := r.HandleRPC(&RPCRequest{
		Type: MsgAppendEntries,
		Payload: encodePayload(&AppendEntriesRequest{
			Term:     5,
			LeaderID: 2,
		}),
	})
	if err != nil {
		t.Fatalf("HandleRPC error: %v", err)
	}

	var aer AppendEntriesResponse
	if err := decodePayloadInto(resp.Payload, &aer); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if aer.Success {
		t.Fatal("expected Success=false for lower term")
	}
}

func TestRaftRequestVoteRejectLowerTerm(t *testing.T) {
	eng := newMemEngine()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "a", Voter: true},
			{ID: 2, Address: "b", Voter: true},
		},
	}
	r := NewRaft(RaftConfig{
		ID:     1,
		Config: cfg,
		Engine: eng,
		Transport: NewMemTransport(),
		ElectionTimeout: 500 * time.Millisecond,
	})

	r.mu.Lock()
	r.currentTerm = 10
	r.mu.Unlock()

	resp, err := r.HandleRPC(&RPCRequest{
		Type: MsgRequestVote,
		Payload: encodePayload(&RequestVoteRequest{
			Term:        5,
			CandidateID: 2,
		}),
	})
	if err != nil {
		t.Fatalf("HandleRPC error: %v", err)
	}

	var rvr RequestVoteResponse
	if err := decodePayloadInto(resp.Payload, &rvr); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if rvr.VoteGranted {
		t.Fatal("expected VoteGranted=false for lower term")
	}
}

func TestRaftRequestVoteGrant(t *testing.T) {
	eng := newMemEngine()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "a", Voter: true},
			{ID: 2, Address: "b", Voter: true},
		},
	}
	r := NewRaft(RaftConfig{
		ID:     1,
		Config: cfg,
		Engine: eng,
		Transport: NewMemTransport(),
		ElectionTimeout: 500 * time.Millisecond,
	})

	resp, err := r.HandleRPC(&RPCRequest{
		Type: MsgRequestVote,
		Payload: encodePayload(&RequestVoteRequest{
			Term:        1,
			CandidateID: 2,
		}),
	})
	if err != nil {
		t.Fatalf("HandleRPC error: %v", err)
	}

	var rvr RequestVoteResponse
	if err := decodePayloadInto(resp.Payload, &rvr); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !rvr.VoteGranted {
		t.Fatal("expected VoteGranted=true")
	}
}

func TestRaftLeaderID(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	// Leader should report its own ID
	if leader.LeaderID() != leader.id {
		t.Fatalf("expected leader LeaderID=%d, got %d", leader.id, leader.LeaderID())
	}

	// Followers should know the leader (via votedFor after election)
	for _, r := range nodes {
		if r == leader {
			continue
		}
		// Wait a bit for heartbeat to propagate
	}
	time.Sleep(100 * time.Millisecond)

	// After heartbeat, follower should update votedFor
	for _, r := range nodes {
		if r == leader {
			continue
		}
		r.mu.Lock()
		vf := r.votedFor
		r.mu.Unlock()
		if vf != leader.id {
			t.Fatalf("follower votedFor=%d, want leader=%d", vf, leader.id)
		}
	}
}

func TestRaftTermMonotonic(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)
	term1 := leader.Term()
	if term1 == 0 {
		t.Fatal("expected non-zero term after election")
	}

	// Term should not decrease
	time.Sleep(100 * time.Millisecond)
	for _, r := range nodes {
		if r.Term() < term1 {
			t.Fatalf("term decreased: was %d, now %d", term1, r.Term())
		}
	}
}

func TestRaftStateAndCommitIndex(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	if leader.State() != StateLeader {
		t.Fatal("expected StateLeader")
	}

	// Propose
	idx, _, err := leader.Propose([]byte("test"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if leader.CommitIndex() < idx {
		t.Fatalf("commit index %d < proposed %d", leader.CommitIndex(), idx)
	}
}

func TestRaftQuorum(t *testing.T) {
	cfg := testClusterConfig(5)
	// 5 voters -> quorum = 3
	q := cfg.Quorum()
	if q != 3 {
		t.Fatalf("expected quorum=3 for 5 voters, got %d", q)
	}

	cfg2 := testClusterConfig(3)
	q2 := cfg2.Quorum()
	if q2 != 2 {
		t.Fatalf("expected quorum=2 for 3 voters, got %d", q2)
	}
}

// decodePayloadInto is a test helper.
func decodePayloadInto(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// Test Raft StepDown
func TestRaftStepDown(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	// Step down
	err := leader.StepDown()
	if err != nil {
		t.Fatalf("StepDown: %v", err)
	}

	// Should no longer be leader
	if leader.State() == StateLeader {
		t.Error("expected leader to step down")
	}

	// Wait for new leader
	time.Sleep(300 * time.Millisecond)

	// A new leader should be elected
	newLeader := waitForLeader(t, nodes, 2*time.Second)
	if newLeader.id == leader.id {
		t.Error("expected a different leader after step down")
	}
}

// Test Raft StepDown from non-leader fails
func TestRaftStepDown_NotLeader(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	// Find a follower
	var follower *Raft
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	// Step down from follower should fail
	err := follower.StepDown()
	if err != ErrNotLeader {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}
}

// Test Raft CreateSnapshot
func TestRaftCreateSnapshot(t *testing.T) {
	nodes, _ := createCluster(t, 3, 50*time.Millisecond)
	startAll(t, nodes)
	defer stopAll(t, nodes)

	leader := waitForLeader(t, nodes, 2*time.Second)

	// Propose some entries
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("entry_%d", i))
		_, _, err := leader.Propose(data)
		if err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Create snapshot
	snap, err := leader.CreateSnapshot()
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Verify snapshot data
	if snap.LastIncludedIndex == 0 {
		t.Error("expected non-zero LastIncludedIndex")
	}
	if snap.LastIncludedTerm == 0 {
		t.Error("expected non-zero LastIncludedTerm")
	}
	if len(snap.Data) == 0 {
		t.Error("expected non-empty snapshot data")
	}
}

// Test handleInstallSnapshot
func TestRaftHandleInstallSnapshot(t *testing.T) {
	eng := newMemEngine()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "a", Voter: true},
			{ID: 2, Address: "b", Voter: true},
		},
	}
	r := NewRaft(RaftConfig{
		ID:              1,
		Config:          cfg,
		Engine:          eng,
		Transport:       NewMemTransport(),
		ElectionTimeout: 500 * time.Millisecond,
	})

	// Set a known term
	r.mu.Lock()
	r.currentTerm = 5
	r.mu.Unlock()

	// Send InstallSnapshot request
	resp, err := r.HandleRPC(&RPCRequest{
		Type: MsgInstallSnapshot,
		Payload: encodePayload(&InstallSnapshotRequest{
			Term:              5,
			LeaderID:          2,
			LastIncludedIndex: 10,
			LastIncludedTerm:  3,
			Data:              []byte("snapshot data"),
		}),
	})
	if err != nil {
		t.Fatalf("HandleRPC error: %v", err)
	}

	// Verify response
	var isr InstallSnapshotResponse
	if err := decodePayloadInto(resp.Payload, &isr); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if isr.Term != 5 {
		t.Errorf("expected Term=5, got %d", isr.Term)
	}
}

// Test RaftLogEngine and simpleBatch
func TestRaftLogEngine_BatchOperations(t *testing.T) {
	eng := newMemEngine()

	// Create an engineAdapter to access NewBatch
	adapter := &engineAdapter{eng}

	// Test Put through batch
	batch := adapter.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))

	// Commit the batch
	err := batch.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify values were written
	val1, err := eng.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get key1 failed: %v", err)
	}
	if string(val1) != "value1" {
		t.Errorf("key1: expected 'value1', got '%s'", string(val1))
	}

	val2, err := eng.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if string(val2) != "value2" {
		t.Errorf("key2: expected 'value2', got '%s'", string(val2))
	}
}

// Test simpleBatch Delete
func TestRaftLogEngine_BatchDelete(t *testing.T) {
	eng := newMemEngine()

	// Create an engineAdapter to access NewBatch
	adapter := &engineAdapter{eng}

	// Pre-populate data
	eng.Put([]byte("key1"), []byte("value1"))
	eng.Put([]byte("key2"), []byte("value2"))

	// Delete through batch
	batch := adapter.NewBatch()
	batch.Delete([]byte("key1"))

	// Commit the batch
	err := batch.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify key1 was deleted
	_, err = eng.Get([]byte("key1"))
	if err == nil {
		t.Error("key1 should have been deleted")
	}

	// Verify key2 still exists
	val2, err := eng.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Get key2 failed: %v", err)
	}
	if string(val2) != "value2" {
		t.Errorf("key2: expected 'value2', got '%s'", string(val2))
	}
}

// Test simpleBatch mixed operations
func TestRaftLogEngine_BatchMixed(t *testing.T) {
	eng := newMemEngine()

	// Create an engineAdapter to access NewBatch
	adapter := &engineAdapter{eng}

	// Pre-populate data
	eng.Put([]byte("old_key"), []byte("old_value"))

	// Mixed operations
	batch := adapter.NewBatch()
	batch.Put([]byte("new_key"), []byte("new_value"))
	batch.Delete([]byte("old_key"))
	batch.Put([]byte("another_key"), []byte("another_value"))

	// Commit
	err := batch.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify old_key deleted
	_, err = eng.Get([]byte("old_key"))
	if err == nil {
		t.Error("old_key should have been deleted")
	}

	// Verify new keys exist
	val, _ := eng.Get([]byte("new_key"))
	if string(val) != "new_value" {
		t.Errorf("new_key: expected 'new_value', got '%s'", string(val))
	}

	val, _ = eng.Get([]byte("another_key"))
	if string(val) != "another_value" {
		t.Errorf("another_key: expected 'another_value', got '%s'", string(val))
	}
}
