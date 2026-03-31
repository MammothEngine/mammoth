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

func drainApplied(ch <-chan LogEntry, timeout time.Duration) []LogEntry {
	var entries []LogEntry
	for {
		select {
		case e := <-ch:
			entries = append(entries, e)
		case <-time.After(timeout):
			return entries
		}
	}
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
