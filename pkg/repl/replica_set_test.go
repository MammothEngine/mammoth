package repl

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestReplicaSetManager_AddMember(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	mt.Register(1, rs.RaftNode())

	rsm := NewReplicaSetManager(rs)

	// Add a member
	member := &MemberInfo{
		ID:   4,
		Host: "localhost:27017",
	}

	err := rsm.AddMember(member)
	if err != nil {
		t.Logf("AddMember returned: %v", err)
	}

	// Verify member was added to local map
	if _, ok := rsm.members[4]; !ok {
		t.Error("member should be in local map")
	}
}

func TestReplicaSetManager_Status(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add some members
	rsm.members[1] = &MemberInfo{ID: 1, Host: "localhost:27017"}
	rsm.members[2] = &MemberInfo{ID: 2, Host: "localhost:27018"}

	status := rsm.Status()
	if status == nil {
		t.Fatal("expected status")
	}

	if len(status.Members) != 2 {
		t.Errorf("expected 2 members, got %d", len(status.Members))
	}
}

func TestReplicaSetManager_IsMaster(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	resp := rsm.IsMaster()
	if resp == nil {
		t.Fatal("expected isMaster response")
	}

	// Should not be master (not started)
	if resp.IsMaster {
		t.Error("should not be master before starting")
	}

	// Secondary may be true if node is in follower state
	_ = resp.Secondary
}

func TestReplicaSetManager_ReplicationLag(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Update lag for member
	rsm.UpdateReplicationLag(2, 5*time.Second)

	lag := rsm.GetReplicationLag(2)
	if lag != 5*time.Second {
		t.Errorf("expected 5s lag, got %v", lag)
	}
}

func TestReplicaSetManager_UpdateHeartbeat(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add member
	rsm.members[2] = &MemberInfo{ID: 2, Host: "localhost:27018"}

	// Update heartbeat
	rsm.UpdateHeartbeat(2, 10)

	if _, ok := rsm.lastHeartbeat[2]; !ok {
		t.Error("heartbeat should be recorded")
	}
}

func TestReplicaSetManager_StepDown(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Try to step down (should fail as not leader)
	err := rsm.StepDown(false)
	if err == nil {
		t.Error("expected error when stepping down as non-leader")
	}
}

func TestReplicaSetManager_Freeze(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Freeze for short duration
	rsm.Freeze(100 * time.Millisecond)

	// Just verify it doesn't panic
}

func TestReplicaSetManager_RemoveMember(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add then remove
	rsm.members[2] = &MemberInfo{ID: 2, Host: "localhost:27018"}

	err := rsm.RemoveMember(2)
	if err != nil {
		t.Logf("RemoveMember returned: %v", err)
	}

	if _, ok := rsm.members[2]; ok {
		t.Error("member should be removed")
	}
}

func TestReplicaSetManager_Reconfig(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Try invalid config
	err := rsm.Reconfig([]*MemberInfo{})
	if err == nil {
		t.Error("expected error for empty config")
	}

	// Valid config
	members := []*MemberInfo{
		{ID: 1, Host: "localhost:27017"},
		{ID: 2, Host: "localhost:27018"},
	}

	err = rsm.Reconfig(members)
	if err != nil {
		t.Logf("Reconfig returned: %v", err)
	}
}

// mockEngine is a simple mock for EngineInterface
type mockEngine struct {
	data map[string][]byte
	mu   sync.Mutex
}

func (m *mockEngine) Get(key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[string(key)]; ok {
		return v, nil
	}
	return nil, errors.New("not found")
}

func (m *mockEngine) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = value
	return nil
}

func (m *mockEngine) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

func (m *mockEngine) Scan(prefix []byte, fn func(key, value []byte) bool) error {
	return nil
}

func (m *mockEngine) NewBatch() BatchInterface {
	return &mockBatch{eng: m}
}

type mockBatch struct {
	eng *mockEngine
}

func (b *mockBatch) Put(key, value []byte) {}
func (b *mockBatch) Delete(key []byte) {}
func (b *mockBatch) Commit() error { return nil }

// Test AddMember with duplicate member ID
func TestReplicaSetManager_AddMember_Duplicate(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add first member
	member := &MemberInfo{
		ID:   2,
		Host: "localhost:27018",
	}
	err := rsm.AddMember(member)
	if err != nil {
		t.Logf("First AddMember: %v", err)
	}

	// Try to add duplicate
	duplicate := &MemberInfo{
		ID:   2,
		Host: "localhost:27019",
	}
	err = rsm.AddMember(duplicate)
	if err == nil {
		t.Error("expected error for duplicate member ID")
	}
}

// Test AddMember with missing host
func TestReplicaSetManager_AddMember_NoHost(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add member with no host
	member := &MemberInfo{
		ID: 2,
		// Host is empty
	}
	err := rsm.AddMember(member)
	if err == nil {
		t.Error("expected error for missing host")
	}
}

// Test AddMember with default priority and votes
func TestReplicaSetManager_AddMember_Defaults(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add member with no priority or votes set
	member := &MemberInfo{
		ID:   2,
		Host: "localhost:27018",
		// Priority and Votes are 0, should get defaults
	}
	err := rsm.AddMember(member)
	if err != nil {
		t.Logf("AddMember: %v", err)
	}

	// Check defaults were set
	if rsm.members[2] == nil {
		t.Fatal("member not added")
	}
	if rsm.members[2].Priority != 1 {
		t.Errorf("expected default priority=1, got %d", rsm.members[2].Priority)
	}
	if rsm.members[2].Votes != 1 {
		t.Errorf("expected default votes=1, got %d", rsm.members[2].Votes)
	}
}

// Test RemoveMember with non-existent member
func TestReplicaSetManager_RemoveMember_NotFound(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Try to remove member that doesn't exist
	err := rsm.RemoveMember(999)
	if err == nil {
		t.Error("expected error for non-existent member")
	}
	if err.Error() != "member 999 not found" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// Test AddMember with arbiter (should have priority 0)
func TestReplicaSetManager_AddMember_Arbiter(t *testing.T) {
	mt := NewMemTransport()
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
		},
	}

	eng := &mockEngine{data: make(map[string][]byte)}
	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: mt,
	})

	rsm := NewReplicaSetManager(rs)

	// Add arbiter member
	member := &MemberInfo{
		ID:      2,
		Host:    "localhost:27018",
		Arbiter: true,
		// Priority should stay 0 for arbiter
	}
	err := rsm.AddMember(member)
	if err != nil {
		t.Logf("AddMember: %v", err)
	}

	// Check arbiter priority is 0
	if rsm.members[2] == nil {
		t.Fatal("member not added")
	}
	if rsm.members[2].Priority != 0 {
		t.Errorf("expected arbiter priority=0, got %d", rsm.members[2].Priority)
	}
}
