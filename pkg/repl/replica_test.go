package repl

import (
	"fmt"
	"testing"
	"time"
)

// CreateTestCluster creates a test cluster of n nodes.
func CreateTestCluster(n int, timeout time.Duration) ([]*ReplicaSet, error) {
	cfg := &ClusterConfig{}
	for i := 1; i <= n; i++ {
		cfg.Nodes = append(cfg.Nodes, NodeConfig{
			ID:      uint64(i),
			Address: fmt.Sprintf("127.0.0.1:%d", 19000+i),
			Voter:   true,
		})
	}

	transport := newMemTransport()
	nodes := make([]*ReplicaSet, n)

	for i := range n {
		id := uint64(i + 1)
		eng := newMemEngine()
		sm := NewMammothStateMachine(eng)
		raft := NewRaft(RaftConfig{
			ID:                id,
			Config:            cfg,
			Engine:            eng,
			Transport:         transport,
			ElectionTimeout:   timeout,
			HeartbeatInterval: 20 * time.Millisecond,
		})
		transport.Register(id, raft)

		rs := &ReplicaSet{
			raft:      raft,
			sm:        sm,
			transport: transport,
			config:    cfg,
			id:        id,
		}
		go rs.applyLoop()
		rs.Start()
		nodes[i] = rs
	}

	return nodes, nil
}

func TestReplicaSetLeaderElection(t *testing.T) {
	nodes, err := CreateTestCluster(3, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		defer n.Stop()
	}

	leader := waitForReplicaLeader(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	count := 0
	for _, n := range nodes {
		if n.IsLeader() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 leader, got %d", count)
	}
}

func TestReplicaSetPutGet(t *testing.T) {
	nodes, err := CreateTestCluster(3, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		defer n.Stop()
	}

	leader := waitForReplicaLeader(t, nodes, 2*time.Second)

	// Put through Raft
	idx, _, err := leader.Put([]byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if idx == 0 {
		t.Fatal("expected non-zero index")
	}

	// Wait for replication + apply
	time.Sleep(300 * time.Millisecond)

	// Get from leader's local engine
	val, err := leader.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "val1" {
		t.Fatalf("expected val1, got %s", val)
	}
}

func TestReplicaSetRejectNonLeaderWrite(t *testing.T) {
	nodes, err := CreateTestCluster(3, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		defer n.Stop()
	}

	leader := waitForReplicaLeader(t, nodes, 2*time.Second)

	// Find a follower
	var follower *ReplicaSet
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	_, _, err = follower.Put([]byte("key"), []byte("val"))
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got: %v", err)
	}
}

func TestReplicaSetStatus(t *testing.T) {
	nodes, err := CreateTestCluster(3, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		defer n.Stop()
	}

	leader := waitForReplicaLeader(t, nodes, 2*time.Second)
	status := leader.Status()
	if status.State != "leader" {
		t.Fatalf("expected state=leader, got %s", status.State)
	}
	if status.ID == 0 {
		t.Fatal("expected non-zero ID")
	}
	if status.Term == 0 {
		t.Fatal("expected non-zero term")
	}
}

func TestReplicaSetDelete(t *testing.T) {
	nodes, err := CreateTestCluster(3, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		defer n.Stop()
	}

	leader := waitForReplicaLeader(t, nodes, 2*time.Second)

	// Put
	_, _, err = leader.Put([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	// Verify put
	val, err := leader.Get([]byte("k"))
	if err != nil || string(val) != "v" {
		t.Fatalf("expected k=v before delete, got %s, err=%v", val, err)
	}

	// Delete
	_, _, err = leader.Delete([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	// Get should fail
	_, err = leader.Get([]byte("k"))
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestReplicaSetMultiNodeReplication(t *testing.T) {
	nodes, err := CreateTestCluster(3, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		defer n.Stop()
	}

	leader := waitForReplicaLeader(t, nodes, 2*time.Second)

	// Put several values
	for i := 0; i < 5; i++ {
		leader.Put([]byte{byte(i)}, []byte{byte(i + 100)})
	}
	time.Sleep(500 * time.Millisecond)

	// Verify leader has all data
	for i := 0; i < 5; i++ {
		val, err := leader.Get([]byte{byte(i)})
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if val[0] != byte(i+100) {
			t.Fatalf("expected %d, got %d", i+100, val[0])
		}
	}
}

// --- Helpers ---

func waitForReplicaLeader(t *testing.T, nodes []*ReplicaSet, timeout time.Duration) *ReplicaSet {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}
