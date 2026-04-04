package repl

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestGetMemberStateStr(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	// Test PRIMARY state (ID matches and is leader)
	if rs.IsLeader() {
		state := rsm.getMemberStateStr(rs.ID())
		if state != "PRIMARY" {
			t.Errorf("expected PRIMARY, got %s", state)
		}
	}

	// Test for unknown member (non-existent ID)
	state := rsm.getMemberStateStr(999)
	if state != "UNKNOWN" {
		t.Errorf("expected UNKNOWN for non-existent member, got %s", state)
	}
}

// Test getMemberState for all cases
func TestGetMemberState_AllCases(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	// Test PRIMARY state (ID matches local node)
	state := rsm.getMemberState(1)
	// Since rs is not started, it won't be leader, so should be SECONDARY
	if state != 2 {
		t.Errorf("getMemberState(1) = %d, want 2 (SECONDARY) when not leader", state)
	}

	// Test SECONDARY state for remote member with recent heartbeat
	rsm.lastHeartbeat[2] = time.Now()
	state = rsm.getMemberState(2)
	if state != 2 {
		t.Errorf("getMemberState(2) = %d, want 2 (SECONDARY) with recent heartbeat", state)
	}

	// Test UNKNOWN state for remote member without heartbeat
	delete(rsm.lastHeartbeat, 2)
	state = rsm.getMemberState(2)
	if state != 6 {
		t.Errorf("getMemberState(2) = %d, want 6 (UNKNOWN) without heartbeat", state)
	}

	// Test UNKNOWN state for old heartbeat
	rsm.lastHeartbeat[2] = time.Now().Add(-20 * time.Second)
	state = rsm.getMemberState(2)
	if state != 6 {
		t.Errorf("getMemberState(2) = %d, want 6 (UNKNOWN) with old heartbeat", state)
	}

	_ = rs
}

// Test getMemberStateStr for all possible state values returned by getMemberState
func TestGetMemberStateStr_AllStates(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	// Test PRIMARY (state 1) - this would require being leader
	// Test SECONDARY (state 2) - current state
	stateStr := rsm.getMemberStateStr(1)
	if stateStr != "SECONDARY" {
		t.Errorf("getMemberStateStr(1) = %q, want SECONDARY when not leader", stateStr)
	}

	// Test UNKNOWN (state 6) - member without heartbeat
	stateStr = rsm.getMemberStateStr(999)
	if stateStr != "UNKNOWN" {
		t.Errorf("getMemberStateStr(999) = %q, want UNKNOWN", stateStr)
	}

	_ = rs
}

func TestGetMemberHosts(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	_, rsm := setupTestReplicaSet(t, eng)

	hosts := rsm.getMemberHosts()
	// Just verify it doesn't panic and returns a slice
	_ = hosts
}

// Test getMemberHosts with members populated
func TestGetMemberHosts_WithMembers(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:27017", Voter: true},
			{ID: 2, Address: "localhost:27018", Voter: true},
			{ID: 3, Address: "localhost:27019", Voter: false},
		},
	}

	rs := NewReplicaSet(ReplicaSetConfig{
		ID:        1,
		Config:    cfg,
		Engine:    eng,
		Transport: NewMemTransport(),
	})

	rsm := NewReplicaSetManager(rs)

	// Add members
	rsm.members[2] = &MemberInfo{ID: 2, Host: "localhost:27018"}
	rsm.members[3] = &MemberInfo{ID: 3, Host: "localhost:27019"}

	hosts := rsm.getMemberHosts()

	// Should return all hosts except self (ID 1)
	if len(hosts) != 2 {
		t.Errorf("getMemberHosts() returned %d hosts, want 2", len(hosts))
	}

	// Verify hosts contain expected addresses
	hostMap := make(map[string]bool)
	for _, h := range hosts {
		hostMap[h] = true
	}

	if !hostMap["localhost:27018"] {
		t.Error("getMemberHosts() should include localhost:27018")
	}
	if !hostMap["localhost:27019"] {
		t.Error("getMemberHosts() should include localhost:27019")
	}
	if hostMap["localhost:27017"] {
		t.Error("getMemberHosts() should not include self (localhost:27017)")
	}
}

func TestGetMemberOptime(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	// Get optime for self
	optime := rsm.getMemberOptime(rs.ID())
	if optime == 0 {
		t.Error("expected non-zero optime for self")
	}

	// Get optime for unknown member
	optime = rsm.getMemberOptime(999)
	// Should return 0 or current time based on lag
	_ = optime
}

func TestReplicaSetManagerStepDown(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	// Only test if we are leader
	if rs.IsLeader() {
		err := rsm.StepDown(false)
		// May error or succeed depending on implementation
		_ = err
	}
}

func TestReplicaSetManagerFreeze(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	_, rsm := setupTestReplicaSet(t, eng)

	// Freeze should not panic
	rsm.Freeze(10 * time.Second)
}

func TestReplicaSetManagerStatus(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	_, rsm := setupTestReplicaSet(t, eng)

	status := rsm.Status()
	if status == nil {
		t.Error("expected non-nil status")
	}
}

func TestReplicaSetManagerIsMaster(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	isMaster := rsm.IsMaster()
	if isMaster == nil {
		t.Fatal("expected non-nil IsMaster response")
	}

	// Verify IsMaster matches our leader status
	if rs.IsLeader() && !isMaster.IsMaster {
		t.Error("expected IsMaster to be true when we are leader")
	}
}

func TestReplicaSetManagerGetReplicationLag(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	lag := rsm.GetReplicationLag(rs.ID())
	// Lag for self should be 0 or small
	_ = lag
}

func TestReplicaSetManagerUpdateHeartbeat(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	_, rsm := setupTestReplicaSet(t, eng)

	// Should not panic
	rsm.UpdateHeartbeat(999, 50)
}

func TestReplicaSetManagerUpdateReplicationLag(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	_, rsm := setupTestReplicaSet(t, eng)

	// Should not panic
	rsm.UpdateReplicationLag(999, 5*time.Second)
}

func TestMustEncode(t *testing.T) {
	// Test successful encoding
	doc := bson.D("name", bson.VString("test"))
	data := mustEncode(doc)
	if len(data) == 0 {
		t.Error("expected non-empty encoded data")
	}

	// Test with nil (should not panic)
	data = mustEncode(nil)
	// May return empty or nil
	_ = data
}

func TestMustEncode_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unencodable value")
		}
	}()

	// Channel cannot be JSON encoded - should panic
	ch := make(chan int)
	_ = mustEncode(ch)
}

// Test ReplicaSet LeaderID method
func TestReplicaSet_LeaderID(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, _ := setupTestReplicaSet(t, eng)

	// LeaderID should return the leader's ID or 0 if no leader
	leaderID := rs.LeaderID()
	if rs.IsLeader() && leaderID != rs.ID() {
		t.Errorf("expected LeaderID = %d when IsLeader, got %d", rs.ID(), leaderID)
	}
}

// Test ReplicaSet Transport method
func TestReplicaSet_Transport(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, _ := setupTestReplicaSet(t, eng)

	// Transport should return the transport
	transport := rs.Transport()
	_ = transport
}

// Test ReplicaSet Config method
func TestReplicaSet_Config(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, _ := setupTestReplicaSet(t, eng)

	// Config should return the cluster config
	cfg := rs.Config()
	if cfg == nil {
		t.Error("expected non-nil config")
	}
}

// Test memTransport Register and SendRPC
func TestMemTransport(t *testing.T) {
	transport := NewMemTransport()

	// Try to send RPC to unknown node (no nodes registered)
	req := &RPCRequest{Type: MsgRequestVote}
	resp, err := transport.SendRPC(999, req)
	if err == nil {
		t.Error("expected error when sending to unknown node")
	}
	_ = resp
}

// Test partitionTransport
func TestPartitionTransport(t *testing.T) {
	pt := NewPartitionTransport()

	// Block connection from 1 to 2
	pt.BlockConnection(1, 2)

	// Unblock connection
	pt.UnblockConnection(1, 2)

	// Set partition
	pt.SetPartition([]uint64{1}, []uint64{2})

	// Heal partition
	pt.HealPartition()

	// Try to send RPC to unregistered node
	req := &RPCRequest{Type: MsgRequestVote}
	_, _ = pt.SendRPC(2, req)
}

// Test partitionTransport Register
func TestPartitionTransport_Register(t *testing.T) {
	pt := NewPartitionTransport()
	eng := newMemEngine()

	// Create a Raft node
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
		Transport:       pt,
		ElectionTimeout: 500 * time.Millisecond,
	})

	// Register the node
	pt.Register(1, r)

	// Now sending RPC should work (not fail with "not found")
	req := &RPCRequest{
		Type:   MsgRequestVote,
		From:   2,
		
		Payload: encodePayload(&RequestVoteRequest{Term: 1, CandidateID: 2}),
	}
	resp, err := pt.SendRPC(1, req)
	if err != nil {
		t.Logf("SendRPC error (expected for unconfigured node): %v", err)
	}
	if resp == nil {
		t.Log("expected nil response for partition test")
	}
}

// Test getMemberStateStr - just verify it returns a value without panic
func TestReplicaSetManager_GetMemberStateStr(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	rs, rsm := setupTestReplicaSet(t, eng)

	// Just verify the function returns a string without panic
	result := rsm.getMemberStateStr(rs.ID())
	if result == "" {
		t.Error("expected non-empty state string")
	}
}

// Test ReplSetInitiate
func TestReplSetInitiate(t *testing.T) {
	eng := newMemEngine()

	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "127.0.0.1:9001", Voter: true},
			{ID: 2, Address: "127.0.0.1:9002", Voter: true},
			{ID: 3, Address: "127.0.0.1:9003", Voter: true},
		},
	}

	rs, err := ReplSetInitiate(eng, cfg, 1)
	if err != nil {
		t.Fatalf("ReplSetInitiate failed: %v", err)
	}
	if rs == nil {
		t.Fatal("expected non-nil ReplicaSet")
	}

	// Verify the replica set is configured correctly
	if rs.ID() != 1 {
		t.Errorf("expected ID=1, got %d", rs.ID())
	}

	// Clean up
	rs.Stop()
}
