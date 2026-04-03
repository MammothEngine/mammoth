package repl

import (
	"testing"
)

func TestClusterConfigEncodeDecode(t *testing.T) {
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:10001", Voter: true},
			{ID: 2, Address: "localhost:10002", Voter: true},
			{ID: 3, Address: "localhost:10003", Voter: false},
		},
		Version: 1,
	}

	// Test Encode
	data, err := cfg.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Test Decode
	decoded, err := DecodeClusterConfig(data)
	if err != nil {
		t.Fatalf("DecodeClusterConfig failed: %v", err)
	}

	// Verify decoded data
	if decoded.Version != cfg.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, cfg.Version)
	}
	if len(decoded.Nodes) != len(cfg.Nodes) {
		t.Fatalf("Nodes count = %d, want %d", len(decoded.Nodes), len(cfg.Nodes))
	}
	for i, node := range decoded.Nodes {
		if node.ID != cfg.Nodes[i].ID {
			t.Errorf("Node[%d].ID = %d, want %d", i, node.ID, cfg.Nodes[i].ID)
		}
		if node.Address != cfg.Nodes[i].Address {
			t.Errorf("Node[%d].Address = %s, want %s", i, node.Address, cfg.Nodes[i].Address)
		}
		if node.Voter != cfg.Nodes[i].Voter {
			t.Errorf("Node[%d].Voter = %v, want %v", i, node.Voter, cfg.Nodes[i].Voter)
		}
	}
}

func TestDecodeClusterConfig_InvalidJSON(t *testing.T) {
	_, err := DecodeClusterConfig([]byte("invalid json"))
	if err == nil {
		t.Error("DecodeClusterConfig should fail with invalid JSON")
	}
}

func TestClusterConfigNodeAddr(t *testing.T) {
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:10001", Voter: true},
			{ID: 2, Address: "localhost:10002", Voter: true},
		},
	}

	tests := []struct {
		id       uint64
		expected string
	}{
		{1, "localhost:10001"},
		{2, "localhost:10002"},
		{3, ""}, // Non-existent node
	}

	for _, tc := range tests {
		result := cfg.NodeAddr(tc.id)
		if result != tc.expected {
			t.Errorf("NodeAddr(%d) = %q, want %q", tc.id, result, tc.expected)
		}
	}
}

func TestClusterConfigIsVoter(t *testing.T) {
	cfg := &ClusterConfig{
		Nodes: []NodeConfig{
			{ID: 1, Address: "localhost:10001", Voter: true},
			{ID: 2, Address: "localhost:10002", Voter: true},
			{ID: 3, Address: "localhost:10003", Voter: false},
		},
	}

	tests := []struct {
		id       uint64
		expected bool
	}{
		{1, true},
		{2, true},
		{3, false},
		{4, false}, // Non-existent node
	}

	for _, tc := range tests {
		result := cfg.IsVoter(tc.id)
		if result != tc.expected {
			t.Errorf("IsVoter(%d) = %v, want %v", tc.id, result, tc.expected)
		}
	}
}

func TestClusterConfigQuorum(t *testing.T) {
	tests := []struct {
		name     string
		nodes    []NodeConfig
		expected int
	}{
		{
			name:     "Empty cluster",
			nodes:    []NodeConfig{},
			expected: 1, // 0/2 + 1 = 1
		},
		{
			name:     "Single voter",
			nodes:    []NodeConfig{{ID: 1, Voter: true}},
			expected: 1, // 1/2 + 1 = 1
		},
		{
			name: "Three voters",
			nodes: []NodeConfig{
				{ID: 1, Voter: true},
				{ID: 2, Voter: true},
				{ID: 3, Voter: true},
			},
			expected: 2, // 3/2 + 1 = 2
		},
		{
			name: "Five voters (typical cluster)",
			nodes: []NodeConfig{
				{ID: 1, Voter: true},
				{ID: 2, Voter: true},
				{ID: 3, Voter: true},
				{ID: 4, Voter: true},
				{ID: 5, Voter: true},
			},
			expected: 3, // 5/2 + 1 = 3
		},
		{
			name: "Mixed voters and non-voters",
			nodes: []NodeConfig{
				{ID: 1, Voter: true},
				{ID: 2, Voter: true},
				{ID: 3, Voter: false},
				{ID: 4, Voter: false},
			},
			expected: 2, // 2/2 + 1 = 2
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ClusterConfig{Nodes: tc.nodes}
			result := cfg.Quorum()
			if result != tc.expected {
				t.Errorf("Quorum() = %d, want %d", result, tc.expected)
			}
		})
	}
}
