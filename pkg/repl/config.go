package repl

import "encoding/json"

// NodeConfig describes a single node in the cluster.
type NodeConfig struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
	Voter   bool   `json:"voter"`
}

// ClusterConfig describes the cluster membership.
type ClusterConfig struct {
	Nodes    []NodeConfig `json:"nodes"`
	Version  uint64       `json:"version"`
}

// Encode serializes the cluster config.
func (c *ClusterConfig) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeClusterConfig deserializes a cluster config.
func DecodeClusterConfig(data []byte) (*ClusterConfig, error) {
	var cfg ClusterConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// NodeAddr returns the address of a node by ID.
func (c *ClusterConfig) NodeAddr(id uint64) string {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n.Address
		}
	}
	return ""
}

// IsVoter checks if a node is a voter.
func (c *ClusterConfig) IsVoter(id uint64) bool {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n.Voter
		}
	}
	return false
}

// Quorum returns the majority threshold.
func (c *ClusterConfig) Quorum() int {
	voters := 0
	for _, n := range c.Nodes {
		if n.Voter {
			voters++
		}
	}
	return voters/2 + 1
}
