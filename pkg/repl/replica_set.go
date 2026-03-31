package repl

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ReplicaSetManager manages replica set operations and monitoring.
type ReplicaSetManager struct {
	mu        sync.RWMutex
	rs        *ReplicaSet
	members   map[uint64]*MemberInfo
	lagMap    map[uint64]time.Duration // replication lag per member
	lastHeartbeat map[uint64]time.Time
}

// MemberInfo holds information about a replica set member.
type MemberInfo struct {
	ID       uint64 `json:"_id"`
	Host     string `json:"host"`
	Priority int    `json:"priority"`
	Votes    int    `json:"votes"`
	Arbiter  bool   `json:"arbiterOnly,omitempty"`
	Hidden   bool   `json:"hidden,omitempty"`
	SlaveDelay int  `json:"slaveDelay,omitempty"` // seconds

	// Runtime info
	State       string        `json:"stateStr"`
	Uptime      int64         `json:"uptime"`
	Optime      int64         `json:"optimeDate"`
	LastHeartbeat time.Time   `json:"lastHeartbeat"`
	PingMS      int64         `json:"pingMs"`
	Lag         time.Duration `json:"-"`
}

// NewReplicaSetManager creates a new replica set manager.
func NewReplicaSetManager(rs *ReplicaSet) *ReplicaSetManager {
	return &ReplicaSetManager{
		rs:            rs,
		members:       make(map[uint64]*MemberInfo),
		lagMap:        make(map[uint64]time.Duration),
		lastHeartbeat: make(map[uint64]time.Time),
	}
}

// AddMember adds a new member to the replica set.
func (rsm *ReplicaSetManager) AddMember(info *MemberInfo) error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// Check if member already exists
	if _, exists := rsm.members[info.ID]; exists {
		return fmt.Errorf("member %d already exists", info.ID)
	}

	// Validate member info
	if info.Host == "" {
		return fmt.Errorf("host is required")
	}

	// Set defaults
	if info.Priority == 0 && !info.Arbiter {
		info.Priority = 1
	}
	if info.Votes == 0 {
		info.Votes = 1
	}

	rsm.members[info.ID] = info

	// If we're the leader, propose config change
	if rsm.rs.IsLeader() {
		cmd := Command{
			Op:     "reconfig",
			Key:    []byte(fmt.Sprintf("member_%d", info.ID)),
			Value:  mustEncode(info),
		}
		_, _, err := rsm.rs.Propose(cmd)
		return err
	}

	return nil
}

// RemoveMember removes a member from the replica set.
func (rsm *ReplicaSetManager) RemoveMember(memberID uint64) error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if _, exists := rsm.members[memberID]; !exists {
		return fmt.Errorf("member %d not found", memberID)
	}

	delete(rsm.members, memberID)
	delete(rsm.lagMap, memberID)
	delete(rsm.lastHeartbeat, memberID)

	// Propose config change if leader
	if rsm.rs.IsLeader() {
		cmd := Command{
			Op:  "remove_member",
			Key: []byte(fmt.Sprintf("member_%d", memberID)),
		}
		_, _, err := rsm.rs.Propose(cmd)
		return err
	}

	return nil
}

// Reconfig updates the replica set configuration.
func (rsm *ReplicaSetManager) Reconfig(members []*MemberInfo) error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// Validate config
	if len(members) < 1 {
		return fmt.Errorf("replica set must have at least 1 member")
	}

	// Check for majority voters
	voters := 0
	for _, m := range members {
		if !m.Arbiter && m.Votes > 0 {
			voters++
		}
	}
	if voters < 1 {
		return fmt.Errorf("replica set must have at least 1 voting data member")
	}

	// Update members
	newMembers := make(map[uint64]*MemberInfo)
	for _, m := range members {
		newMembers[m.ID] = m
	}
	rsm.members = newMembers

	// Propose if leader
	if rsm.rs.IsLeader() {
		data, _ := json.Marshal(members)
		cmd := Command{
			Op:    "reconfig",
			Key:   []byte("config"),
			Value: data,
		}
		_, _, err := rsm.rs.Propose(cmd)
		return err
	}

	return nil
}

// Status returns the current replica set status.
func (rsm *ReplicaSetManager) Status() *ReplSetStatus {
	rsm.mu.RLock()
	defer rsm.mu.RUnlock()

	status := &ReplSetStatus{
		Set:     "rs0",
		Date:    time.Now(),
		MyState: rsm.rs.State(),
		Term:    rsm.rs.Term(),
	}

	// Add members
	for id, info := range rsm.members {
		member := MemberStatus{
			ID:       id,
			Name:     info.Host,
			Health:   1,
			State:    rsm.getMemberState(id),
			StateStr: rsm.getMemberStateStr(id),
			Uptime:   int64(time.Since(info.LastHeartbeat).Seconds()),
			Optime:   rsm.getMemberOptime(id),
		}

		if lag, ok := rsm.lagMap[id]; ok {
			member.OptimeDate = time.Now().Add(-lag)
		}

		if lastHB, ok := rsm.lastHeartbeat[id]; ok {
			member.LastHeartbeat = lastHB
		}

		status.Members = append(status.Members, member)
	}

	return status
}

// IsMaster returns the isMaster response info.
func (rsm *ReplicaSetManager) IsMaster() *IsMasterResponse {
	rsm.mu.RLock()
	defer rsm.mu.RUnlock()

	resp := &IsMasterResponse{
		IsMaster:  rsm.rs.IsLeader(),
		Secondary: !rsm.rs.IsLeader() && rsm.rs.State() == StateFollower,
		SetName:   "rs0",
	}

	if rsm.rs.IsLeader() {
		resp.Hosts = rsm.getMemberHosts()
	}

	return resp
}

// StepDown steps down as primary (if currently primary).
func (rsm *ReplicaSetManager) StepDown(force bool) error {
	if !rsm.rs.IsLeader() {
		return fmt.Errorf("not primary")
	}

	if force {
		// Force step down immediately
		return rsm.rs.RaftNode().StepDown()
	}

	// Graceful step down - wait for secondary to catch up
	// In real implementation, check secondary lag first
	return rsm.rs.RaftNode().StepDown()
}

// Freeze prevents this node from becoming primary for the given duration.
func (rsm *ReplicaSetManager) Freeze(duration time.Duration) {
	if rsm.rs != nil && rsm.rs.RaftNode() != nil {
		rsm.rs.RaftNode().Freeze(duration)
	}
}

// UpdateHeartbeat updates heartbeat info for a member.
func (rsm *ReplicaSetManager) UpdateHeartbeat(memberID uint64, pingMS int64) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	rsm.lastHeartbeat[memberID] = time.Now()

	if info, ok := rsm.members[memberID]; ok {
		info.LastHeartbeat = time.Now()
		info.PingMS = pingMS
	}
}

// UpdateReplicationLag updates the replication lag for a member.
func (rsm *ReplicaSetManager) UpdateReplicationLag(memberID uint64, lag time.Duration) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	rsm.lagMap[memberID] = lag
}

// GetReplicationLag returns the replication lag for a member.
func (rsm *ReplicaSetManager) GetReplicationLag(memberID uint64) time.Duration {
	rsm.mu.RLock()
	defer rsm.mu.RUnlock()

	return rsm.lagMap[memberID]
}

// getMemberHosts returns list of member hosts.
func (rsm *ReplicaSetManager) getMemberHosts() []string {
	var hosts []string
	for _, m := range rsm.members {
		hosts = append(hosts, m.Host)
	}
	return hosts
}

// getMemberState returns the state of a member.
func (rsm *ReplicaSetManager) getMemberState(id uint64) int {
	if id == rsm.rs.ID() {
		if rsm.rs.IsLeader() {
			return 1 // PRIMARY
		}
		return 2 // SECONDARY
	}

	// Check heartbeat
	if lastHB, ok := rsm.lastHeartbeat[id]; ok {
		if time.Since(lastHB) < 10*time.Second {
			return 2 // SECONDARY (assumed)
		}
	}

	return 6 // UNKNOWN
}

// getMemberStateStr returns the state string.
func (rsm *ReplicaSetManager) getMemberStateStr(id uint64) string {
	state := rsm.getMemberState(id)
	switch state {
	case 1:
		return "PRIMARY"
	case 2:
		return "SECONDARY"
	case 3:
		return "RECOVERING"
	case 6:
		return "UNKNOWN"
	case 7:
		return "ARBITER"
	case 8:
		return "DOWN"
	default:
		return "UNKNOWN"
	}
}

// getMemberOptime returns the optime for a member.
func (rsm *ReplicaSetManager) getMemberOptime(id uint64) int64 {
	if id == rsm.rs.id {
		return time.Now().Unix()
	}

	// Calculate based on lag
	if lag, ok := rsm.lagMap[id]; ok {
		return time.Now().Add(-lag).Unix()
	}

	return 0
}

// --- Status types ---

// ReplSetStatus is the replica set status.
type ReplSetStatus struct {
	Set     string          `json:"set"`
	Date    time.Time       `json:"date"`
	MyState NodeState       `json:"myState"`
	Term    uint64          `json:"term"`
	Members []MemberStatus  `json:"members"`
}

// MemberStatus is the status of a replica set member.
type MemberStatus struct {
	ID            uint64    `json:"_id"`
	Name          string    `json:"name"`
	Health        int       `json:"health"`
	State         int       `json:"state"`
	StateStr      string    `json:"stateStr"`
	Uptime        int64     `json:"uptime"`
	Optime        int64     `json:"optime"`
	OptimeDate    time.Time `json:"optimeDate"`
	LastHeartbeat time.Time `json:"lastHeartbeat"`
	PingMS        int64     `json:"pingMs"`
	Lag           int64     `json:"syncSourceLagSecs,omitempty"`
}

// IsMasterResponse is the response to isMaster command.
type IsMasterResponse struct {
	IsMaster  bool     `json:"ismaster"`
	Secondary bool     `json:"secondary"`
	SetName   string   `json:"setName,omitempty"`
	Hosts     []string `json:"hosts,omitempty"`
	Primary   string   `json:"primary,omitempty"`
}

// mustEncode encodes a value to JSON or panics.
func mustEncode(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
