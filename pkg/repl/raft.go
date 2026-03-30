package repl

import (
	"encoding/json"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// NodeState represents the Raft node state.
type NodeState int

const (
	StateFollower  NodeState = iota
	StateCandidate
	StateLeader
)

// Raft implements the Raft consensus protocol.
type Raft struct {
	mu sync.Mutex

	// Persistent state
	currentTerm uint64
	votedFor    uint64 // 0 = none
	log         *RaftLog

	// Volatile state
	commitIndex uint64
	lastApplied uint64

	// Leader state
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Configuration
	id       uint64
	peers    []uint64
	config   *ClusterConfig
	transport Transport

	// Election
	state           NodeState
	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer
	electionTimeout time.Duration
	heartbeatInterval time.Duration

	// Callbacks
	applyCh  chan LogEntry
	stopCh   chan struct{}
	closed   atomic.Bool

	// Votes received (candidate only)
	votesReceived map[uint64]bool
}

// RaftConfig configures a Raft node.
type RaftConfig struct {
	ID              uint64
	Config          *ClusterConfig
	Engine          EngineInterface
	Transport       Transport
	ElectionTimeout time.Duration
	HeartbeatInterval time.Duration
}

// EngineInterface defines what Raft needs from the storage engine.
type EngineInterface interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
	Scan(prefix []byte, fn func(key, value []byte) bool) error
	NewBatch() BatchInterface
}

// BatchInterface defines batch operations.
type BatchInterface interface {
	Put(key, value []byte)
	Delete(key []byte)
	Commit() error
}

// Transport sends RPCs to peers.
type Transport interface {
	SendRPC(to uint64, req *RPCRequest) (*RPCResponse, error)
}

// NewRaft creates a new Raft node.
func NewRaft(cfg RaftConfig) *Raft {
	electionTimeout := cfg.ElectionTimeout
	if electionTimeout == 0 {
		electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 50 * time.Millisecond
	}

	var peers []uint64
	for _, n := range cfg.Config.Nodes {
		if n.ID != cfg.ID {
			peers = append(peers, n.ID)
		}
	}

	r := &Raft{
		id:              cfg.ID,
		config:          cfg.Config,
		log:             NewRaftLogEngine(cfg.Engine),
		transport:       cfg.Transport,
		peers:           peers,
		state:           StateFollower,
		electionTimeout: electionTimeout,
		heartbeatInterval: heartbeatInterval,
		nextIndex:       make(map[uint64]uint64),
		matchIndex:      make(map[uint64]uint64),
		votesReceived:   make(map[uint64]bool),
		applyCh:         make(chan LogEntry, 256),
		stopCh:          make(chan struct{}),
	}

	// Recover persistent state
	r.recoverState()

	r.electionTimer = time.AfterFunc(r.electionTimeout, r.electionTick)
	r.heartbeatTimer = time.NewTimer(r.heartbeatInterval)
	r.heartbeatTimer.Stop()

	return r
}

// Start begins the Raft event loop.
func (r *Raft) Start() {
	go r.run()
}

// Stop shuts down the Raft node.
func (r *Raft) Stop() {
	if r.closed.CompareAndSwap(false, true) {
		close(r.stopCh)
		r.electionTimer.Stop()
		r.heartbeatTimer.Stop()
	}
}

// State returns the current node state.
func (r *Raft) State() NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Term returns the current term.
func (r *Raft) Term() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// CommitIndex returns the current commit index.
func (r *Raft) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// IsLeader returns true if this node is the leader.
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == StateLeader
}

// LeaderID returns the current leader's ID (0 if unknown).
func (r *Raft) LeaderID() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == StateLeader {
		return r.id
	}
	return r.votedFor
}

// Propose submits a proposal to the Raft cluster.
func (r *Raft) Propose(data []byte) (uint64, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return 0, 0, ErrNotLeader
	}

	lastIdx, _ := r.log.LastIndex()
	entry := LogEntry{
		Index: lastIdx + 1,
		Term:  r.currentTerm,
		Data:  data,
	}
	if err := r.log.Append(entry); err != nil {
		return 0, 0, err
	}

	r.nextIndex[r.id] = entry.Index + 1
	r.matchIndex[r.id] = entry.Index

	// Maybe advance commit
	r.advanceCommit()

	return entry.Index, entry.Term, nil
}

// ApplyCh returns the channel for applied entries.
func (r *Raft) ApplyCh() <-chan LogEntry {
	return r.applyCh
}

// HandleRPC processes an incoming RPC message.
func (r *Raft) HandleRPC(req *RPCRequest) (*RPCResponse, error) {
	switch req.Type {
	case MsgAppendEntries:
		return r.handleAppendEntries(req)
	case MsgRequestVote:
		return r.handleRequestVote(req)
	case MsgInstallSnapshot:
		return r.handleInstallSnapshot(req)
	default:
		return nil, ErrInvalidRPC
	}
}

// --- RPC Handlers ---

func (r *Raft) handleAppendEntries(req *RPCRequest) (*RPCResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	aer, err := decodeAppendEntriesReq(req.Payload)
	if err != nil {
		return nil, err
	}

	resp := &AppendEntriesResponse{Term: r.currentTerm}

	// Reply false if term < currentTerm
	if aer.Term < r.currentTerm {
		resp.Success = false
		return &RPCResponse{Type: MsgAppendEntriesResp, Payload: encodePayload(resp)}, nil
	}

	// Step down if we see a higher term
	if aer.Term > r.currentTerm {
		r.currentTerm = aer.Term
		r.votedFor = 0
		r.becomeFollower()
		r.persistState()
	}

	r.resetElectionTimer()

	// Check log consistency
	if aer.PrevLogIndex > 0 {
		entry, err := r.log.Entry(aer.PrevLogIndex)
		if err != nil || entry.Term != aer.PrevLogTerm {
			resp.Success = false
			return &RPCResponse{Type: MsgAppendEntriesResp, Payload: encodePayload(resp)}, nil
		}
	}

	// Append entries
	for i, entry := range aer.Entries {
		existing, err := r.log.Entry(entry.Index)
		if err != nil || existing.Term != entry.Term {
			// Truncate conflicting entries
			r.log.TruncateAfter(entry.Index - 1)
			// Append new entries
			r.log.AppendBatch(aer.Entries[i:])
			break
		}
	}

	// Update commit index
	if aer.LeaderCommit > r.commitIndex {
		lastIdx, _ := r.log.LastIndex()
		r.commitIndex = min(aer.LeaderCommit, lastIdx)
		r.applyCommitted()
	}

	resp.Success = true
	resp.Term = r.currentTerm
	lastIdx, _ := r.log.LastIndex()
	resp.MatchIndex = lastIdx

	return &RPCResponse{Type: MsgAppendEntriesResp, Payload: encodePayload(resp)}, nil
}

func (r *Raft) handleRequestVote(req *RPCRequest) (*RPCResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rvr, err := decodeRequestVoteReq(req.Payload)
	if err != nil {
		return nil, err
	}

	resp := &RequestVoteResponse{Term: r.currentTerm}

	// Reply false if term < currentTerm
	if rvr.Term < r.currentTerm {
		return &RPCResponse{Type: MsgRequestVoteResp, Payload: encodePayload(resp)}, nil
	}

	// Step down if higher term
	if rvr.Term > r.currentTerm {
		r.currentTerm = rvr.Term
		r.votedFor = 0
		r.becomeFollower()
		r.persistState()
	}

	// Check if we can vote
	if r.votedFor == 0 || r.votedFor == rvr.CandidateID {
		// Check if candidate's log is at least as up-to-date
		lastIdx, _ := r.log.LastIndex()
		var lastTerm uint64
		if lastIdx > 0 {
			entry, _ := r.log.Entry(lastIdx)
			lastTerm = entry.Term
		}
		if rvr.LastLogTerm > lastTerm || (rvr.LastLogTerm == lastTerm && rvr.LastLogIndex >= lastIdx) {
			r.votedFor = rvr.CandidateID
			resp.VoteGranted = true
			r.resetElectionTimer()
			r.persistState()
		}
	}

	resp.Term = r.currentTerm
	return &RPCResponse{Type: MsgRequestVoteResp, Payload: encodePayload(resp)}, nil
}

func (r *Raft) handleInstallSnapshot(_ *RPCRequest) (*RPCResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &InstallSnapshotResponse{Term: r.currentTerm}
	// Simplified: accept snapshot
	return &RPCResponse{Type: MsgInstallSnapshotResp, Payload: encodePayload(resp)}, nil
}

// --- Internal ---

func (r *Raft) run() {
	<-r.stopCh
}

func (r *Raft) electionTick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() {
		return
	}

	r.becomeCandidate()
}

func (r *Raft) heartbeatTick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed.Load() || r.state != StateLeader {
		return
	}

	r.sendAppendEntriesAll()
	r.heartbeatTimer.Reset(r.heartbeatInterval)
}

func (r *Raft) becomeFollower() {
	r.state = StateFollower
	r.heartbeatTimer.Stop()
	r.resetElectionTimer()
}

func (r *Raft) becomeCandidate() {
	r.state = StateCandidate
	r.currentTerm++
	r.votedFor = r.id
	r.votesReceived = map[uint64]bool{r.id: true}
	r.persistState()

	// Send RequestVote to all peers
	lastIdx, _ := r.log.LastIndex()
	var lastTerm uint64
	if lastIdx > 0 {
		entry, _ := r.log.Entry(lastIdx)
		lastTerm = entry.Term
	}

	req := &RequestVoteRequest{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	for _, peer := range r.peers {
		go func(p uint64) {
			resp, err := r.transport.SendRPC(p, &RPCRequest{
				Type:    MsgRequestVote,
				From:    r.id,
				Payload: encodePayload(req),
			})
			if err != nil {
				return
			}
			r.handleVoteResponse(p, resp)
		}(peer)
	}

	r.resetElectionTimer()
}

func (r *Raft) handleVoteResponse(from uint64, resp *RPCResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rvr, err := decodeRequestVoteResp(resp.Payload)
	if err != nil {
		return
	}

	if rvr.Term > r.currentTerm {
		r.currentTerm = rvr.Term
		r.votedFor = 0
		r.becomeFollower()
		r.persistState()
		return
	}

	if r.state != StateCandidate {
		return
	}

	if rvr.VoteGranted {
		r.votesReceived[from] = true
		if len(r.votesReceived) >= r.config.Quorum() {
			r.becomeLeader()
		}
	}
}

func (r *Raft) becomeLeader() {
	r.state = StateLeader
	r.electionTimer.Stop()

	lastIdx, _ := r.log.LastIndex()
	for _, peer := range r.peers {
		r.nextIndex[peer] = lastIdx + 1
		r.matchIndex[peer] = 0
	}

	// Start heartbeat
	go func() {
		for {
			if r.closed.Load() || !r.IsLeader() {
				return
			}
			r.heartbeatTick()
			time.Sleep(r.heartbeatInterval)
		}
	}()
}

func (r *Raft) sendAppendEntriesAll() {
	for _, peer := range r.peers {
		r.sendAppendEntries(peer)
	}
}

func (r *Raft) sendAppendEntries(peer uint64) {
	nextIdx := r.nextIndex[peer]
	prevIdx := nextIdx - 1
	var prevTerm uint64
	if prevIdx > 0 {
		entry, err := r.log.Entry(prevIdx)
		if err != nil {
			return
		}
		prevTerm = entry.Term
	}

	// Get entries to send
	entries, err := r.log.EntriesFrom(nextIdx)
	if err != nil {
		return
	}

	// Limit batch size
	if len(entries) > 64 {
		entries = entries[:64]
	}

	req := &AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		LeaderCommit: r.commitIndex,
		Entries:      entries,
	}

	go func() {
		resp, err := r.transport.SendRPC(peer, &RPCRequest{
			Type:    MsgAppendEntries,
			From:    r.id,
			Payload: encodePayload(req),
		})
		if err != nil {
			return
		}
		r.handleAppendResponse(peer, resp)
	}()
}

func (r *Raft) handleAppendResponse(peer uint64, resp *RPCResponse) {
	r.mu.Lock()
	defer r.mu.Unlock()

	aer, err := decodeAppendEntriesResp(resp.Payload)
	if err != nil {
		return
	}

	if aer.Term > r.currentTerm {
		r.currentTerm = aer.Term
		r.votedFor = 0
		r.becomeFollower()
		r.persistState()
		return
	}

	if r.state != StateLeader {
		return
	}

	if aer.Success {
		r.nextIndex[peer] = aer.MatchIndex + 1
		r.matchIndex[peer] = aer.MatchIndex
		r.advanceCommit()
	} else {
		if r.nextIndex[peer] > 1 {
			r.nextIndex[peer]--
		}
	}
}

func (r *Raft) advanceCommit() {
	if r.state != StateLeader {
		return
	}

	lastIdx, _ := r.log.LastIndex()
	for n := r.commitIndex + 1; n <= lastIdx; n++ {
		entry, err := r.log.Entry(n)
		if err != nil || entry.Term != r.currentTerm {
			break
		}
		// Count replicas
		count := 1 // self
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= r.config.Quorum() {
			r.commitIndex = n
		}
	}
	r.applyCommitted()
}

func (r *Raft) applyCommitted() {
applyLoop:
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry, err := r.log.Entry(r.lastApplied)
		if err != nil {
			r.lastApplied--
			break
		}
		select {
		case r.applyCh <- entry:
		default:
			// Channel full, will be applied next tick
			r.lastApplied--
			break applyLoop
		}
	}
}

func (r *Raft) resetElectionTimer() {
	timeout := r.electionTimeout
	// Add jitter
	jitter := time.Duration(rand.Int63n(int64(timeout) / 5))
	r.electionTimer.Reset(timeout + jitter)
}

func (r *Raft) recoverState() {
	// Try to recover term and votedFor from engine
	data, err := r.log.engine.Get([]byte("raft:term"))
	if err == nil {
		json.Unmarshal(data, &r.currentTerm)
	}
	data, err = r.log.engine.Get([]byte("raft:votedFor"))
	if err == nil {
		json.Unmarshal(data, &r.votedFor)
	}
}

func (r *Raft) persistState() {
	termData, _ := json.Marshal(r.currentTerm)
	r.log.engine.Put([]byte("raft:term"), termData)
	voteData, _ := json.Marshal(r.votedFor)
	r.log.engine.Put([]byte("raft:votedFor"), voteData)
}

// NewRaftLogEngine creates a RaftLog using an EngineInterface.
func NewRaftLogEngine(eng EngineInterface) *RaftLog {
	return NewRaftLog(&engineAdapter{eng})
}

// engineAdapter wraps EngineInterface to satisfy LogEngine.
type engineAdapter struct {
	EngineInterface
}

func (a *engineAdapter) NewBatch() LogBatch {
	return &batchAdapter{a.EngineInterface.NewBatch()}
}

type batchAdapter struct {
	BatchInterface
}
