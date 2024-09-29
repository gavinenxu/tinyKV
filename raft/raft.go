// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records, use for candidate count on the total granted votes
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomize electionTimeout, range is [electionTimeout, electionTimeout*2)
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	rf := new(Raft)
	rf.id = c.ID
	rf.Term = None
	rf.Vote = None
	rf.RaftLog = new(RaftLog)
	rf.Prs = make(map[uint64]*Progress, len(c.peers))
	rf.State = StateFollower
	rf.votes = make(map[uint64]bool, len(c.peers))
	rf.msgs = make([]pb.Message, 0)
	rf.Lead = None
	rf.heartbeatTimeout = c.HeartbeatTick
	rf.electionTimeout = c.ElectionTick
	rf.heartbeatElapsed = 0
	rf.heartbeatElapsed = 0
	rf.leadTransferee = 0
	rf.PendingConfIndex = 0
	rf.randomElectionTimeout = 0

	for _, peer := range c.peers {
		rf.Prs[peer] = &Progress{}
		rf.votes[peer] = false
	}

	return rf
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.handleFollowerTick()
	case StateCandidate:
		r.handleCandidateTick()
	case StateLeader:
		r.handleLeaderTick()
	}
}

// handleFollowerTick to check if needs to start an election
func (r *Raft) handleFollowerTick() {
	r.electionElapsed++
	if r.electionElapsed > r.randomElectionTimeout {
		r.electionElapsed = 0
		// start an election if timeout
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
	}
}

// handleCandidateTick to check if needs to restart an election
func (r *Raft) handleCandidateTick() {
	r.electionElapsed++
	if r.electionElapsed > r.randomElectionTimeout {
		r.electionElapsed = 0
		// start an election if timeout
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
	}
}

// handleLeaderTick to check whether to send heartbeat or append log
func (r *Raft) handleLeaderTick() {

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	// guarantee follower won't start an election during this period
	r.resetRandomElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	// push up term, more chance to be elected as leader who has a higher term
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetRandomElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// Follower should handle: MsgHup, MsgBeat, MsgAppend, MsgRequestVote
		r.handleFollowerStep(m)
	case StateCandidate:
		// Candidate should handle: MsgHup, MsgBeat, MsgRequestVote, MsgRequestVoteResponse,
		r.handleCandidateStep(m)
	case StateLeader:
		// Leader should handle: MsgBeat, MsgHeartbeatResponse, MsgRequestVote
		r.handleLeaderStep(m)
	}
	return nil
}

func (r *Raft) handleFollowerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) handleCandidateStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleStartElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) handleLeaderStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// <----------------------- RPC Message Handler --------------------------->
// Message handler should interact with outside layer through protobuf message

// handleAppendEntries handle AppendEntries RPC request
// requester: leader
// receiver:  candidate and follower
// sendTo: none
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.Vote = None
}

// handleBeat handle Beat RPC request
// requester: RawNode
// receiver:  leader
// sendTo: candidate and follower
func (r *Raft) handleBeat(m pb.Message) {
	if r.Term < m.Term {
		return
	}

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			From:    r.id,
			To:      peer,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
// requester: leader
// receiver:  candidate and follower
// sendTo: none
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.Vote = None
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleStartElection handle startElection RPC request
// which starts from follower or candidate (timeout restart election), and send Message to rest of peers to request vote
// requester: RawNode
// receiver:  candidate and follower
// sendTo: candidate and follower
func (r *Raft) handleStartElection() {
	// reset vote
	r.votes = make(map[uint64]bool, len(r.Prs))
	r.Vote = None

	r.becomeCandidate()

	// The case only one server in the raft group
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// request votes from peers
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peer,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

// handleRequestVote handle requestVote RPC request
// from candidate, receive by any of roles, we need to return vote response to requester with voteGranted value
// requester: candidate
// receiver:  candidate, follower, leader
// sendTo: candidate
func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    m.Term,
		Reject:  false,
	}

	// current term is higher than vote requester's
	// current term is equal to requester's term, but voted for another candidate
	// we need to reject request
	if r.Term > m.Term || (r.Term == m.Term && r.Vote != m.From) {
		msg.Reject = true
	} else {
		// higher term from requester or request from same candidate, we give vote to it
		r.Vote = m.From
		r.becomeFollower(m.Term, m.From)
	}

	r.msgs = append(r.msgs, msg)
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
// from any of role, and only candidate could get response
// requester: candidate, follower, leader
// receiver:  candidate
// sendTo: none
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Reject {
		r.votes[m.From] = false
	} else {
		r.votes[m.From] = true
	}

	count := 0
	for _, v := range r.votes {
		if v {
			count++
		}

		if count > len(r.Prs)/2 {
			r.becomeLeader()
			break
		}
	}
}

// <----------------------- End Message Handler --------------------------->

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) resetRandomElectionTimeout() {
	rd := rand.New(rand.NewSource(int64(r.electionTimeout)))
	r.randomElectionTimeout = r.electionTimeout + rd.Intn(r.electionTimeout)
}
