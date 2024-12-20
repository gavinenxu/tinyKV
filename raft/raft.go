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

/*

Raft Peer State: Match, Next
These terms describe the relationship between a follower node and the leader during log replication.

* Match Index:

The index of the highest log entry known to be replicated on a follower.
Maintained by the leader for each follower.
Used during leader election and log commitment. For example, to determine the "majority committed index."

* Next Index:

The index of the next log entry the leader will send to a follower.
Starts as leader's last log index + 1 when the leader is elected.
Adjusted during log synchronization (e.g., decreased if a follower rejects entries due to inconsistencies).

Raft Log State: Committed, Applied, Stabled
These terms describe the lifecycle of log entries within a node.

* Committed Index:

The highest log entry index that is safely replicated to a majority of nodes and guaranteed to be durable.
Once an entry is committed, it can be applied to the state machine.

* Applied Index:

The highest log entry index that has been applied to the state machine on a node.
Always ≤ the committed index, as you can’t apply an uncommitted log entry.

* Stabled Index:

The highest log entry index that has been persisted to stable storage (disk or equivalent) on the local node.
Ensures data durability even in case of crashes.
Typically ≥ committed but can lag behind if there’s a delay in writing logs to storage.

Example Workflow
1. Log Replication:
The leader sends new entries to followers, starting at their Next Index.
When a follower acknowledges an entry, the leader updates the follower's Match Index and advances the Next Index.

2. Log Commitment:
The leader calculates the Committed Index as the highest index replicated on a majority of followers.
Once an entry is committed, it is guaranteed to remain part of the log (safety).

3. State Machine Application:
Each node independently applies committed log entries to its state machine, advancing its Applied Index.

4. Persistence:
Nodes persist log entries to stable storage, updating their Stabled Index.
*/

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"math/rand"
	"sort"
)

// Commit log flow:
// client -> leader Proposal -> 1. leader alone, commit log layer 2. send Peer appendEntries RPC
// -> Peer get RPC compare term and logIndex, send back response
// -> leader commit majority match index, compare term, then update commit log layer

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
	Match, Next uint64 // Match is to calc log's commited index (which is correct value), Next is to get prev index (not always correct, will be rollback)
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

	// msgs need to send, used by RawNode
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

	transferElapsed int

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

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	// Get peers from storage config state when bootstrap
	if c.peers == nil {
		c.peers = confState.Nodes
	}

	rf := new(Raft)
	rf.id = c.ID
	rf.Term = hardState.Term // read term from storage's hardstate
	rf.Vote = hardState.Vote // read vote from storage's hardstate
	rf.RaftLog = newLog(c.Storage)
	rf.Prs = make(map[uint64]*Progress)
	rf.State = StateFollower
	rf.votes = make(map[uint64]bool)
	rf.msgs = nil
	rf.Lead = None
	rf.heartbeatTimeout = c.HeartbeatTick
	rf.electionTimeout = c.ElectionTick
	rf.heartbeatElapsed = 0
	rf.heartbeatElapsed = 0
	rf.leadTransferee = None
	rf.transferElapsed = 0
	rf.PendingConfIndex = rf.RaftLog.getPendingConfIndex()
	rf.randomElectionTimeout = 0

	// initialize peer's index from storage while bootstrap
	lastIndex, _ := c.Storage.LastIndex()
	rf.Prs = rf.initProgresses(c.peers, lastIndex)

	return rf
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// send append log also responsible for sending snapshot once the peer's log is not same as leader's log (For example, leader has updated truncated index, but peer not install it yet, prev index < truncated index )
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)

	if err != nil {
		r.sendSnapshot(to)
		return false
	}

	// send new append logs to given peer
	newLogs := make([]*pb.Entry, 0)

	// Copy new log from leader's log entries
	for _, entry := range r.RaftLog.EntriesFrom(prevLogIndex + 1) {
		newLogs = append(newLogs, &entry)
	}

	// the size of 0 appendLog is acceptable
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: newLogs,
		Commit:  r.RaftLog.committed,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// Commit:  r.RaftLog.committed, // Note: no need to send committed index
	})
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

// handleFollowerTick to check to start an election
func (r *Raft) handleFollowerTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup is local message, don't need RawNode to manage the message
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
	}
}

// handleCandidateTick to check to restart an election
func (r *Raft) handleCandidateTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup is local message, don't need RawNode to manage the message
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		})
	}
}

// handleLeaderTick to check whether to send heartbeat or append log
func (r *Raft) handleLeaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0

		// MessageType_MsgHeartbeat is local message, don't need RawNode to manage the message
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      r.id,
			Term:    r.Term,
		})
	}

	if r.leadTransferee != None {
		// 在选举超时后领导权禅让仍然未完成，则 leader 应该终止领导权禅让，这样可以恢复客户端请求
		r.transferElapsed++
		if r.transferElapsed >= r.electionTimeout {
			r.leadTransferee = None
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.electionElapsed = 0
	r.leadTransferee = None
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

	// initialize next index for peers while elected as leader, only leader needs to maintain Progress state
	lastIndex := r.RaftLog.LastIndex()
	for _, progress := range r.Prs {
		progress.Next = lastIndex + 1
		progress.Match = lastIndex
	}

	r.PendingConfIndex = r.RaftLog.getPendingConfIndex()

	// Note: newly elected leader should append a noop entry on its term
	// This could help to broadcast previous leader's logs in current log entries to all peers
	// And use Step to broadcast message immediately
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
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
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			// should transfer this msg to leader
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
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
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			// should transfer this msg to leader
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.handleTimeoutNow(m)
	}
}

func (r *Raft) handleLeaderStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleLeadershipTransfer(m)
	case pb.MessageType_MsgTimeoutNow:
	}
}

// <----------------------- RPC Message Handler --------------------------->
// Message handler should interact with outside layer through protobuf message

// handleAppendEntries handle AppendEntries RPC request, and reply leader AppendEntriesResponse
// requester: leader
// receiver:  candidate and follower
// sendTo: leader
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	reply := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	}

	// compare role term
	if r.Term > m.Term {
		reply.Reject = true
	} else {
		// set as follower first
		r.becomeFollower(m.Term, m.From)

		// Note: current node raft log should align to leader's log

		// get raft node prev log's term and index from leader's progress
		prevLogTerm, prevLogIndex := m.LogTerm, m.Index
		lastLogIndex := r.RaftLog.LastIndex()

		// compare log index and term
		if lastLogIndex < prevLogIndex {
			// conflict log: last log index is far behind leader's sent index
			// the term could be the same or different
			reply.Reject = true
			// sent last log index to leader as conflict index
			reply.Index = lastLogIndex
		} else {
			// Note: prevLogIndex is the source of truth, which stores in leader's Next,
			// we use this to get conflict Term, the prevLogTerm which passed in, is based on leader's log,
			// this could be incorrect to be used for this follower or candidate
			maybeConflictTerm, _ := r.RaftLog.Term(prevLogIndex) // since prevLogIndex is guaranteed less than LastIndex (curLogIndex), it's safe to ignore err
			if maybeConflictTerm != prevLogTerm {
				// conflict log: term is different
				reply.Reject = true
				reply.Index, _ = r.RaftLog.getStartIndexForATerm(maybeConflictTerm)
			} else {
				if len(m.Entries) > 0 {
					// Note: here is going to append log entries, so we only consider the case len(inputEntries) > 0

					// no conflict: term equal
					// 1. prevLogIndex and lastLogIndex equal, no common part, just append logs to the end of raftLog
					// 2. lastLogIndex > prevLogIndex, (compare interval) we should start from prevLogIndex+1 to skip the common part between raft log and input entries,
					// then truncate rest from raft log and append left input entries

					if lastLogIndex == prevLogIndex {
						r.RaftLog.appendNewEntries(m.Entries)
					} else {
						// The case lastLogIndex > prevLogIndex
						// Example:
						// raftLog [1,2,3], (last = 3) Entries [1,2,3] (prev = 0), i starts at 1, i could reach to entries end while compare each entry, no action
						// raftLog [1,2,3], (last = 3) Entries [1,2,3,4] (prev = 0), i starts at 1, i reach to raft log end, truncate input entries from i, then append to raft log's end
						// raftLog [1,2,(3,3)], (last =3) Entries [1,2,(4,3),(4,4)] (prev = 0), i starts at 1, i stopped in the middle of raftLog and Entries due to conflict Term or Index (diffs at the 3 due to term), truncate raft log and entries at i

						start := prevLogIndex + 1
						i := start
						for ; i <= lastLogIndex && i-start < uint64(len(m.Entries)); i++ {
							logEntry, _ := r.RaftLog.EntryAt(i)
							if logEntry.Term != m.Entries[i-start].Term || logEntry.Index != m.Entries[i-start].Index {
								// find the conflict index i here
								break
							}
						}

						if i-start >= uint64(len(m.Entries)) {
							// reach to end of input entries, no action based on example above
						} else if i > lastLogIndex {
							// reach end of raft log, truncate input entries
							r.RaftLog.appendNewEntries(m.Entries[i-start:])
						} else {
							// i stopped in the middle, i is conflict index, we pull out the common part from RaftLog, and extra data from input entries
							if err := r.RaftLog.truncateEntriesTo(i - 1); err != nil {
								log.Panicf(err.Error())
							}
							r.RaftLog.appendNewEntries(m.Entries[i-start:])

							// Note: because we're truncating the raftLog, the persisted storage layer should be updated,
							r.RaftLog.stabled = min(r.RaftLog.stabled, i-1)
						}
					}

					// Return to leader to set the next index
					reply.Index = r.RaftLog.LastIndex()
				}

				// Note: important! In the sendAppend method, we could send an empty entry to peers, so here we need to accept this case
				// It will happen at the moment of leader get peer's log index, and update commit index if majority index > leader's commit index
				// and then leader send an empty appendEntry rpc to that peer, just to update commit index in this peer as follows.
				reply.Reject = false
				if r.RaftLog.committed < m.Commit {
					// update commit index based on leader's commit index,
					// min of leader's commit log and current log's commit log index + new entries
					r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
				}

			}
		}
	}

	r.msgs = append(r.msgs, reply)
}

// handleAppendEntries handle AppendEntriesResponse RPC request
// requester: candidate and follower, request body {From, To, Reject, Term}
// receiver: leader
// sendTo: none
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
			return
		} else {
			// log conflict: apply new log index for the peer
			r.Prs[m.From].Next = max(m.Index, 1) // Note nextIndex should greater than 0
			// send append log message to peer and sync up log again
			r.sendAppend(m.From)
			return
		}
	} else {
		// update match, and next index for the peer
		if m.Index+1 > r.Prs[m.From].Next {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1

			// check need to update leader's commit index
			mmi := r.getMajorityMatchIndex()
			if r.canLeaderToCommit(mmi) {
				r.RaftLog.committed = mmi

				// Note: since most peers has update Next Index and raftLog, but minority may not update it so far
				// we broadcast to update peer's append log and commit index
				for peer := range r.Prs {
					if peer == r.id {
						continue
					}
					r.sendAppend(peer)
				}
			}
		}

		// transfer leadership to transferee if append log has been updated
		if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgTimeoutNow,
				From:    r.id,
				To:      m.From,
			})
		}
	}
}

// handleBeat handle Beat RPC request
// requester: RawNode
// receiver:  leader
// sendTo: candidate and follower
func (r *Raft) handleBeat(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		r.sendHeartbeat(peer)
	}
}

// handleHeartbeat handle Heartbeat RPC request, to sync up log
// requester: leader
// receiver:  candidate and follower
// sendTo: leader
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	reply := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		//Commit:  r.RaftLog.committed, // no need to send committed index
		Reject: false,
	}

	if r.Term > m.Term {
		reply.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}

	r.msgs = append(r.msgs, reply)
}

// handleHeartbeatResponse handle Heartbeat response RPC request
// requester: follower and candidate
// receiver:  leader
// sendTo: follower
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, None)
	} else {
		// we should use match index to check whether to send append request
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
// requester: leader
// receiver:  follower and candidate
// sendTo: leader
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	reply := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	}

	if r.Term > m.Term {
		reply.Reject = true
	} else if r.RaftLog.committed > m.Snapshot.Metadata.Index {
		reply.Reject = true
		reply.Index = r.RaftLog.committed
	} else {
		r.becomeFollower(m.Term, m.From)
		r.RaftLog.installSnapshot(m.Snapshot)
		// update conf change for peers
		r.Prs = r.initProgresses(m.Snapshot.Metadata.ConfState.Nodes, m.Snapshot.Metadata.Index)

		reply.Index = r.RaftLog.committed
	}

	r.msgs = append(r.msgs, reply)
}

// handleStartElection handle startElection RPC request
// which starts from follower or candidate (timeout restart election), and send Message to rest of peers to request vote
// requester: RawNode, request body {From, To, MsgType}
// receiver:  candidate and follower
// sendTo: MsgAppend -> candidate and follower
func (r *Raft) handleStartElection() {
	// reset vote
	r.votes = make(map[uint64]bool)

	r.becomeCandidate()

	// The case only one server in the raft group
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	prevLogIndex := r.RaftLog.LastIndex()
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

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
			Index:   prevLogIndex, // Require to send log index to peers
			LogTerm: prevLogTerm,
		})
	}
}

// handleRequestVote handle requestVote RPC request
// from candidate, receive by any of roles, we need to return vote response to requester with voteGranted value
// requester: candidate, request body {From, To, MsgType, Term}
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

	// reject due to
	// 1. current term is higher than vote requester's
	// 2. current term is equal to requester's term, but voted for another candidate
	// 3. current peer's raft log is more update to date
	// we need to reject request
	if r.Term > m.Term {
		msg.Reject = true
	} else if r.Term == m.Term {
		if (r.Vote == None || r.Vote == m.From) && !r.isLogMoreUpdateToDate(m.LogTerm, m.Index) {
			// update election timeout for this follower
			r.becomeFollower(m.Term, None)
			r.Vote = m.From
		} else {
			msg.Reject = true
		}
	} else {
		// The case current term is less than request term
		r.becomeFollower(m.Term, None) // since term is lower than requester, become follower

		if r.isLogMoreUpdateToDate(m.LogTerm, m.Index) {
			msg.Reject = true
		} else {
			r.Vote = m.From
		}
	}

	r.msgs = append(r.msgs, msg)
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
// from any of role, and only candidate could get response
// requester: candidate, follower, leader, request body {From, To, MsgType, Reject}
// receiver:  candidate
// sendTo: none
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// align the term
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	if m.Reject {
		r.votes[m.From] = false
	} else {
		r.votes[m.From] = true
	}

	voteCount, notVoteCount, half := 0, 0, len(r.Prs)/2
	for _, v := range r.votes {
		if v {
			voteCount++
		} else {
			notVoteCount++
		}

		if voteCount > half {
			r.becomeLeader()
			break
		} else if notVoteCount > half {
			r.becomeFollower(m.Term, None)
			break
		}
	}
}

// handlePropose handle leader propose RPC request to append log entry and broadcast to peers
// requester: RawNode or becomeLeader, request body: {MsgType, Entries{Data}}
// receiver:  leader
// sendTo: candidate and follower
func (r *Raft) handlePropose(m pb.Message) {
	// save logs
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		if entry.Term == None {
			entry.Term = r.Term
		}
		if entry.Index == None {
			entry.Index = lastIndex + uint64(i) + 1
		}
		if entry.EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entry.Index
		}
	}
	r.RaftLog.appendNewEntries(m.Entries)

	// if it's on transferring leadership, stop broadcasting logs to peers
	// to prevent from starting leader election in append log response with an infinity loop
	if r.leadTransferee != None {
		return
	}

	// update match and next index for leader itself
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// no peer,
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
		return
	}

	// broadcast to peers to commit logsgyyyy6
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		// fail to send peer to append log
		r.sendAppend(peer)
	}
}

// handleLeadershipTransfer handle leadership transfer RPC request
// requester: RawNode, request body: {from, to}
// receiver:  leader
// sendTo: candidate and follower
func (r *Raft) handleLeadershipTransfer(m pb.Message) {
	if r.State != StateLeader || m.From == r.id {
		return
	}

	if _, ok := r.Prs[m.From]; !ok {
		// no transferee
		return
	}

	// update transferee
	r.leadTransferee = m.From

	// just compare index, don't need to take care of term
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		// current leader's log is more update to date, need to help transferee update logs
		r.sendAppend(m.From)
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
		})
	}
}

// handleLeadershipTransfer handle timeout RPC request to start an election immediately
// requester: leader, request body: {from, to}
// receiver:  follower, candidate
// sendTo: none
func (r *Raft) handleTimeoutNow(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		// a node that has been removed from the group, nothing happens.
		return
	}

	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
		From:    r.id,
		To:      r.id,
	})
}

// <----------------------- End Message Handler --------------------------->

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{
		Match: None,
		Next:  None + 1,
	}
	// clear config change
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)

		// Just in case a node have accepted conf change proposal, but the leader haven't commit the previous append log
		// we handle the append logs here
		if r.State == StateLeader {
			mmi := r.getMajorityMatchIndex()
			if r.canLeaderToCommit(mmi) {
				r.RaftLog.committed = mmi

				for peer := range r.Prs {
					if peer == r.id {
						continue
					}
					r.sendAppend(peer)
				}
			}
		}
	}
	// clear config change
	r.PendingConfIndex = None
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) isLogMoreUpdateToDate(term, index uint64) bool {
	prevIndex := r.RaftLog.LastIndex()
	prevTerm, _ := r.RaftLog.Term(prevIndex)

	if prevTerm < term {
		return false
	} else if prevTerm > term {
		return true
	}

	return prevIndex > index
}

func (r *Raft) getMajorityMatchIndex() uint64 {
	matchIndexes := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchIndexes = append(matchIndexes, progress.Match)
	}

	sort.Sort(matchIndexes)

	// get most match index greater than mid-index
	mid := (len(matchIndexes) - 1) / 2
	return matchIndexes[mid]
}

func (r *Raft) canLeaderToCommit(majorityMatchIndex uint64) bool {
	if r.State != StateLeader {
		return false
	}

	matchIndexesTerm, _ := r.RaftLog.Term(majorityMatchIndex)
	// To update commit index's requirement is:
	// leader's commited index is far behind the most match index
	// current leader's term is same as log's term, because Raft won't commit previous log's term
	return r.RaftLog.committed < majorityMatchIndex && r.Term == matchIndexesTerm
}

func (r *Raft) initProgresses(peers []uint64, index uint64) map[uint64]*Progress {
	ps := make(map[uint64]*Progress)

	for _, peer := range peers {
		ps[peer] = &Progress{
			Match: index,
			Next:  index + 1,
		}
	}

	return ps
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err == ErrSnapshotTemporarilyUnavailable {
		// leader use goroutine to async install snap, which is not ready yet, just return,
		// and peer will retry next time by heart beat or propose log
		return
	}
	if err != nil {
		panic(err)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snap,
	})

	// advance next index to prevent resending snapshot
	r.Prs[to].Next = snap.Metadata.Index + 1
}
