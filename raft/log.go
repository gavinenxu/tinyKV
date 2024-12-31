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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

var (
	ErrInvalidIndex = errors.New("invalid index")
	ErrInvalidTerm  = errors.New("invalid term")
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64 // To record the majority raft peer's log last index, log entry index submitted on memory

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64 // The log index applied to the upper application layer (state machine) on memory

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64 // The index to keep track of how many logs put into storage layer

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	raftLog := new(RaftLog)
	raftLog.storage = storage
	raftLog.applied = firstIndex - 1
	raftLog.committed = hardState.Commit
	raftLog.stabled = lastIndex
	raftLog.dummyIndex = firstIndex
	raftLog.entries = entries
	raftLog.pendingSnapshot = new(pb.Snapshot)

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).

	firstIndex, _ := l.storage.FirstIndex()
	if firstIndex > l.dummyIndex {
		// copy not truncated entries
		entries := l.entries[firstIndex-l.dummyIndex:]
		l.entries = make([]pb.Entry, len(entries))
		copy(l.entries, entries)
	}

	l.dummyIndex = firstIndex
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entriesFrom(l.stabled + 1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed > l.applied {
		for i := l.applied + 1 - l.dummyIndex; i <= l.committed-l.dummyIndex; i++ {
			ents = append(ents, l.entries[i])
		}
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < 0 || i > l.LastIndex() {
		return None, ErrInvalidIndex
	}

	if i < l.dummyIndex {
		if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
			// get from snapshot
			return l.pendingSnapshot.Metadata.Term, nil
		} else {
			// get from storage
			return l.storage.Term(i)
		}

	} else {
		return l.entries[i-l.dummyIndex].Term, nil
	}
}

func (l *RaftLog) entryAt(i uint64) (pb.Entry, error) {
	if i < 0 || i > l.LastIndex() {
		return pb.Entry{}, ErrInvalidIndex
	}

	return l.entries[i-l.dummyIndex], nil
}

// EntriesFrom return the global index of log entries from start index
func (l *RaftLog) entriesFrom(startIndex uint64) []pb.Entry {
	if startIndex < l.dummyIndex {
		log.Panicf("index out of range: %d", startIndex)
	}

	return l.entries[startIndex-l.dummyIndex:]
}

func (l *RaftLog) appendNewEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
}

func (l *RaftLog) truncateEntriesTo(i uint64) error {
	if i < 0 || i > l.LastIndex() {
		return ErrInvalidIndex
	}

	l.entries = l.entries[:i-l.dummyIndex+1]
	return nil
}

func (l *RaftLog) getStartIndexForATerm(term uint64) (uint64, error) {
	for _, entry := range l.entries {
		if entry.Term == term {
			return entry.Index, nil
		}

		if entry.Term > term {
			break
		}
	}

	return None, ErrInvalidTerm
}

func (l *RaftLog) installSnapshot(snap *pb.Snapshot) {
	l.applied = snap.Metadata.Index
	l.committed = snap.Metadata.Index
	l.stabled = snap.Metadata.Index
	l.dummyIndex = snap.Metadata.Index + 1
	l.entries = make([]pb.Entry, 0)
	l.pendingSnapshot = snap
}

func (l *RaftLog) getPendingConfIndex() uint64 {
	for i := l.applied + 1; i < l.LastIndex(); i++ {
		entry, _ := l.entryAt(i)
		if entry.EntryType == pb.EntryType_EntryConfChange {
			return i
		}
	}
	return None
}
