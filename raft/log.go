package raft

import (
	"errors"
	"fmt"
)

var (
	// ErrCompacted means the requested index has already been folded into a
	// snapshot and is no longer addressable.
	ErrCompacted = errors.New("raft: index compacted into snapshot")
	// ErrUnavailable means the requested index is past the end of the log.
	ErrUnavailable = errors.New("raft: index past end of log")
	// ErrNotLeader is returned by Propose on a non-leader.
	ErrNotLeader = errors.New("raft: not the leader")
)

// raftLog is the in-memory view of the replicated log. Entries at or below
// snapshotIndex live only inside the snapshot, so this structure always holds
// a suffix: entries (snapshotIndex, lastIndex].
//
// The log is kept in memory in full because Raft's conflict resolution needs
// random access to a *suffix* (to compute a backtracking hint) and because
// compaction, not paging, is the mechanism that bounds it. Keeping it resident
// makes the whole core allocation-free on the read path, which is what lets the
// simulator run hundreds of thousands of ticks per second.
type raftLog struct {
	snapshotIndex Index
	snapshotTerm  Term
	// ents[i] holds index snapshotIndex+1+i.
	ents []Entry
	// committed is the highest index known to be committed cluster-wide.
	committed Index
	// applied is the highest index handed to the state machine.
	applied Index
}

func newRaftLog(snapshotIndex Index, snapshotTerm Term, ents []Entry) *raftLog {
	l := &raftLog{snapshotIndex: snapshotIndex, snapshotTerm: snapshotTerm}
	if len(ents) > 0 {
		l.ents = append([]Entry(nil), ents...)
	}
	return l
}

func (l *raftLog) firstIndex() Index { return l.snapshotIndex + 1 }

func (l *raftLog) lastIndex() Index { return l.snapshotIndex + Index(len(l.ents)) }

func (l *raftLog) lastTerm() Term {
	if len(l.ents) == 0 {
		return l.snapshotTerm
	}
	return l.ents[len(l.ents)-1].Term
}

// term reports the term of the entry at index i. Index 0 is defined to have
// term 0 so that a leader can address the empty log with prevLogIndex 0 and
// prevLogTerm 0, which is the only self-consistent way to say "match nothing
// and start from the beginning".
func (l *raftLog) term(i Index) (Term, error) {
	switch {
	case i == 0:
		return 0, nil
	case i == l.snapshotIndex:
		return l.snapshotTerm, nil
	case i < l.snapshotIndex:
		return 0, fmt.Errorf("%w: index %d < snapshot %d", ErrCompacted, i, l.snapshotIndex)
	case i > l.lastIndex():
		return 0, fmt.Errorf("%w: index %d > last %d", ErrUnavailable, i, l.lastIndex())
	}
	return l.ents[i-l.snapshotIndex-1].Term, nil
}

// matchTerm is the Log Matching check from §5.3: the entries agree at index i
// and have term t.
func (l *raftLog) matchTerm(i Index, t Term) bool {
	if i == 0 {
		return t == 0
	}
	got, err := l.term(i)
	return err == nil && got == t
}

// firstIndexInTerm returns the lowest index holding an entry of term t, or 0
// if the log has no such entry. Entries are sorted by term, so this is a
// binary search.
func (l *raftLog) firstIndexInTerm(t Term) Index {
	lo, hi := 0, len(l.ents)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if l.ents[mid].Term < t {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(l.ents) && l.ents[lo].Term == t {
		return l.ents[lo].Index
	}
	if t == l.snapshotTerm {
		return l.snapshotIndex
	}
	return 0
}

// findConflict returns 0 when every entry in ents is already present with the
// same term, and otherwise the index of the first entry that differs. It is
// how a follower decides where the leader's log starts to diverge.
func (l *raftLog) findConflict(ents []Entry) Index {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// slice returns entries [lo, hi). The result aliases the log's backing array;
// callers must treat it as read-only and must not retain it across a mutation.
func (l *raftLog) slice(lo, hi Index) ([]Entry, error) {
	if lo < l.firstIndex() {
		return nil, fmt.Errorf("%w: slice from %d before first %d", ErrCompacted, lo, l.firstIndex())
	}
	if hi > l.lastIndex()+1 {
		return nil, fmt.Errorf("%w: slice to %d past last %d", ErrUnavailable, hi, l.lastIndex())
	}
	if lo >= hi {
		return nil, nil
	}
	return l.ents[lo-l.snapshotIndex-1 : hi-l.snapshotIndex-1], nil
}

// truncateAndAppend installs ents starting at index `from`, discarding any
// existing suffix from that point. The caller is responsible for having proven
// that the discarded suffix is uncommitted; that proof is what makes the
// Leader Completeness Property hold.
func (l *raftLog) truncateAndAppend(from Index, ents []Entry) {
	if len(ents) == 0 {
		if from == 0 || from <= l.snapshotIndex || from > l.lastIndex() {
			return
		}
		l.ents = l.ents[:from-l.snapshotIndex-1]
		return
	}
	if from == 0 {
		// from == 0 is the "append at the end" form, used when a leader seals a
		// new entry and there is nothing to replace.
		if ents[0].Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("raft: log gap: appending at %d after last index %d", ents[0].Index, l.lastIndex()))
		}
		l.ents = append(l.ents, ents...)
		return
	}
	off := from - l.snapshotIndex - 1
	if off > Index(len(l.ents)) {
		panic(fmt.Sprintf("raft: log gap: appending at %d with last index %d", from, l.lastIndex()))
	}
	l.ents = append(l.ents[:off], ents...)
}

// compactTo discards everything up to and including index i. Only the
// snapshotting caller, which has just persisted an image covering i, may do
// this.
func (l *raftLog) compactTo(i Index, t Term) {
	if i <= l.snapshotIndex {
		return
	}
	if i > l.lastIndex() {
		panic(fmt.Sprintf("raft: compact to %d beyond last index %d", i, l.lastIndex()))
	}
	l.ents = append([]Entry(nil), l.ents[i-l.snapshotIndex:]...)
	l.snapshotIndex, l.snapshotTerm = i, t
}

// restore replaces the entire log with a snapshot. Any uncommitted suffix is
// dropped: the snapshot is authoritative for everything up to its index, and
// the leader will re-send anything after it.
func (l *raftLog) restore(s Snapshot) {
	l.snapshotIndex, l.snapshotTerm = s.Index, s.Term
	l.ents = nil
	if l.committed < s.Index {
		l.committed = s.Index
	}
	l.applied = s.Index
}

// isUpToDate implements the §5.4.1 election restriction: a voter refuses a
// candidate whose log is less complete than its own, comparing last term
// first.
func (l *raftLog) isUpToDate(lastIndex Index, lastTerm Term) bool {
	myTerm := l.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= l.lastIndex()
}

// entriesFrom returns a copy of the in-memory log, used by invariant checks.
func (l *raftLog) entriesFrom() []Entry {
	return append([]Entry(nil), l.ents...)
}
