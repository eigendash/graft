package raft

import "fmt"

// NodeID identifies a cluster member. Zero is reserved to mean "nobody" — no
// leader, no vote — so real identifiers must start at one. Reserving zero
// rather than using a separate optional field keeps HardState comparable and
// makes an unset vote indistinguishable from "no vote" after a reload.
type NodeID uint64

// Term is a Raft term number, monotonically increasing across the cluster.
type Term uint64

// Index is a one-based position in the replicated log. Zero means "before the
// first entry", which is the prevLogIndex a leader sends to an empty follower.
type Index uint64

// StateType is the role a node currently plays in the protocol.
type StateType uint8

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

func (s StateType) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return fmt.Sprintf("StateType(%d)", uint8(s))
	}
}

// EntryType distinguishes application payloads from cluster configuration.
type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

func (t EntryType) String() string {
	switch t {
	case EntryNormal:
		return "normal"
	case EntryConfChange:
		return "confchange"
	default:
		return fmt.Sprintf("EntryType(%d)", uint8(t))
	}
}

// Entry is one replicated log record.
//
// Data is treated as immutable for the whole lifetime of the entry: the same
// backing array may be visible to several nodes and to the caller through
// Ready, so any mutation would silently corrupt a "copy" that shares storage.
type Entry struct {
	Term  Term
	Index Index
	Type  EntryType
	Data  []byte
}

func (e Entry) String() string {
	return fmt.Sprintf("%d/%d(%s)", e.Term, e.Index, e.Type)
}

// MessageType enumerates the wire protocol.
type MessageType uint8

const (
	// MsgVote requests a vote for a term. LogTerm and Index carry the
	// candidate's last log position, which is what the §5.4.1 up-to-date check
	// compares against.
	MsgVote MessageType = iota
	MsgVoteResp
	// MsgApp is AppendEntries. It doubles as the heartbeat when Entries is
	// empty, and as a probe when a leader has lost track of a follower.
	MsgApp
	MsgAppResp
	// MsgSnap is InstallSnapshot. It is only sent when the leader has already
	// compacted the entries a follower would need.
	MsgSnap
	MsgSnapResp
)

func (t MessageType) String() string {
	switch t {
	case MsgVote:
		return "MsgVote"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgApp:
		return "MsgApp"
	case MsgAppResp:
		return "MsgAppResp"
	case MsgSnap:
		return "MsgSnap"
	case MsgSnapResp:
		return "MsgSnapResp"
	default:
		return fmt.Sprintf("MessageType(%d)", uint8(t))
	}
}

// Message is the unit of communication. It is passed by value and, like Entry,
// is never mutated after being produced.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// LogTerm and Index are prevLogTerm/prevLogIndex on MsgApp and the
	// candidate's last log position on MsgVote.
	LogTerm Term
	Index   Index

	// Commit is the leader's commit index, piggybacked on MsgApp so followers
	// learn where it is safe to apply.
	Commit Index

	Entries  []Entry
	Snapshot *Snapshot

	// Reject marks a negative MsgAppResp/MsgVoteResp.
	Reject bool
	// RejectHint is the follower's suggested next prevLogIndex+1 after a
	// rejection: the first index of the conflicting term, or its own last
	// index plus one when it simply does not have the requested entry. It lets
	// the leader skip back in one round trip instead of one entry at a time.
	RejectHint Index
}

func (m Message) String() string {
	s := fmt.Sprintf("%s %d->%d term=%d", m.Type, m.From, m.To, m.Term)
	switch m.Type {
	case MsgApp:
		s += fmt.Sprintf(" prev=%d/%d n=%d commit=%d", m.LogTerm, m.Index, len(m.Entries), m.Commit)
	case MsgVote:
		s += fmt.Sprintf(" last=%d/%d", m.LogTerm, m.Index)
	case MsgAppResp:
		s += fmt.Sprintf(" index=%d", m.Index)
		if m.Reject {
			s += fmt.Sprintf(" reject hint=%d", m.RejectHint)
		}
	case MsgSnap:
		if m.Snapshot != nil {
			s += fmt.Sprintf(" snap=%d/%d", m.Snapshot.Term, m.Snapshot.Index)
		}
	}
	if m.Reject && m.Type != MsgAppResp {
		s += " reject"
	}
	return s
}
