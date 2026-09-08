package raft

import (
	"errors"
	"fmt"
	"sort"
)

// HardState is the durable part of Figure 2's "persistent state on all
// servers". Every field here must reach stable storage before the node acts on
// it: a vote must be durable before it is answered, or a crash could let the
// node vote twice in one term and elect two leaders.
type HardState struct {
	Term   Term
	Vote   NodeID
	Commit Index
}

func (h HardState) String() string {
	return fmt.Sprintf("term=%d vote=%d commit=%d", h.Term, h.Vote, h.Commit)
}

// Snapshot is the point at which the log has been compacted together with the
// state machine image that replaces the discarded prefix of the log.
type Snapshot struct {
	Index Index
	Term  Term
	// Voters is the membership as of Index. A node restored from a snapshot
	// has no log entries describing how the cluster reached that membership,
	// so the membership has to travel with the image.
	Voters []NodeID
	Data   []byte
}

func (s Snapshot) String() string {
	return fmt.Sprintf("snapshot term=%d index=%d voters=%v bytes=%d", s.Term, s.Index, s.Voters, len(s.Data))
}

// Clone returns a deep copy so that a caller cannot mutate storage-owned
// memory through a returned snapshot.
func (s *Snapshot) Clone() *Snapshot {
	if s == nil {
		return nil
	}
	c := *s
	c.Voters = append([]NodeID(nil), s.Voters...)
	c.Data = append([]byte(nil), s.Data...)
	return &c
}

// LogMutation is a single atomic change to the persisted log.
//
// Truncation and appending are expressed as one operation because Raft's
// conflict resolution replaces a suffix: doing it as two durable operations
// would leave a window in which a crash loses an entry that was already
// acknowledged.
type LogMutation struct {
	// TruncateFrom, when non-zero, discards every persisted entry with an
	// index >= TruncateFrom. Raft only truncates a suffix it has proven is not
	// committed, so this can never drop a committed entry.
	TruncateFrom Index
	// Entries are appended after any truncation. They must be contiguous and
	// begin at TruncateFrom, or at the current end of the persisted log.
	Entries []Entry
}

// PersistentState is everything a node needs to resume after a crash. It
// deliberately excludes commit progress that has not been persisted: a
// restarted node must rediscover the commit index from the leader, which is
// safe because commit indices only ever move forward.
type PersistentState struct {
	HardState HardState
	Entries   []Entry
	Snapshot  *Snapshot
}

// Storage is the durability boundary. Implementations may be a write-ahead log
// on disk, an in-memory slab in a simulator, or anything else; the consensus
// core only requires the documented ordering.
//
// The core calls these methods synchronously and never in parallel, so an
// implementation need not be safe for concurrent use.
type Storage interface {
	// Load is called once, before the node starts. Entries must be the
	// contiguous run immediately after Snapshot (or from index 1 when there is
	// no snapshot).
	Load() (PersistentState, error)
	// SaveHardState must be durable before it returns.
	SaveHardState(HardState) error
	// SaveLog applies a mutation atomically.
	SaveLog(LogMutation) error
	// SaveSnapshot stores an image this node produced itself by compacting its
	// log. Entries at or below the image's index are subsumed and may be
	// discarded; entries above it are still part of this node's log and must
	// be kept.
	SaveSnapshot(Snapshot) error
	// InstallSnapshot stores an image received from the leader. It is a
	// different operation from SaveSnapshot because it invalidates the entire
	// local log: the leader sent this image precisely because the suffix this
	// node holds is not the right one. Treating the two as one operation is
	// what lets a stale suffix survive a restart and be replayed against a
	// snapshot that already supersedes it.
	InstallSnapshot(Snapshot) error
}

// ErrCorruptState is returned by a Storage implementation that finds persisted
// state it cannot interpret.
var ErrCorruptState = errors.New("raft: corrupt persisted state")

// MemoryStorage is a Storage that keeps everything in memory. It is what the
// simulator uses — persistence across a simulated crash is exactly "the same
// MemoryStorage survives while the raft.Node does not" — and it is a convenient
// fixture for tests.
//
// Every value it hands out or accepts is copied at the boundary, so callers
// can neither observe nor corrupt its internal state.
type MemoryStorage struct {
	hardState HardState
	entries   []Entry
	snapshot  *Snapshot

	// Saves counts durable writes so that tests and benchmarks can measure how
	// much write amplification the protocol produces.
	Saves int
}

// NewMemoryStorage returns an empty store.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

// Load implements Storage.
func (m *MemoryStorage) Load() (PersistentState, error) {
	ps := PersistentState{HardState: m.hardState, Snapshot: m.snapshot.Clone()}
	if len(m.entries) > 0 {
		ps.Entries = append([]Entry(nil), m.entries...)
	}
	return ps, nil
}

// SaveHardState implements Storage.
func (m *MemoryStorage) SaveHardState(hs HardState) error {
	m.hardState = hs
	m.Saves++
	return nil
}

// SaveLog implements Storage.
func (m *MemoryStorage) SaveLog(mut LogMutation) error {
	m.Saves++
	if mut.TruncateFrom != 0 {
		base := Index(0)
		if m.snapshot != nil {
			base = m.snapshot.Index
		}
		if mut.TruncateFrom <= base {
			return fmt.Errorf("%w: truncate from %d at or below snapshot index %d",
				ErrCorruptState, mut.TruncateFrom, base)
		}
		off := int(mut.TruncateFrom - base - 1)
		if off < len(m.entries) {
			m.entries = m.entries[:off]
		}
	}
	if len(mut.Entries) > 0 {
		want := Index(1)
		if m.snapshot != nil {
			want = m.snapshot.Index + 1
		}
		if n := len(m.entries); n > 0 {
			want = m.entries[n-1].Index + 1
		}
		if mut.Entries[0].Index != want {
			return fmt.Errorf("%w: append starts at %d, want %d",
				ErrCorruptState, mut.Entries[0].Index, want)
		}
		m.entries = append(m.entries, mut.Entries...)
	}
	return nil
}

// SaveSnapshot implements Storage.
func (m *MemoryStorage) SaveSnapshot(s Snapshot) error {
	m.Saves++
	c := s.Clone()
	m.snapshot = c
	kept := m.entries[:0:0]
	for _, e := range m.entries {
		if e.Index > s.Index {
			kept = append(kept, e)
		}
	}
	m.entries = kept
	if m.hardState.Commit < s.Index {
		m.hardState.Commit = s.Index
	}
	return nil
}

// InstallSnapshot implements Storage.
func (m *MemoryStorage) InstallSnapshot(s Snapshot) error {
	m.Saves++
	m.snapshot = s.Clone()
	m.entries = nil
	if m.hardState.Commit < s.Index {
		m.hardState.Commit = s.Index
	}
	return nil
}

// Clone returns an independent copy, used by tests that need to distinguish
// "the disk survived" from "the process survived".
func (m *MemoryStorage) Clone() *MemoryStorage {
	c := &MemoryStorage{hardState: m.hardState, snapshot: m.snapshot.Clone(), Saves: m.Saves}
	c.entries = append([]Entry(nil), m.entries...)
	return c
}

// SortedVoters returns a sorted copy of ids. Membership sets are compared and
// iterated in sorted order everywhere in this package so that the sequence of
// emitted messages — and therefore an entire simulation run — is a pure
// function of the inputs.
func SortedVoters(ids []NodeID) []NodeID {
	out := append([]NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	uniq := out[:0]
	for i, id := range out {
		if i > 0 && out[i-1] == id {
			continue
		}
		uniq = append(uniq, id)
	}
	return uniq
}
