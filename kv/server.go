package kv

import (
	"fmt"

	"github.com/eigendash/graft/raft"
)

// Server ties a consensus node to a state machine. It is the only place where
// the two meet, and it contains no concurrency and no clock: the caller drives
// it by calling Poll, exactly as it drives the raft.Node.
type Server struct {
	node *raft.Node
	sm   *StateMachine

	// snapshotThreshold is how many applied entries may accumulate past the
	// last snapshot before Poll compacts. Compaction is driven from here
	// rather than from a timer because the state machine image can only be
	// taken at an applied index.
	snapshotThreshold uint64
	snapshots         int
}

// NewServer wraps node. A snapshotThreshold of zero disables compaction.
func NewServer(node *raft.Node, sm *StateMachine, snapshotThreshold uint64) *Server {
	if sm == nil {
		sm = NewStateMachine()
	}
	return &Server{node: node, sm: sm, snapshotThreshold: snapshotThreshold}
}

// Node exposes the underlying consensus node.
func (s *Server) Node() *raft.Node { return s.node }

// StateMachine exposes the application state.
func (s *Server) StateMachine() *StateMachine { return s.sm }

// Snapshots reports how many times the log has been compacted.
func (s *Server) Snapshots() int { return s.snapshots }

// Propose sends a command through the log. It fails with raft.ErrNotLeader if
// this node is not the leader, which is the caller's signal to retry
// elsewhere.
func (s *Server) Propose(cmd Command) error {
	_, _, err := s.node.Propose(cmd.Encode())
	return err
}

// ProposeConfChange is passed through to the consensus layer.
func (s *Server) ProposeConfChange(cc raft.ConfChange) error {
	_, _, err := s.node.ProposeConfChange(cc)
	return err
}

// Poll applies every newly committed entry to the state machine, installs any
// snapshot the leader sent, compacts when the log has grown far enough, and
// returns the messages the node wants to send.
//
// Order matters: the snapshot must be installed before the committed entries
// that follow it, and Advance must not run until the entries have actually been
// applied, because Advance is what tells the consensus layer it may compact.
func (s *Server) Poll() ([]raft.Message, error) {
	r := s.node.Ready()
	if r.Snapshot != nil {
		if err := s.sm.Restore(r.Snapshot.Data); err != nil {
			return nil, fmt.Errorf("kv: installing snapshot at %d: %w", r.Snapshot.Index, err)
		}
	}
	for _, e := range r.CommittedEntries {
		if e.Type != raft.EntryNormal || len(e.Data) == 0 {
			// The leader's no-op and configuration changes carry no command.
			continue
		}
		cmd, err := DecodeCommand(e.Data)
		if err != nil {
			return nil, fmt.Errorf("kv: entry %d/%d: %w", e.Term, e.Index, err)
		}
		s.sm.Apply(cmd)
	}
	s.node.Advance()

	if s.snapshotThreshold > 0 {
		applied := s.node.Applied()
		if snap := s.node.Snapshot(); snap == nil || applied-snap.Index >= raft.Index(s.snapshotThreshold) {
			if _, err := s.node.CreateSnapshot(s.sm.Snapshot()); err != nil {
				return nil, fmt.Errorf("kv: compacting at %d: %w", applied, err)
			}
			s.snapshots++
		}
	}
	return r.Messages, nil
}
