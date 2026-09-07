package raft

import (
	"encoding/binary"
	"fmt"
)

// ConfChangeType is the kind of single-server membership change.
type ConfChangeType uint8

const (
	ConfAddNode ConfChangeType = iota + 1
	ConfRemoveNode
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfAddNode:
		return "add"
	case ConfRemoveNode:
		return "remove"
	default:
		return fmt.Sprintf("ConfChangeType(%d)", uint8(t))
	}
}

// ConfChange adds or removes exactly one voter.
//
// This implementation supports single-server changes only. Joint consensus is
// strictly more general but needs a two-phase configuration (C_old,new) that
// has to survive compaction as its own kind of entry; single-server changes are
// provably safe on their own because the old and new quorums always intersect.
type ConfChange struct {
	Type ConfChangeType
	Node NodeID
}

func (cc ConfChange) String() string { return fmt.Sprintf("%s(%d)", cc.Type, cc.Node) }

// Encode is a frozen fixed-width little-endian record rather than a
// reflection-based codec: the bytes are replicated inside log entries and must
// stay decodable for the lifetime of a cluster.
func (cc ConfChange) Encode() []byte {
	b := make([]byte, 9)
	b[0] = byte(cc.Type)
	binary.LittleEndian.PutUint64(b[1:], uint64(cc.Node))
	return b
}

// DecodeConfChange is the inverse of Encode. It rejects anything it did not
// produce, because a mis-decoded configuration change would silently change
// the quorum size.
func DecodeConfChange(b []byte) (ConfChange, error) {
	if len(b) != 9 {
		return ConfChange{}, fmt.Errorf("raft: conf change payload is %d bytes, want 9", len(b))
	}
	cc := ConfChange{Type: ConfChangeType(b[0]), Node: NodeID(binary.LittleEndian.Uint64(b[1:]))}
	if cc.Type != ConfAddNode && cc.Type != ConfRemoveNode {
		return ConfChange{}, fmt.Errorf("raft: unknown conf change type %d", b[0])
	}
	if cc.Node == 0 {
		return ConfChange{}, fmt.Errorf("raft: conf change names node 0, which is reserved")
	}
	return cc, nil
}
