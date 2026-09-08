// Package kv implements a linearizable key-value store on top of the
// replicated log.
//
// The store is split in two: StateMachine is a pure, deterministic function
// from (state, command) to (state', result) with no I/O and no clock, and
// Server is the thin adapter that turns raft.Ready into calls on that state
// machine. Keeping the split sharp is what lets the simulator checkpoint and
// restore a server at an arbitrary log index without involving the consensus
// layer.
package kv

import (
	"encoding/binary"
	"fmt"
)

// OpKind is the operation a command performs.
type OpKind uint8

const (
	OpGet OpKind = iota
	OpPut
	OpAppend
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpAppend:
		return "append"
	default:
		return fmt.Sprintf("OpKind(%d)", uint8(k))
	}
}

// Command is the client request that travels through the log.
//
// Client and Seq together are the request identifier. Including them in the
// replicated command rather than in a side channel is what makes retries safe:
// a resent command is byte-identical, so any server that applies it twice can
// recognise the duplicate from the log alone.
type Command struct {
	Client uint64
	Seq    uint64
	Op     OpKind
	Key    string
	Val    string
}

func (c Command) String() string {
	if c.Op == OpGet || c.Op == OpAppend && c.Val == "" {
		return fmt.Sprintf("c%d#%d %s(%s)", c.Client, c.Seq, c.Op, c.Key)
	}
	return fmt.Sprintf("c%d#%d %s(%s,%q)", c.Client, c.Seq, c.Op, c.Key, c.Val)
}

// Result is the outcome of applying a command.
type Result struct {
	Val   string
	Found bool
	// Err is reserved for a command that could not be interpreted at all. It
	// is part of the replicated result so that a retry observes the same
	// answer as the original attempt.
	Err string
}

func (r Result) String() string {
	if r.Err != "" {
		return "err:" + r.Err
	}
	if !r.Found {
		return "<absent>"
	}
	return fmt.Sprintf("%q", r.Val)
}

// Encode renders a command as a self-describing byte string. It is a
// length-prefixed binary record rather than a reflective codec because the
// bytes are replicated and must stay decodable for the life of a cluster.
func (c Command) Encode() []byte {
	buf := make([]byte, 0, 20+len(c.Key)+len(c.Val))
	buf = binary.AppendUvarint(buf, c.Client)
	buf = binary.AppendUvarint(buf, c.Seq)
	buf = append(buf, byte(c.Op))
	buf = binary.AppendUvarint(buf, uint64(len(c.Key)))
	buf = append(buf, c.Key...)
	buf = binary.AppendUvarint(buf, uint64(len(c.Val)))
	buf = append(buf, c.Val...)
	return buf
}

// DecodeCommand is the inverse of Encode. It rejects anything malformed rather
// than guessing, because a mis-decoded command would be applied to the state
// machine as if it were real.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	client, n := binary.Uvarint(b)
	if n <= 0 {
		return c, fmt.Errorf("kv: truncated client id")
	}
	b = b[n:]
	seq, n := binary.Uvarint(b)
	if n <= 0 {
		return c, fmt.Errorf("kv: truncated sequence number")
	}
	b = b[n:]
	if len(b) < 1 {
		return c, fmt.Errorf("kv: truncated operation kind")
	}
	c.Client, c.Seq, c.Op = client, seq, OpKind(b[0])
	b = b[1:]
	keyLen, n := binary.Uvarint(b)
	if n <= 0 {
		return c, fmt.Errorf("kv: truncated key length")
	}
	b = b[n:]
	if uint64(len(b)) < keyLen {
		return c, fmt.Errorf("kv: key length %d exceeds the %d remaining bytes", keyLen, len(b))
	}
	c.Key = string(b[:keyLen])
	b = b[keyLen:]
	valLen, n := binary.Uvarint(b)
	if n <= 0 {
		return c, fmt.Errorf("kv: truncated value length")
	}
	b = b[n:]
	if uint64(len(b)) < valLen {
		return c, fmt.Errorf("kv: value length %d exceeds the %d remaining bytes", valLen, len(b))
	}
	c.Val = string(b[:valLen])
	if len(b) != int(valLen) {
		return c, fmt.Errorf("kv: %d trailing bytes after the command", len(b)-int(valLen))
	}
	if c.Op > OpAppend {
		return c, fmt.Errorf("kv: unknown operation kind %d", uint8(c.Op))
	}
	return c, nil
}
