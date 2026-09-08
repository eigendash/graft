package kv

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// session is the per-client record that makes retries idempotent.
//
// A client that does not receive a response resends the same command, and the
// linearizable log may contain both copies. Without the session record the
// second copy would execute twice, turning an Append retry into a doubled
// value — the classic exactly-once failure. The session therefore remembers the
// highest sequence number applied for each client, and any command at or below
// it returns the cached answer instead of executing.
type session struct {
	seq  uint64
	last Result
}

// StateMachine is the replicated application. Everything it does is a pure
// function of its state and the command, which is what makes the log a
// specification for the state and snapshots a faithful representation of it.
type StateMachine struct {
	data     map[string]string
	sessions map[uint64]*session
}

// NewStateMachine returns an empty store.
func NewStateMachine() *StateMachine {
	return &StateMachine{data: map[string]string{}, sessions: map[uint64]*session{}}
}

// Apply executes a command and returns its result. It is idempotent for
// commands at or below the client's highest applied sequence number.
func (s *StateMachine) Apply(cmd Command) Result {
	sess := s.sessions[cmd.Client]
	if sess != nil && cmd.Seq <= sess.seq {
		// A duplicate. Returning the cached answer rather than re-executing is
		// what makes a retry indistinguishable from the original.
		return sess.last
	}
	var res Result
	switch cmd.Op {
	case OpGet:
		v, ok := s.data[cmd.Key]
		res = Result{Val: v, Found: ok}
	case OpPut:
		s.data[cmd.Key] = cmd.Val
	case OpAppend:
		s.data[cmd.Key] = s.data[cmd.Key] + cmd.Val
	default:
		res = Result{Err: fmt.Sprintf("unknown operation kind %d", uint8(cmd.Op))}
	}
	if sess == nil {
		sess = &session{}
		s.sessions[cmd.Client] = sess
	}
	sess.seq = cmd.Seq
	sess.last = res
	return res
}

// Result returns the cached answer for a command if it has already been
// applied, so a client can poll for completion without re-proposing.
//
// A command with a sequence number below the client's high-water mark has been
// superseded; reporting the cached answer for the client is the useful
// response, because the ask is "did my request take effect", not "what exactly
// did this older request return".
func (s *StateMachine) Result(client, seq uint64) (Result, bool) {
	sess := s.sessions[client]
	if sess == nil || seq > sess.seq {
		return Result{}, false
	}
	return sess.last, true
}

// Get reads the state machine directly. It exists for tests and for the
// snapshot path; a client's read goes through the log so that it is
// linearizable.
func (s *StateMachine) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

// Len reports the number of keys.
func (s *StateMachine) Len() int { return len(s.data) }

// Clone returns an independent copy. The simulator uses it to take a
// consistent image at an applied index.
func (s *StateMachine) Clone() *StateMachine {
	c := NewStateMachine()
	for k, v := range s.data {
		c.data[k] = v
	}
	for id, sess := range s.sessions {
		cp := *sess
		c.sessions[id] = &cp
	}
	return c
}

// Snapshot serialises the whole state, including the client sessions: a
// restarted server that forgot its sessions would re-execute retries and break
// exactly-once.
func (s *StateMachine) Snapshot() []byte {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	buf := make([]byte, 0, 64+len(keys)*16)
	buf = binary.AppendUvarint(buf, uint64(len(keys)))
	for _, k := range keys {
		buf = appendString(buf, k)
		buf = appendString(buf, s.data[k])
	}
	buf = binary.AppendUvarint(buf, uint64(len(ids)))
	for _, id := range ids {
		sess := s.sessions[id]
		buf = binary.AppendUvarint(buf, id)
		buf = binary.AppendUvarint(buf, sess.seq)
		buf = appendString(buf, sess.last.Val)
		if sess.last.Found {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		buf = appendString(buf, sess.last.Err)
	}
	return buf
}

// Restore replaces the state with a serialised image.
func (s *StateMachine) Restore(data []byte) error {
	if len(data) == 0 {
		s.data = map[string]string{}
		s.sessions = map[uint64]*session{}
		return nil
	}
	b := data
	nkeys, n := binary.Uvarint(b)
	if n <= 0 {
		return fmt.Errorf("kv: snapshot has no key count")
	}
	b = b[n:]
	data2 := make(map[string]string, nkeys)
	for i := uint64(0); i < nkeys; i++ {
		var k, v string
		var err error
		if k, b, err = takeString(b); err != nil {
			return err
		}
		if v, b, err = takeString(b); err != nil {
			return err
		}
		data2[k] = v
	}
	nsess, n := binary.Uvarint(b)
	if n <= 0 {
		return fmt.Errorf("kv: snapshot has no session count")
	}
	b = b[n:]
	sessions := make(map[uint64]*session, nsess)
	for i := uint64(0); i < nsess; i++ {
		id, n := binary.Uvarint(b)
		if n <= 0 {
			return fmt.Errorf("kv: truncated session id")
		}
		b = b[n:]
		seq, n := binary.Uvarint(b)
		if n <= 0 {
			return fmt.Errorf("kv: truncated session sequence")
		}
		b = b[n:]
		var res Result
		var err error
		if res.Val, b, err = takeString(b); err != nil {
			return err
		}
		if len(b) < 1 {
			return fmt.Errorf("kv: truncated session found flag")
		}
		res.Found = b[0] == 1
		b = b[1:]
		if res.Err, b, err = takeString(b); err != nil {
			return err
		}
		sessions[id] = &session{seq: seq, last: res}
	}
	if len(b) != 0 {
		return fmt.Errorf("kv: %d trailing bytes in snapshot", len(b))
	}
	s.data, s.sessions = data2, sessions
	return nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

func takeString(b []byte) (string, []byte, error) {
	n, k := binary.Uvarint(b)
	if k <= 0 {
		return "", nil, fmt.Errorf("kv: truncated string length")
	}
	b = b[k:]
	if uint64(len(b)) < n {
		return "", nil, fmt.Errorf("kv: string length %d exceeds the %d remaining bytes", n, len(b))
	}
	return string(b[:n]), b[n:], nil
}
