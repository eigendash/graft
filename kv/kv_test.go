package kv

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/eigendash/graft/raft"
)

func TestCommandEncodingRoundTrip(t *testing.T) {
	cases := []Command{
		{Client: 0, Seq: 0, Op: OpGet, Key: "", Val: ""},
		{Client: 1, Seq: 2, Op: OpPut, Key: "k", Val: "v"},
		{Client: 1 << 40, Seq: 1 << 41, Op: OpAppend, Key: "hello world", Val: "\x00\xff binary"},
	}
	for _, want := range cases {
		got, err := DecodeCommand(want.Encode())
		if err != nil {
			t.Fatalf("DecodeCommand(%v): %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip changed %v into %v", want, got)
		}
	}
}

func TestCommandDecodingRejectsMalformedInput(t *testing.T) {
	good := Command{Client: 7, Seq: 8, Op: OpPut, Key: "ab", Val: "cd"}.Encode()
	for _, cut := range []int{0, 1, 2, 3, 4, 5} {
		if _, err := DecodeCommand(good[:cut]); err == nil {
			t.Errorf("DecodeCommand accepted a command truncated to %d bytes", cut)
		}
	}
	trailing := append(append([]byte(nil), good...), 0x00)
	if _, err := DecodeCommand(trailing); err == nil {
		t.Error("DecodeCommand accepted trailing bytes")
	}
	badKind := Command{Client: 1, Seq: 1, Op: OpKind(9)}.Encode()
	if _, err := DecodeCommand(badKind); err == nil {
		t.Error("DecodeCommand accepted an unknown operation kind")
	}
}

func TestStateMachineSemantics(t *testing.T) {
	sm := NewStateMachine()
	if got := sm.Apply(Command{Client: 1, Seq: 1, Op: OpGet, Key: "x"}); got.Found {
		t.Fatalf("Get on an empty store returned %v", got)
	}
	sm.Apply(Command{Client: 1, Seq: 2, Op: OpAppend, Key: "x", Val: "a"})
	sm.Apply(Command{Client: 1, Seq: 3, Op: OpAppend, Key: "x", Val: "b"})
	if got := sm.Apply(Command{Client: 1, Seq: 4, Op: OpGet, Key: "x"}); got.Val != "ab" || !got.Found {
		t.Fatalf("after two appends, Get returned %v", got)
	}
	sm.Apply(Command{Client: 1, Seq: 5, Op: OpPut, Key: "x", Val: "z"})
	if got := sm.Apply(Command{Client: 1, Seq: 6, Op: OpGet, Key: "x"}); got.Val != "z" {
		t.Fatalf("after a Put, Get returned %v", got)
	}
	bad := sm.Apply(Command{Client: 2, Seq: 1, Op: OpKind(42)})
	if bad.Err == "" {
		t.Error("an unknown operation kind produced no error result")
	}
}

// TestRetriesAreIdempotent is the property that makes a client's resend safe.
// Without the session record the second Append would be applied twice and the
// value would be doubled.
func TestRetriesAreIdempotent(t *testing.T) {
	sm := NewStateMachine()
	cmd := Command{Client: 9, Seq: 1, Op: OpAppend, Key: "k", Val: "x"}
	for i := 0; i < 5; i++ {
		sm.Apply(cmd)
	}
	if v, _ := sm.Get("k"); v != "x" {
		t.Fatalf("applying the same command five times produced %q, want %q", v, "x")
	}

	// A Get retry must return the answer from the first attempt even after the
	// value has changed underneath it, because that is what the client saw.
	read := Command{Client: 10, Seq: 1, Op: OpGet, Key: "k"}
	first := sm.Apply(read)
	sm.Apply(Command{Client: 11, Seq: 1, Op: OpPut, Key: "k", Val: "changed"})
	again := sm.Apply(read)
	if again != first {
		t.Fatalf("repeated read returned %v, first attempt returned %v", again, first)
	}
	if again.Val != "x" {
		t.Fatalf("cached read result = %v, want the value observed at first apply", again)
	}

	// A later sequence number from the same client must execute normally.
	third := sm.Apply(Command{Client: 10, Seq: 2, Op: OpGet, Key: "k"})
	if third.Val != "changed" || !third.Found {
		t.Fatalf("a new sequence number returned %v, want the current value", third)
	}
}

func TestResultLookup(t *testing.T) {
	sm := NewStateMachine()
	if _, ok := sm.Result(1, 1); ok {
		t.Error("Result reported a command that was never applied")
	}
	sm.Apply(Command{Client: 1, Seq: 3, Op: OpPut, Key: "k", Val: "v"})
	if _, ok := sm.Result(1, 4); ok {
		t.Error("Result reported a sequence number above the applied one")
	}
	for _, seq := range []uint64{1, 2, 3} {
		if _, ok := sm.Result(1, seq); !ok {
			t.Errorf("Result did not report an already applied command at seq %d", seq)
		}
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	sm := NewStateMachine()
	for i := 0; i < 50; i++ {
		sm.Apply(Command{Client: uint64(i % 7), Seq: uint64(i + 1), Op: OpPut, Key: string(rune('a' + i%13)), Val: strings.Repeat("v", i%5)})
	}
	sm.Apply(Command{Client: 3, Seq: 100, Op: OpAppend, Key: "zz", Val: "tail"})

	image := sm.Snapshot()
	restored := NewStateMachine()
	if err := restored.Restore(image); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if restored.Len() != sm.Len() {
		t.Fatalf("restored %d keys, original had %d", restored.Len(), sm.Len())
	}
	for k, want := range sm.data {
		got, ok := restored.Get(k)
		if !ok || got != want {
			t.Fatalf("key %q restored as %q/%v, want %q", k, got, ok, want)
		}
	}
	// Sessions must survive, or a retry after a restart would execute twice.
	dup := Command{Client: 3, Seq: 100, Op: OpAppend, Key: "zz", Val: "tail"}
	before, _ := restored.Get("zz")
	restored.Apply(dup)
	after, _ := restored.Get("zz")
	if before != after {
		t.Fatalf("a retry of an already applied command was re-executed after restore: %q -> %q", before, after)
	}

	// Round-tripping the restored image must be a fixed point.
	second := NewStateMachine()
	if err := second.Restore(restored.Snapshot()); err != nil {
		t.Fatalf("second Restore: %v", err)
	}
	if string(second.Snapshot()) != string(restored.Snapshot()) {
		t.Error("snapshot encoding is not a fixed point after a restore")
	}
}

func TestSnapshotOfEmptyState(t *testing.T) {
	sm := NewStateMachine()
	other := NewStateMachine()
	other.Apply(Command{Client: 1, Seq: 1, Op: OpPut, Key: "x", Val: "y"})
	if err := other.Restore(sm.Snapshot()); err != nil {
		t.Fatalf("Restore of an empty image: %v", err)
	}
	if other.Len() != 0 {
		t.Fatalf("restoring an empty image left %d keys", other.Len())
	}
	if err := other.Restore(nil); err != nil {
		t.Fatalf("Restore(nil): %v", err)
	}
}

func TestSnapshotDecodingRejectsTruncation(t *testing.T) {
	sm := NewStateMachine()
	sm.Apply(Command{Client: 1, Seq: 1, Op: OpPut, Key: "key", Val: "value"})
	image := sm.Snapshot()
	// A zero-length image is meaningful — it is what a node with no snapshot
	// has — so truncation is only interesting from one byte upwards.
	for cut := 1; cut < len(image); cut++ {
		other := NewStateMachine()
		if err := other.Restore(image[:cut]); err == nil {
			t.Fatalf("Restore accepted a snapshot truncated to %d of %d bytes", cut, len(image))
		}
	}
}

func TestCloneIsIndependent(t *testing.T) {
	sm := NewStateMachine()
	sm.Apply(Command{Client: 1, Seq: 1, Op: OpPut, Key: "k", Val: "v"})
	c := sm.Clone()
	c.Apply(Command{Client: 2, Seq: 1, Op: OpPut, Key: "k", Val: "other"})
	if v, _ := sm.Get("k"); v != "v" {
		t.Fatalf("mutating a clone changed the original: %q", v)
	}
}

// TestServerAppliesThroughTheLog drives a single-node cluster end to end. It is
// the smallest test that shows a client's read going through the replicated log
// rather than straight to the map, which is what makes it linearizable.
func TestServerAppliesThroughTheLog(t *testing.T) {
	store := raft.NewMemoryStorage()
	node, err := raft.NewNode(raft.Config{
		ID:             1,
		Voters:         []raft.NodeID{1},
		ElectionTick:   5,
		ElectionJitter: 2,
		HeartbeatTick:  1,
		Storage:        store,
		Rand:           rand.New(rand.NewSource(1)),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	srv := NewServer(node, NewStateMachine(), 4)

	for i := 0; i < 50 && node.State() != raft.StateLeader; i++ {
		node.Tick()
	}
	if node.State() != raft.StateLeader {
		t.Fatalf("single node did not become leader")
	}

	if err := srv.Propose(Command{Client: 1, Seq: 1, Op: OpPut, Key: "k", Val: "v"}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if err := srv.Propose(Command{Client: 1, Seq: 2, Op: OpAppend, Key: "k", Val: "!"}); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if err := srv.Propose(Command{Client: 1, Seq: 3, Op: OpGet, Key: "k"}); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	msgs, err := srv.Poll()
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("a single-node cluster emitted %d messages", len(msgs))
	}
	if got, ok := srv.StateMachine().Get("k"); !ok || got != "v!" {
		t.Fatalf("state machine holds %q/%v, want \"v!\"", got, ok)
	}
	if res, ok := srv.StateMachine().Result(1, 3); !ok || res.Val != "v!" {
		t.Fatalf("read result = %v/%v, want the value through the log", res, ok)
	}
	if srv.Snapshots() == 0 {
		t.Fatal("the server never compacted its log")
	}
	if node.FirstIndex() <= 1 {
		t.Fatalf("node first index = %d, so nothing was compacted", node.FirstIndex())
	}
}

func TestServerRejectsProposalOnFollower(t *testing.T) {
	store := raft.NewMemoryStorage()
	node, err := raft.NewNode(raft.Config{
		ID: 2, Voters: []raft.NodeID{1, 2, 3}, Storage: store, Rand: rand.New(rand.NewSource(2)),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := NewServer(node, nil, 0)
	if err := srv.Propose(Command{Client: 1, Seq: 1, Op: OpGet, Key: "k"}); err != raft.ErrNotLeader {
		t.Fatalf("Propose on a follower returned %v, want ErrNotLeader", err)
	}
}
