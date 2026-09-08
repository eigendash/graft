package raft

import (
	"testing"
)

// TestInstallSnapshotSupersedesStaleSuffix is the regression test for the bug
// the simulator's model-check sweep found at seed 35.
//
// The bug: Storage had one operation for two different things. Locally
// compacting a log may keep the entries above the image, because they are still
// part of this node's history; installing a leader's image may not, because the
// leader sent it precisely because the local suffix is wrong. Sharing one
// operation meant a leader-supplied image left a stale suffix in durable
// storage, and a restart rebuilt:
//
//	snapshot(index=2, term=9) + entry(index=3, term=2)
//
// a log whose terms decrease. Two nodes then held different entries at the same
// (index, term), which is a Log Matching failure and therefore a safety
// failure.
//
// The in-memory path was already correct, which is why only a crash exposed it.
func TestInstallSnapshotSupersedesStaleSuffix(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveHardState(HardState{Term: 2, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{
		ent(1, 1, "a"), ent(2, 2, "b"), ent(2, 3, "c"), ent(2, 4, "d"),
	}}); err != nil {
		t.Fatal(err)
	}
	n := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(1)})

	snap := Snapshot{Index: 2, Term: 9, Voters: []NodeID{1, 2, 3}, Data: []byte("image")}
	n.Step(Message{Type: MsgSnap, From: 2, To: 1, Term: 9, Snapshot: &snap})
	r := n.Ready()
	if r.Snapshot == nil || r.Snapshot.Index != 2 {
		t.Fatalf("the installed snapshot was not handed to the state machine: %+v", r.Snapshot)
	}
	n.Advance()

	if got := n.FirstIndex(); got != 3 {
		t.Fatalf("first index after installing a snapshot at 2 = %d, want 3", got)
	}
	if got := n.LastIndex(); got != 2 {
		t.Fatalf("the node kept %d entries past the snapshot; the stale suffix must be dropped", got-2)
	}

	// The durable state has to agree, otherwise a crash resurrects the suffix.
	restarted := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(2)})
	if got := restarted.LastIndex(); got != 2 {
		t.Fatalf("a restart resurrected a stale suffix: last index %d, want 2", got)
	}
	if got := restarted.FirstIndex(); got != 3 {
		t.Fatalf("a restart lost the snapshot: first index %d, want 3", got)
	}
	if term, err := restarted.TermAt(2); err != nil || term != 9 {
		t.Fatalf("index 2 after restart is term %d (err %v), want 9", term, err)
	}
	ps, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(ps.Entries) != 0 {
		t.Fatalf("storage still holds %d entries from the superseded log", len(ps.Entries))
	}
	if ps.Snapshot == nil || ps.Snapshot.Index != 2 || ps.Snapshot.Term != 9 {
		t.Fatalf("storage snapshot = %v, want index 2 term 9", ps.Snapshot)
	}
	// The persisted log must remain usable: the next append belongs at index 3.
	if _, _, err := n.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("Propose on a follower returned %v", err)
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{ent(9, 3, "next")}}); err != nil {
		t.Fatalf("appending after an installed snapshot: %v", err)
	}
}

// TestLocalCompactionKeepsTheSuffix is the other half of the contract above.
// A snapshot this node created itself must not throw away entries above it,
// because they are still the tail of its own log and may be needed to catch a
// follower up.
func TestLocalCompactionKeepsTheSuffix(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveHardState(HardState{Term: 3, Vote: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{
		ent(1, 1, "a"), ent(2, 2, "b"), ent(3, 3, "c"), ent(3, 4, "d"),
	}}); err != nil {
		t.Fatal(err)
	}
	n := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(1)})
	n.Ready()
	n.Advance()

	// Only index 2 has been applied, so compaction must stop there.
	snap, err := n.CreateSnapshot([]byte("image"))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap == nil || snap.Index != 2 {
		t.Fatalf("snapshot = %v, want index 2", snap)
	}
	if got := n.LastIndex(); got != 4 {
		t.Fatalf("local compaction truncated the log to %d; entries above the image must survive", got)
	}
	if got := n.FirstIndex(); got != 3 {
		t.Fatalf("first index after compaction = %d, want 3", got)
	}
	restarted := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(2)})
	if got := restarted.LastIndex(); got != 4 {
		t.Fatalf("restart after local compaction lost the suffix: last index %d, want 4", got)
	}
	if term, err := restarted.TermAt(3); err != nil || term != 3 {
		t.Fatalf("index 3 after restart is term %d (err %v), want 3", term, err)
	}
}

// TestStorageRejectsInconsistentMutations documents the storage-level checks
// that turn a would-be silent corruption into a loud error.
func TestStorageRejectsInconsistentMutations(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveSnapshot(Snapshot{Index: 5, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{ent(2, 9, "gap")}}); err == nil {
		t.Error("storage accepted an append that does not follow the snapshot")
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{ent(2, 6, "ok")}}); err != nil {
		t.Fatalf("storage rejected a contiguous append: %v", err)
	}
	if err := st.SaveLog(LogMutation{TruncateFrom: 3}); err == nil {
		t.Error("storage accepted a truncation at or below the snapshot index")
	}
}

func TestTermOrderIsEnforcedOnAppend(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveLog(LogMutation{Entries: []Entry{ent(5, 1, "a"), ent(5, 2, "b")}}); err != nil {
		t.Fatal(err)
	}
	n := mustNode(t, Config{
		ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(1),
		Bug: func(msg string) { panic("caught: " + msg) },
	})
	// A leader in term 4 asking this node to append a term-4 entry after a
	// term-5 entry would produce a log whose terms decrease. That cannot come
	// from a correct leader, and the node must refuse to construct it.
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("appending an older term after a newer one was accepted")
		}
	}()
	n.appendEntriesToLog(0, []Entry{ent(4, 3, "backwards")})
}

// TestOutboundAppendEntriesDoesNotAliasTheLog is the regression test for the
// second bug the model-check sweep found, at seed 207 of the harsh schedule.
//
// The bug: sendAppend built the outbound message's Entries as a sub-slice of
// the leader's log. A message sits on the wire for several ticks, and a node
// that loses leadership truncates its log and appends replacements into
// exactly that memory. The in-flight message then reads back a mixture — a
// newer prefix followed by a stale suffix — which is a batch whose terms
// decrease. The follower correctly refused it, and the refusal surfaced as a
// Log Matching failure.
//
// A real transport serialises the message and is immune. This copy is what
// stands in for that serialisation.
func TestOutboundAppendEntriesDoesNotAliasTheLog(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveHardState(HardState{Term: 1, Vote: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{
		ent(1, 1, "a"), ent(1, 2, "b"), ent(1, 3, "c"),
		ent(1, 4, "d"), ent(1, 5, "e"), ent(1, 6, "f"),
	}}); err != nil {
		t.Fatal(err)
	}
	n := mustNode(t, Config{
		ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(1), MaxEntriesPerMsg: 64,
	})

	n.campaign()
	n.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: n.Term()})
	if n.State() != StateLeader {
		t.Fatalf("node 1 is %v, expected leader", n.State())
	}
	n.Ready() // the first batch is only the leader's no-op

	// Force a backtrack, which resends the whole log in one message.
	n.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: n.Term(), Reject: true, Index: n.LastIndex() - 1, RejectHint: 1})

	r := n.Ready()
	var captured Message
	for _, m := range r.Messages {
		if m.Type == MsgApp && len(m.Entries) >= 4 {
			captured = m
			break
		}
	}
	if len(captured.Entries) < 4 {
		t.Fatalf("test setup did not produce a multi-entry batch: %v", r.Messages)
	}
	type snapshot struct {
		term  Term
		index Index
		data  string
	}
	before := make([]snapshot, len(captured.Entries))
	for i, e := range captured.Entries {
		before[i] = snapshot{e.Term, e.Index, string(e.Data)}
	}

	// The node loses leadership and rewrites the middle of its log, in place.
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: n.Term() + 1, Index: 2, LogTerm: 1,
		Entries: []Entry{ent(3, 3, "X"), ent(3, 4, "Y")}})

	for i, e := range captured.Entries {
		got := snapshot{e.Term, e.Index, string(e.Data)}
		if got != before[i] {
			t.Fatalf("an in-flight AppendEntries changed after it was sent: entry %d was %+v and is now %+v",
				i, before[i], got)
		}
	}
	// And the log really was rewritten, so the check above is not vacuous.
	ents := n.Entries()
	if len(ents) < 4 || ents[2].Term != 3 || ents[3].Term != 3 || string(ents[2].Data) != "X" {
		t.Fatalf("the log was not rewritten as the test intended: %v", ents)
	}
	prev := Term(0)
	for _, e := range ents {
		if e.Term < prev {
			t.Fatalf("the rewritten log decreases: %d/%d after term %d", e.Term, e.Index, prev)
		}
		prev = e.Term
	}
}
