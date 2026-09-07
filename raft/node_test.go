package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

func testRand(seed int64) Source { return rand.New(rand.NewSource(seed)) }

func mustNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}
	if cfg.Rand == nil {
		cfg.Rand = testRand(1)
	}
	if cfg.Bug == nil {
		cfg.Bug = func(msg string) { t.Errorf("raft invariant: %s", msg) }
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestConfigValidation(t *testing.T) {
	if _, err := NewNode(Config{ID: 0, Voters: []NodeID{1}, Storage: NewMemoryStorage(), Rand: testRand(1)}); err == nil {
		t.Error("NewNode accepted ID 0, which is the no-node sentinel")
	}
	if _, err := NewNode(Config{ID: 1, Voters: []NodeID{1}, Rand: testRand(1)}); err == nil {
		t.Error("NewNode accepted a nil Storage")
	}
	if _, err := NewNode(Config{ID: 1, Voters: []NodeID{1}, Storage: NewMemoryStorage()}); err == nil {
		t.Error("NewNode accepted a nil Rand; the core must not invent entropy")
	}
}

func TestSingleNodeElectsItselfAndCommits(t *testing.T) {
	n := mustNode(t, Config{ID: 1, Voters: []NodeID{1}, ElectionTick: 5, ElectionJitter: 1, HeartbeatTick: 1})
	for i := 0; i < 50 && n.State() != StateLeader; i++ {
		n.Tick()
	}
	if n.State() != StateLeader {
		t.Fatalf("single-node cluster did not elect itself; state=%v", n.State())
	}
	n.Ready()
	n.Advance()

	idx, term, err := n.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose on a single-node leader: %v", err)
	}
	if idx != 2 {
		t.Errorf("proposal index = %d, want 2 (index 1 is the leader's no-op)", idx)
	}
	if term != n.Term() {
		t.Errorf("proposal term = %d, want %d", term, n.Term())
	}
	r := n.Ready()
	if len(r.CommittedEntries) != 1 || string(r.CommittedEntries[0].Data) != "hello" {
		t.Fatalf("committed entries = %v, want exactly the proposal", r.CommittedEntries)
	}
	n.Advance()
	if n.Applied() != 2 {
		t.Errorf("applied = %d, want 2", n.Applied())
	}
}

func TestProposeOnFollowerIsRejected(t *testing.T) {
	n := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}})
	if _, _, err := n.Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("Propose on a follower returned %v, want ErrNotLeader", err)
	}
}

func TestThreeNodeReplicationAndLogMatching(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.waitLeader(300)

	var want []string
	for i := 0; i < 12; i++ {
		data := fmt.Sprintf("cmd-%02d", i)
		c.propose(data)
		want = append(want, data)
	}
	c.waitApplied(c.nodes[mustLeader(t, c)].LastIndex(), 300)

	var reference []string
	for _, id := range c.ids {
		if got := c.appliedData(id); len(got) != 0 {
			reference = got
			break
		}
	}
	if len(reference) < len(want) {
		t.Fatalf("only %d entries applied, want at least %d", len(reference), len(want))
	}
	for _, id := range c.ids {
		got := c.appliedData(id)
		if len(got) != len(reference) {
			t.Fatalf("node %d applied %d entries, node reference applied %d", id, len(got), len(reference))
		}
		for i := range got {
			if got[i] != reference[i] {
				t.Fatalf("node %d applied %q at position %d, reference has %q (Log Matching violated)", id, got[i], i, reference[i])
			}
		}
	}
	// Every proposed command must actually be in the applied sequence.
	for _, w := range want {
		found := false
		for _, g := range reference {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("committed sequence is missing %q: %v", w, reference)
		}
	}
}

func mustLeader(t *testing.T, c *cluster) NodeID {
	t.Helper()
	id, ok := c.leader()
	if !ok {
		t.Fatal("cluster has no leader")
	}
	return id
}

func TestElectionRestrictionRejectsIncompleteLog(t *testing.T) {
	c := newCluster(t, 3, nil)
	// Nodes 1 and 2 hold committed history; node 3 has an empty log but a
	// higher term, which is not enough to win under §5.4.1.
	c.seed(1, HardState{Term: 4, Commit: 2}, []Entry{ent(1, 1, "a"), ent(2, 2, "b")}, nil)
	c.seed(2, HardState{Term: 4, Commit: 2}, []Entry{ent(1, 1, "a"), ent(2, 2, "b")}, nil)
	c.seed(3, HardState{Term: 9, Commit: 0}, nil, nil)
	for _, id := range c.ids {
		c.restart(id)
	}

	c.setAllLinks(false)
	c.flush()
	for i := 0; i < 200 && c.nodes[3].State() != StateCandidate; i++ {
		c.nodes[3].Tick()
		c.flush()
	}
	if c.nodes[3].State() != StateCandidate {
		t.Fatalf("node 3 never campaigned")
	}
	c.setAllLinks(true)
	c.flush()
	if c.nodes[3].State() == StateLeader {
		t.Fatal("a candidate with an empty log won an election; §5.4.1 is not enforced")
	}
	if votes := c.nodes[3].votesGranted; len(votes) != 1 {
		t.Fatalf("node 3 collected %d votes, want only its own", len(votes))
	}
}

func TestVoteIsDurableBeforeItIsAnswered(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveHardState(HardState{Term: 5}); err != nil {
		t.Fatal(err)
	}
	cfg := func() Config {
		return Config{ID: 3, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(3), ElectionTick: 10, ElectionJitter: 4}
	}
	n := mustNode(t, cfg())
	n.Step(Message{Type: MsgVote, From: 2, To: 3, Term: 5, LogTerm: 0, Index: 0})
	r := n.Ready()
	if len(r.Messages) != 1 || r.Messages[0].Reject {
		t.Fatalf("expected one granted vote, got %v", r.Messages)
	}
	ps, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	if ps.HardState.Vote != 2 {
		t.Fatalf("durable vote = %d immediately after answering, want 2", ps.HardState.Vote)
	}

	// Crash and come back from the same storage. The recovered node must not
	// hand the same term's vote to anybody else.
	n2 := mustNode(t, cfg())
	n2.Step(Message{Type: MsgVote, From: 1, To: 3, Term: 5, LogTerm: 0, Index: 0})
	r2 := n2.Ready()
	if len(r2.Messages) != 1 || !r2.Messages[0].Reject {
		t.Fatalf("restarted node double-voted in term 5: %v", r2.Messages)
	}
}

func TestBacktrackingCatchesUpLaggingFollower(t *testing.T) {
	c := newCluster(t, 5, nil)
	leader := c.waitLeader(300)
	for i := 0; i < 4; i++ {
		c.propose(fmt.Sprintf("pre-%d", i))
	}
	victim := NodeID(1)
	for victim == leader {
		victim++
	}
	c.isolate(victim)

	for i := 0; i < 30; i++ {
		c.propose(fmt.Sprintf("post-%d", i))
	}
	before := c.nodes[victim].LastIndex()
	if before > 5 {
		t.Fatalf("the isolated node advanced to index %d, so nothing was tested", before)
	}

	c.heal()
	c.ticks(400)

	ref := c.nodes[mustLeader(t, c)].LastIndex()
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.LastIndex() < ref {
			t.Fatalf("node %d stuck at index %d, leader at %d", id, n.LastIndex(), ref)
		}
	}
	var reference []string
	for _, id := range c.ids {
		if got := c.appliedData(id); len(got) > len(reference) {
			reference = got
		}
	}
	for _, id := range c.ids {
		got := c.appliedData(id)
		if len(got) < len(reference) {
			t.Fatalf("node %d applied %d entries, another node applied %d", id, len(got), len(reference))
		}
		for i := range reference {
			if got[i] != reference[i] {
				t.Fatalf("node %d diverged at applied position %d: %q vs %q", id, i, got[i], reference[i])
			}
		}
	}
}

func TestSnapshotInstallCatchesUpCompactedFollower(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.MaxEntriesPerMsg = 4 })
	leader := c.waitLeader(300)
	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("early-%d", i))
	}
	victim := NodeID(1)
	for victim == leader {
		victim++
	}
	c.isolate(victim)

	for i := 0; i < 40; i++ {
		c.tickExcept(victim)
		c.propose(fmt.Sprintf("late-%d", i))
	}
	c.snapshotAll()
	if snap := c.nodes[leader].Snapshot(); snap == nil {
		t.Fatal("leader did not retain a snapshot after CreateSnapshot")
	}
	if c.nodes[leader].FirstIndex() <= c.nodes[victim].LastIndex() {
		t.Fatalf("leader's log still starts at %d while the follower is at %d; the snapshot path was not forced",
			c.nodes[leader].FirstIndex(), c.nodes[victim].LastIndex())
	}

	c.snapInstalls = 0
	c.heal()
	c.ticks(400)

	if c.snapInstalls == 0 {
		t.Fatal("the lagging follower caught up without an InstallSnapshot; the test did not exercise compaction")
	}
	ref := c.nodes[mustLeader(t, c)].Applied()
	for _, id := range c.ids {
		if got := c.nodes[id].Applied(); got != ref {
			t.Fatalf("node %d applied to %d, leader applied to %d", id, got, ref)
		}
	}
}

func TestAddVoterThenRemoveVoter(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.MaxEntriesPerMsg = 2 })
	leader := c.waitLeader(300)
	for i := 0; i < 5; i++ {
		c.propose(fmt.Sprintf("v-%d", i))
	}
	c.addProcess(4)

	if _, _, err := c.nodes[leader].ProposeConfChange(ConfChange{Type: ConfAddNode, Node: 4}); err != nil {
		t.Fatalf("ProposeConfChange(add 4): %v", err)
	}
	c.flush()
	c.ticks(300)

	for _, id := range c.ids {
		if !hasVoter(c.nodes[id], 4) {
			t.Fatalf("node %d never learned that 4 joined: voters=%v", id, c.nodes[id].Voters())
		}
	}
	ref := c.nodes[mustLeader(t, c)].Applied()
	if c.nodes[4].Applied() != ref {
		t.Fatalf("new voter applied to %d, leader applied to %d", c.nodes[4].Applied(), ref)
	}

	c.forceElect(4)
	if c.nodes[4].State() != StateLeader {
		t.Fatalf("the newly added voter could not win an election")
	}

	if _, _, err := c.nodes[4].ProposeConfChange(ConfChange{Type: ConfRemoveNode, Node: 2}); err != nil {
		t.Fatalf("ProposeConfChange(remove 2): %v", err)
	}
	c.flush()
	c.ticks(300)
	for _, id := range []NodeID{1, 3, 4} {
		if hasVoter(c.nodes[id], 2) {
			t.Fatalf("node %d still counts 2 as a voter: %v", id, c.nodes[id].Voters())
		}
	}
}

func hasVoter(n *Node, id NodeID) bool {
	for _, v := range n.Voters() {
		if v == id {
			return true
		}
	}
	return false
}

func TestRemovingTheLeaderStepsItDown(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader(300)
	if _, _, err := c.nodes[leader].ProposeConfChange(ConfChange{Type: ConfRemoveNode, Node: leader}); err != nil {
		t.Fatalf("ProposeConfChange(remove self): %v", err)
	}
	c.flush()
	if c.nodes[leader].State() == StateLeader {
		t.Fatal("a leader removed from the configuration kept leading")
	}
}

func TestCheckQuorumMakesAnIsolatedLeaderStepDown(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.CheckQuorum = true })
	leader := c.waitLeader(300)
	c.isolate(leader)
	c.ticks(200)
	if c.nodes[leader].State() == StateLeader {
		t.Fatal("an isolated leader never stepped down; clients would wait forever")
	}
}

func TestDuplicateAndReorderedDeliveryIsTolerated(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.waitLeader(300)
	c.propose("a")
	c.propose("b")
	c.propose("c")

	// Deliver every message twice and in reverse order. Raft must be
	// idempotent under duplication and must not depend on ordering.
	for round := 0; round < 40; round++ {
		var msgs []Message
		for _, id := range c.ids {
			r := c.nodes[id].Ready()
			c.absorb(id, r)
			msgs = append(msgs, r.Messages...)
			c.nodes[id].Advance()
		}
		if len(msgs) == 0 {
			break
		}
		for i := len(msgs) - 1; i >= 0; i-- {
			m := msgs[i]
			c.nodes[m.To].Step(m)
			c.nodes[m.To].Step(m)
		}
		c.checkLeaderCommitRule()
	}
	c.ticks(100)
	ref := c.nodes[mustLeader(t, c)].LastIndex()
	for _, id := range c.ids {
		if c.nodes[id].LastIndex() != ref {
			t.Fatalf("node %d at index %d, leader at %d after duplicate/reordered delivery", id, c.nodes[id].LastIndex(), ref)
		}
	}
}

func TestRestartReplaysOnlyPersistedState(t *testing.T) {
	c := newCluster(t, 3, nil)
	leader := c.waitLeader(300)
	for i := 0; i < 6; i++ {
		c.propose(fmt.Sprintf("e-%d", i))
	}
	c.flush()

	victim := NodeID(1)
	for victim == leader {
		victim++
	}
	appliedBefore := c.nodes[victim].Applied()
	c.crash(victim)
	if c.nodes[victim] != nil {
		t.Fatal("crash left a live node behind")
	}
	// While the node is down the cluster keeps going, so the restarted node
	// must replay persisted entries and then catch up from the leader.
	for i := 0; i < 5; i++ {
		c.propose(fmt.Sprintf("down-%d", i))
	}
	c.restart(victim)
	c.ticks(300)

	if c.nodes[victim].Applied() < appliedBefore {
		t.Fatalf("restarted node applied %d, was at %d before the crash", c.nodes[victim].Applied(), appliedBefore)
	}
	ref := c.nodes[mustLeader(t, c)].LastIndex()
	if c.nodes[victim].LastIndex() != ref {
		t.Fatalf("restarted node at index %d, leader at %d", c.nodes[victim].LastIndex(), ref)
	}
}

func TestSnapshotRoundTripThroughStorage(t *testing.T) {
	st := NewMemoryStorage()
	entries := []Entry{ent(1, 1, "a"), ent(1, 2, "b"), ent(1, 3, "c")}
	if err := st.SaveHardState(HardState{Term: 1, Vote: 1, Commit: 3}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveLog(LogMutation{Entries: entries}); err != nil {
		t.Fatal(err)
	}
	n := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st})
	// Pretend the state machine caught up so there is something to compact.
	n.Ready()
	n.Advance()
	snap, err := n.CreateSnapshot([]byte("image"))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap == nil || snap.Index != 3 {
		t.Fatalf("snapshot = %v, want index 3", snap)
	}

	// Reloading must find the log already compacted and hand the image back.
	n2 := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st})
	r := n2.Ready()
	if r.Snapshot == nil || string(r.Snapshot.Data) != "image" {
		t.Fatalf("reloaded node did not surface its snapshot: %+v", r.Snapshot)
	}
	if got := n2.FirstIndex(); got != 4 {
		t.Fatalf("reloaded node first index = %d, want 4", got)
	}
	if n2.Committed() != 3 {
		t.Fatalf("reloaded node commit = %d, want 3", n2.Committed())
	}
}

func TestSnapshotStructCloneIsDeep(t *testing.T) {
	s := &Snapshot{Index: 2, Term: 1, Voters: []NodeID{1, 2}, Data: []byte("x")}
	c := s.Clone()
	c.Voters[0] = 99
	c.Data[0] = 'y'
	if s.Voters[0] != 1 || s.Data[0] != 'x' {
		t.Fatal("Snapshot.Clone shares storage with the original")
	}
	var nilSnap *Snapshot
	if nilSnap.Clone() != nil {
		t.Fatal("Clone of a nil snapshot must be nil")
	}
}

func TestConfChangeEncodingRoundTrip(t *testing.T) {
	for _, cc := range []ConfChange{
		{Type: ConfAddNode, Node: 1},
		{Type: ConfRemoveNode, Node: 1 << 40},
	} {
		got, err := DecodeConfChange(cc.Encode())
		if err != nil {
			t.Fatalf("DecodeConfChange(%v): %v", cc, err)
		}
		if got != cc {
			t.Fatalf("round trip changed %v into %v", cc, got)
		}
	}
	if _, err := DecodeConfChange([]byte{1, 2, 3}); err == nil {
		t.Error("DecodeConfChange accepted a truncated payload")
	}
	if _, err := DecodeConfChange(make([]byte, 9)); err == nil {
		t.Error("DecodeConfChange accepted an unknown change type")
	}
	bad := ConfChange{Type: ConfAddNode, Node: 0}.Encode()
	if _, err := DecodeConfChange(bad); err == nil {
		t.Error("DecodeConfChange accepted node 0, which is reserved")
	}
}

func TestSortedVotersDeduplicatesAndOrders(t *testing.T) {
	got := SortedVoters([]NodeID{3, 1, 2, 3, 1})
	want := []NodeID{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("SortedVoters = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SortedVoters = %v, want %v", got, want)
		}
	}
}
