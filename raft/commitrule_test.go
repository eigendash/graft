package raft

import (
	"sort"
	"testing"
)

// TestCommitRuleRefusesEarlierTermMajority is the §5.4.2 test at its smallest.
//
// It hand-builds the leader's progress table so that the third-highest match
// index — the one a quorum-counting rule would commit to — names an entry from
// an earlier term. A rule that commits the quorum match index commits it; the
// correct rule refuses, and only advances once an entry of the leader's own
// term reaches a quorum.
func TestCommitRuleRefusesEarlierTermMajority(t *testing.T) {
	st := NewMemoryStorage()
	if err := st.SaveHardState(HardState{Term: 5, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveLog(LogMutation{Entries: []Entry{ent(1, 1, "a"), ent(2, 2, "b"), ent(5, 3, "c")}}); err != nil {
		t.Fatal(err)
	}
	n := mustNode(t, Config{ID: 1, Voters: []NodeID{1, 2, 3, 4, 5}, Storage: st, Rand: testRand(7)})

	// Put the node into a leader's shoes in term 5 with a crafted replication
	// table: index 2 (term 2) is on nodes 1, 2 and 3, while the term-5 entry at
	// index 3 only reached node 2.
	n.state = StateLeader
	n.term = 5
	n.lead = 1
	n.vote = 1
	n.prs = map[NodeID]*Progress{}
	for _, id := range n.voterList {
		n.prs[id] = newProgress(4, 64)
	}
	n.prs[1].Match = 3
	n.prs[2].Match = 3
	n.prs[3].Match = 2
	n.prs[4].Match = 2
	n.prs[5].Match = 0

	quorumMatch := thirdHighestMatch(n)
	if quorumMatch != 2 {
		t.Fatalf("test setup is wrong: the quorum match index is %d, expected the old-term entry at 2", quorumMatch)
	}
	if term, err := n.log.term(quorumMatch); err != nil || term == n.term {
		t.Fatalf("test setup is wrong: the quorum match index holds term %d, leader term %d", term, n.term)
	}

	if n.maybeCommit() {
		t.Fatal("maybeCommit advanced the commit index past an entry of a previous term; §5.4.2 is violated")
	}
	if n.Committed() != 1 {
		t.Fatalf("commit index = %d, want 1", n.Committed())
	}

	// The moment the leader's own term reaches a quorum, it may commit — and
	// that commits the earlier entry as a side effect, which is correct.
	n.prs[3].Match = 3
	if !n.maybeCommit() {
		t.Fatal("leader refused to commit an entry of its own term held by a quorum")
	}
	if n.Committed() != 3 {
		t.Fatalf("commit index = %d, want 3", n.Committed())
	}
}

func thirdHighestMatch(n *Node) Index {
	matches := make([]Index, 0, len(n.voterList))
	for _, id := range n.voterList {
		matches = append(matches, n.prs[id].Match)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	return matches[n.quorum()-1]
}

// figure8Cluster builds the log states from Figure 8 of the Raft paper:
//
//	index:      1    2
//	node 1:   1(t1) 2(t2)   <- replicated to node 2 only, then crashed
//	node 2:   1(t1) 2(t2)
//	node 3:   1(t1)
//	node 4:   1(t1)
//	node 5:   1(t1) 2(t3)   <- a different entry at the same index, term 3
//
// Index 1 is committed everywhere; index 2 is not committed anywhere.
func figure8Cluster(t *testing.T, mutate func(*Config)) *cluster {
	t.Helper()
	c := newCluster(t, 5, func(cfg *Config) {
		cfg.MaxEntriesPerMsg = 1
		cfg.ElectionTick = 10
		cfg.ElectionJitter = 4
		if mutate != nil {
			mutate(cfg)
		}
	})
	c.seed(1, HardState{Term: 3, Commit: 1}, []Entry{ent(1, 1, "a"), ent(2, 2, "b")}, nil)
	c.seed(2, HardState{Term: 2, Commit: 1}, []Entry{ent(1, 1, "a"), ent(2, 2, "b")}, nil)
	c.seed(3, HardState{Term: 2, Commit: 1}, []Entry{ent(1, 1, "a")}, nil)
	c.seed(4, HardState{Term: 2, Commit: 1}, []Entry{ent(1, 1, "a")}, nil)
	c.seed(5, HardState{Term: 3, Commit: 1}, []Entry{ent(1, 1, "a"), ent(3, 2, "b5")}, nil)
	for _, id := range c.ids {
		c.restart(id)
	}
	return c
}

// TestFigure8OldTermEntryOnMajorityIsNotCommitted drives Figure 8(c): node 1 is
// elected leader for term 4 and replicates its entry from term 2 to a majority.
// That entry must not be committed on the strength of the replica count alone,
// because node 5 still holds a different entry at the same index.
//
// MaxEntriesPerMsg is 1 here so that the entry from term 2 really can reach
// three nodes before the leader's own term-4 no-op does; the harness also
// checks, after every single message, that no leader ever advanced its commit
// index onto an entry from an earlier term.
func TestFigure8OldTermEntryOnMajorityIsNotCommitted(t *testing.T) {
	c := figure8Cluster(t, nil)
	c.isolate(5)
	c.forceElect(1)

	if got := c.nodes[1].Term(); got < 4 {
		t.Fatalf("node 1 leads term %d, expected at least 4", got)
	}

	sawOldTermMajority := false
	for i := 0; i < 60; i++ {
		c.tick()
		for _, id := range c.ids {
			n := c.nodes[id]
			if n == nil {
				continue
			}
			if n.Committed() == 2 {
				t.Fatalf("node %d committed index 2 with term %d while the leader's own term was %d; "+
					"the entry is still overwritable (Figure 8)", id, 2, n.Term())
			}
		}
		if c.majorityHasLog([]NodeID{1, 2, 3}, 2) {
			sawOldTermMajority = true
		}
	}
	if !sawOldTermMajority {
		t.Fatal("the term-2 entry never reached a majority, so nothing was tested")
	}
	// After convergence the entry is committed transitively, because the
	// leader's own term-4 entry at index 3 is on a majority.
	if got := c.nodes[1].Committed(); got < 3 {
		t.Fatalf("leader commit index = %d, want at least 3 once its own term reached a quorum", got)
	}
	for _, id := range []NodeID{1, 2, 3, 4} {
		if term, err := c.nodes[id].TermAt(3); err != nil || term != 4 {
			t.Fatalf("node %d has index 3 term %d (err %v), want term 4", id, term, err)
		}
	}
	if c.commitRuleViolations != 0 {
		t.Fatalf("%d leaders committed an entry from an earlier term; §5.4.2 is violated", c.commitRuleViolations)
	}
	if len(c.bugs) != 0 {
		t.Fatalf("invariant failures: %v", c.bugs)
	}
}

// TestFigure8LaterLeaderOverwritesUncommittedEntry drives Figure 8(d): node 1
// gets its term-2 entry onto a majority and then dies before its own term can
// reach a quorum. Node 5, which holds a different entry at that index, wins the
// next election and overwrites it.
//
// Nothing is lost, because the entry was never committed. If §5.4.2 were
// implemented as "commit the quorum match index", node 1 would have committed
// index 2, node 5 would overwrite a committed entry, and the harness would
// report both a changed committed term and a leader committing an older term.
func TestFigure8LaterLeaderOverwritesUncommittedEntry(t *testing.T) {
	c := figure8Cluster(t, nil)
	c.isolate(5)
	c.forceCandidate(1)

	// Drive the election and the replication one message at a time, stopping
	// the moment index 2 (term 2) is on a majority of {1,2,3}. A full flush
	// would run past this instant, because the very next round lets the
	// leader's own term reach a quorum.
	replicated := false
	for i := 0; i < 40; i++ {
		c.round()
		if c.nodes[1].State() == StateLeader && c.majorityHasLog([]NodeID{1, 2, 3}, 2) {
			replicated = true
			break
		}
	}
	if !replicated {
		t.Fatal("node 3 never received the term-2 entry; the scenario did not reproduce")
	}
	for _, id := range []NodeID{1, 2, 3} {
		if got := c.nodes[id].Committed(); got >= 2 {
			t.Fatalf("node %d committed index %d before its own term reached a quorum", id, got)
		}
	}
	if c.commitRuleViolations != 0 {
		t.Fatalf("%d leaders committed an entry from an earlier term; §5.4.2 is violated", c.commitRuleViolations)
	}

	// The leader dies with the old-term entry on a majority of the cluster.
	c.crash(1)

	// Node 2 holds the most complete log, so it would win a free-for-all and
	// preserve the term-2 entry. The scenario needs node 5 to win, so it is
	// given a term strictly greater than the dead leader's and allowed to
	// campaign alone before the links come back.
	c.isolate(5)
	for i := 0; i < 600 && c.nodes[5].Term() < 5; i++ {
		c.round()
		c.nodes[5].Tick()
	}
	if c.nodes[5].State() != StateCandidate {
		t.Fatalf("node 5 is %v, expected a candidate", c.nodes[5].State())
	}
	c.heal()
	c.flush()
	if c.nodes[5].State() != StateLeader {
		t.Fatalf("node 5 did not win term %d (state=%v, terms=%v, commits=%v)",
			c.nodes[5].Term(), c.nodes[5].State(), c.terms(), c.commits())
	}
	c.ticks(200)

	// Node 5's entry from term 3 must have replaced the term-2 entry.
	for _, id := range []NodeID{2, 3, 4, 5} {
		term, err := c.nodes[id].TermAt(2)
		if err != nil {
			t.Fatalf("node %d has no entry at index 2: %v", id, err)
		}
		if term != 3 {
			t.Fatalf("node %d holds index 2 with term %d, want the term-3 entry from node 5", id, term)
		}
	}
	if term, err := c.nodes[5].State(), error(nil); term != StateLeader && err == nil {
		t.Logf("node 5 ended as %v (leadership may have moved on after the overwrite)", term)
	}
	if _, ok := c.commitTerms[2]; !ok {
		t.Fatal("index 2 was never committed even after the overwrite")
	}
	if got := c.commitTerms[2]; got != 3 {
		t.Fatalf("index 2 was committed with term %d, want 3", got)
	}
	if c.commitRuleViolations != 0 {
		t.Fatalf("%d leaders committed an entry from an earlier term; §5.4.2 is violated", c.commitRuleViolations)
	}
	if len(c.bugs) != 0 {
		t.Fatalf("invariant failures: %v", c.bugs)
	}
}
