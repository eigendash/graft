package raft

import (
	"fmt"
	"testing"
)

// BenchmarkStepAppendEntries measures the follower's cost of accepting a batch.
// The consistency check, the conflict scan and the durable write all happen
// here, so this is the per-entry cost that matters when a leader is catching a
// follower up.
func BenchmarkStepAppendEntries(b *testing.B) {
	for _, batch := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			st := NewMemoryStorage()
			n := mustNodeBench(b, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(1)})
			entries := make([]Entry, batch)
			for i := range entries {
				entries[i] = ent(1, Index(i+1), "payload")
			}
			msg := Message{Type: MsgApp, From: 2, To: 1, Term: 1, Index: 0, LogTerm: 0, Entries: entries}
			b.SetBytes(int64(batch) * int64(len("payload")))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// The first delivery appends; later ones are idempotent, which
				// is exactly the duplicate-delivery path a real follower sees.
				n.Step(msg)
				n.Ready()
				n.Advance()
			}
		})
	}
}

// BenchmarkProposeOnLeader measures the leader's cost of accepting, persisting
// and packaging one proposal in a three-node cluster.
func BenchmarkProposeOnLeader(b *testing.B) {
	c := newCluster(b, 3, nil)
	c.waitLeader(300)
	leader := c.nodes[mustLeaderBench(b, c)]
	payload := []byte("benchmark-command-payload")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := leader.Propose(payload); err != nil {
			b.Fatal(err)
		}
		c.round()
	}
}

// BenchmarkTruncateAndAppend measures a follower rewriting a conflicting suffix,
// which is the operation that has to be atomic for safety.
func BenchmarkTruncateAndAppend(b *testing.B) {
	st := NewMemoryStorage()
	seed := make([]Entry, 256)
	for i := range seed {
		seed[i] = ent(1, Index(i+1), "x")
	}
	if err := st.SaveLog(LogMutation{Entries: seed}); err != nil {
		b.Fatal(err)
	}
	n := mustNodeBench(b, Config{ID: 1, Voters: []NodeID{1, 2, 3}, Storage: st, Rand: testRand(1)})
	replacement := []Entry{ent(2, 129, "y"), ent(2, 130, "y")}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.appendEntriesToLog(129, replacement)
	}
}

func mustNodeBench(b *testing.B, cfg Config) *Node {
	b.Helper()
	if cfg.Storage == nil {
		cfg.Storage = NewMemoryStorage()
	}
	if cfg.Rand == nil {
		cfg.Rand = testRand(1)
	}
	cfg.Bug = func(msg string) { b.Fatalf("raft invariant: %s", msg) }
	n, err := NewNode(cfg)
	if err != nil {
		b.Fatalf("NewNode: %v", err)
	}
	return n
}

func mustLeaderBench(b *testing.B, c *cluster) NodeID {
	b.Helper()
	id, ok := c.leader()
	if !ok {
		b.Fatal("cluster has no leader")
	}
	return id
}
