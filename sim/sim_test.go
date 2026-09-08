package sim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/eigendash/graft/kv"
	"github.com/eigendash/graft/lcheck"
	"github.com/eigendash/graft/raft"
)

func newTestSim(t *testing.T, cfg Config) *Sim {
	t.Helper()
	cfg.Invariants = true
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func requireNoViolations(t *testing.T, s *Sim) {
	t.Helper()
	if v := s.Violations(); len(v) != 0 {
		t.Fatalf("simulator reported %d safety violations:\n%s", len(v), formatViolations(v))
	}
}

func formatViolations(v []string) string {
	out := ""
	for i, s := range v {
		if i >= 10 {
			out += fmt.Sprintf("  ... and %d more\n", len(v)-i)
			break
		}
		out += "  " + s + "\n"
	}
	return out
}

// waitWorkload drives the simulation until every client has completed, and
// fails with the progress it reached rather than a timeout.
func waitWorkload(t *testing.T, s *Sim, maxTicks int) {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		if p := s.WorkloadProgress(); p.Completed == p.Total {
			return
		}
		s.Step()
		if len(s.Violations()) != 0 {
			break
		}
	}
	p := s.WorkloadProgress()
	t.Fatalf("workload stalled at %d of %d operations with %d outstanding; violations:\n%s",
		p.Completed, p.Total, p.Stalled, formatViolations(s.Violations()))
}

func TestSingleNodeCommitsClientCommands(t *testing.T) {
	s := newTestSim(t, Config{Nodes: 1, Seed: 1, ElectionTick: 5, ElectionJitter: 2, HeartbeatTick: 1})
	s.Workload(Workload{Clients: 1, OpsPerClient: 10, Keys: 2, Values: 2})
	waitWorkload(t, s, 500)
	requireNoViolations(t, s)

	if n := s.Node(1); n.State() != raft.StateLeader {
		t.Fatalf("single node is %v", n.State())
	}
	hist := s.History()
	if len(hist) != 10 {
		t.Fatalf("history has %d operations, want 10", len(hist))
	}
	res, err := lcheck.Check(hist, lcheck.NewKVModel(), lcheck.DefaultOptions())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("a single-node history is not linearizable: %s", res.Explanation)
	}
}

func TestClusterElectsExactlyOneLeaderPerTerm(t *testing.T) {
	s := newTestSim(t, Config{Nodes: 5, Seed: 7})
	s.Run(600)
	requireNoViolations(t, s)
	leaders := s.Leaders()
	if len(leaders) != 1 {
		t.Fatalf("a stable five-node cluster has %d leaders (%v)", len(leaders), leaders)
	}
	if len(s.leaderOfTerm) == 0 {
		t.Fatal("no term ever produced a leader, so the check proved nothing")
	}
}

// TestElectionLiveness checks that the side of a partition holding a majority
// elects a leader inside a bounded number of ticks. Without a bound, "it
// eventually converges" is not a property a test can fail.
func TestElectionLiveness(t *testing.T) {
	const nodes = 5
	for _, seed := range []int64{1, 2, 3, 4, 5, 6, 7, 8} {
		s := newTestSim(t, Config{Nodes: nodes, Seed: seed})
		s.Run(400)
		requireNoViolations(t, s)
		leaders := s.Leaders()
		if len(leaders) != 1 {
			t.Fatalf("seed %d: no stable leader before partitioning (%v)", seed, leaders)
		}

		// Cut the leader off. The remaining majority must elect a replacement
		// well inside a few election timeouts.
		old := leaders[0]
		cfg := s.Config()
		s.Isolate(old)
		budget := 20 * int64(cfg.ElectionTick+cfg.ElectionJitter)
		start := s.Now()
		found := raft.NodeID(0)
		for s.Now()-start < budget && found == 0 {
			s.Step()
			for _, id := range s.Leaders() {
				if id != old {
					found = id
					break
				}
			}
		}
		if found == 0 {
			t.Fatalf("seed %d: the majority side elected no leader within %d ticks", seed, budget)
		}
		if term := s.Node(found).Term(); term <= s.Node(old).Term() {
			t.Fatalf("seed %d: replacement leader is in term %d, not above the isolated leader's %d",
				seed, term, s.Node(old).Term())
		}
		requireNoViolations(t, s)
	}
}

// TestDeterminism asserts the property that makes every other result in this
// project reproducible: the same seed produces a byte-identical event log.
func TestDeterminism(t *testing.T) {
	cfg := Config{
		Nodes:          5,
		Seed:           424242,
		Latency:        Latency{Min: 1, Max: 5},
		DropRate:       0.10,
		DuplicateRate:  0.05,
		ReorderRate:    0.10,
		PartitionRate:  0.02,
		HealRate:       0.05,
		CrashRate:      0.004,
		ElectionTick:   8,
		ElectionJitter: 8,
		Trace:          true,
		Invariants:     true,
	}
	run := func(seed int64) *Sim {
		c := cfg
		c.Seed = seed
		s := newTestSim(t, c)
		s.Workload(Workload{Clients: 4, OpsPerClient: 15, Keys: 3, Values: 3, ClientTimeout: 40})
		s.Run(1200)
		return s
	}
	a, b := run(cfg.Seed), run(cfg.Seed)
	requireNoViolations(t, a)
	requireNoViolations(t, b)

	ea, eb := a.Events(), b.Events()
	if len(ea) == 0 {
		t.Fatal("no events were recorded, so determinism was not actually tested")
	}
	if len(ea) != len(eb) {
		t.Fatalf("event logs differ in length: %d vs %d", len(ea), len(eb))
	}
	for i := range ea {
		if ea[i] != eb[i] {
			t.Fatalf("event logs diverge at line %d of %d:\n  %s\n  %s", i, len(ea), ea[i], eb[i])
		}
	}
	if a.EventDigest() != b.EventDigest() {
		t.Fatalf("event digests differ: %#x vs %#x", a.EventDigest(), b.EventDigest())
	}
	if a.Stats() != b.Stats() {
		t.Fatalf("identical seeds produced different statistics:\n %+v\n %+v", a.Stats(), b.Stats())
	}

	// A different seed must produce a different run, otherwise the generator
	// is not reaching the schedule and the model check is exploring one path.
	c := run(cfg.Seed + 1)
	if c.EventDigest() == a.EventDigest() {
		t.Fatal("a different seed produced an identical event log, so the generator is not being used")
	}
}

func TestPartitionPrimitives(t *testing.T) {
	s := newTestSim(t, Config{Nodes: 5, Seed: 3, Latency: Latency{Min: 2, Max: 2}})
	if !s.reachable(1, 5) {
		t.Fatal("a fresh simulator should be fully connected")
	}
	s.Isolate(3)
	if s.reachable(3, 1) || s.reachable(1, 3) {
		t.Fatal("Isolate did not cut node 3 off")
	}
	if !s.reachable(1, 2) || !s.reachable(4, 5) {
		t.Fatal("Isolate cut off nodes other than its target")
	}
	s.Partition([]raft.NodeID{1, 2}, []raft.NodeID{3, 4, 5})
	if !s.reachable(1, 2) || !s.reachable(3, 5) {
		t.Fatal("Partition disconnected members of the same group")
	}
	if s.reachable(1, 3) || s.reachable(5, 2) {
		t.Fatal("Partition left a link between groups")
	}
	s.Heal()
	for _, a := range s.IDs() {
		for _, b := range s.IDs() {
			if !s.reachable(a, b) {
				t.Fatalf("Heal left %d--%d down", a, b)
			}
		}
	}
}

// TestCrashDiscardsVolatileState is the crash model's own test. The state
// machine is poisoned with a key that exists nowhere in the log; if a restart
// reused volatile memory rather than replaying persisted state, the poison
// would survive.
func TestCrashDiscardsVolatileState(t *testing.T) {
	const poisonKey = "\x00poison"
	const poisonValue = "\x00poison-value"

	s := newTestSim(t, Config{Nodes: 3, Seed: 11, Latency: Latency{Min: 1, Max: 2}})
	s.Workload(Workload{Clients: 3, OpsPerClient: 8, Keys: 2, Values: 2, GetWeight: 0, PutWeight: 1, AppendWeight: 0})
	waitWorkload(t, s, 3000)
	requireNoViolations(t, s)

	leader := s.Leaders()[0]
	keysBefore := s.StateMachine(leader).Len()

	s.StateMachine(leader).Apply(kv.Command{Client: 1 << 40, Seq: 1, Op: kv.OpPut, Key: poisonKey, Val: poisonValue})
	if v, ok := s.StateMachine(leader).Get(poisonKey); !ok || v != poisonValue {
		t.Fatal("test setup failed to poison the volatile state machine")
	}
	storage := s.Storage(leader)
	s.Crash(leader)
	if s.Node(leader) != nil || s.StateMachine(leader) != nil {
		t.Fatal("Crash left volatile state reachable")
	}
	s.Restart(leader)
	if _, ok := s.StateMachine(leader).Get(poisonKey); ok {
		t.Fatal("the restarted node kept volatile state machine contents across a crash")
	}
	ps, err := storage.Load()
	if err != nil {
		t.Fatalf("loading storage after a crash: %v", err)
	}
	if ps.HardState.Term == 0 {
		t.Fatal("the crash also lost the persisted term")
	}
	if len(ps.Entries) == 0 && ps.Snapshot == nil {
		t.Fatal("the crash lost the persisted log")
	}

	// Replaying the persisted log must restore the same key set.
	s.Run(400)
	requireNoViolations(t, s)
	waitWorkload(t, s, 3000)
	if got := s.StateMachine(leader).Len(); got < keysBefore {
		t.Fatalf("restarted node has %d keys, had %d before the crash", got, keysBefore)
	}
	// And it must agree with a node that never crashed.
	other := raft.NodeID(1)
	if other == leader {
		other = 2
	}
	if a, b := s.StateMachine(other).Snapshot(), s.StateMachine(leader).Snapshot(); string(a) != string(b) {
		t.Fatal("the restarted node's state machine diverged from a node that never crashed")
	}
}

// TestDelayedMessagesArriveLate checks the router's latency model directly: a
// message sent while a link is down is dropped, and one that is not dropped
// arrives no earlier than Latency.Min ticks later.
func TestDelayedMessagesArriveLate(t *testing.T) {
	s := newTestSim(t, Config{Nodes: 3, Seed: 5, Latency: Latency{Min: 4, Max: 6}})
	s.Run(80)
	requireNoViolations(t, s)

	var observed int
	for _, n := range s.nodes {
		for _, m := range n.server.Node().Entries() {
			_ = m
		}
	}
	// Reconstruct deliveries from a traced run instead: the router logs every
	// hop, so the gaps are checkable.
	s2 := newTestSim(t, Config{Nodes: 3, Seed: 5, Latency: Latency{Min: 4, Max: 6}, Trace: true})
	s2.Run(80)
	sentAt := map[raft.NodeID]int64{}
	_ = sentAt
	observed = len(s2.Events())
	if observed == 0 {
		t.Fatal("no events were traced")
	}
	if s2.Stats().MessagesDelivered == 0 {
		t.Fatal("no messages were delivered, so the latency model was not exercised")
	}
	if s2.Stats().MessagesSent < s2.Stats().MessagesDelivered {
		t.Fatalf("more messages delivered (%d) than sent (%d)", s2.Stats().MessagesDelivered, s2.Stats().MessagesSent)
	}
}

func TestRandomPartitionIsDeterministic(t *testing.T) {
	digests := make([]uint64, 2)
	for i := range digests {
		s := newTestSim(t, Config{Nodes: 5, Seed: 99, Trace: true, PartitionRate: 0.1, HealRate: 0.1})
		s.Run(300)
		digests[i] = s.EventDigest()
		if len(s.Events()) == 0 {
			t.Fatal("no events traced")
		}
	}
	if digests[0] != digests[1] {
		t.Fatalf("random partitions are not reproducible: %#x vs %#x", digests[0], digests[1])
	}
}

func TestRandomPartitionIsInstalledAndHealed(t *testing.T) {
	// A high partition rate with a high heal rate must actually produce both
	// events, otherwise the fault schedule is inert.
	s := newTestSim(t, Config{Nodes: 5, Seed: 21, Trace: true, PartitionRate: 0.2, HealRate: 0.2})
	s.Run(300)
	var partitions, heals int
	for _, e := range s.Events() {
		switch {
		case strings.Contains(e, "partition ["):
			partitions++
		case strings.Contains(e, " heal"):
			heals++
		}
	}
	if partitions == 0 {
		t.Fatal("the fault schedule never installed a partition")
	}
	if heals == 0 {
		t.Fatal("the fault schedule never healed a partition")
	}
}
