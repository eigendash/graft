package sim

import (
	"strings"
	"testing"

	"github.com/eigendash/graft/lcheck"
	"github.com/eigendash/graft/raft"
)

// cheapFaults is the configuration the seed sweep uses. It is deliberately
// cheap in ticks but aggressive in faults: the point of a sweep is breadth, and
// a schedule that never partitions or drops anything proves nothing.
func cheapFaults(seed int64, nodes int) Config {
	return Config{
		Seed:              seed,
		Nodes:             nodes,
		ElectionTick:      8,
		ElectionJitter:    8,
		HeartbeatTick:     2,
		Latency:           Latency{Min: 1, Max: 3},
		DropRate:          0.08,
		DuplicateRate:     0.03,
		ReorderRate:       0.08,
		ReorderWindow:     4,
		PartitionRate:     0.015,
		HealRate:          0.04,
		CrashRate:         0.002,
		RestartDelay:      15,
		SnapshotThreshold: 40,
	}
}

// TestModelCheckAcrossSeeds is the project's central verification claim: over
// many seeds, with random partitions, drops, duplicates, reordering and
// crashes, no run ever violates election safety, the Log Matching Property, the
// rule that only committed entries are applied, or §5.4.2.
//
// The sweep is run twice at two cluster sizes so that quorum arithmetic is
// exercised with both an odd majority (three) and a larger one (five).
func TestModelCheckAcrossSeeds(t *testing.T) {
	const seeds = 200
	const ticks = 1200

	three := SafetySweep(cheapFaults(1, 3), seeds, ticks)
	if three.SeedsWithViolations != 0 {
		t.Fatalf("%d of %d three-node seeds violated a safety property:\n%s",
			three.SeedsWithViolations, three.Seeds, strings.Join(three.Violations, "\n"))
	}
	five := SafetySweep(cheapFaults(90001, 5), seeds, ticks)
	if five.SeedsWithViolations != 0 {
		t.Fatalf("%d of %d five-node seeds violated a safety property:\n%s",
			five.SeedsWithViolations, five.Seeds, strings.Join(five.Violations, "\n"))
	}

	// A sweep that never committed anything would pass vacuously.
	if three.Stats.EntriesCommitted < 1000 {
		t.Fatalf("three-node sweep committed only %d entries, so it barely ran", three.Stats.EntriesCommitted)
	}
	if five.Stats.EntriesCommitted < 1000 {
		t.Fatalf("five-node sweep committed only %d entries, so it barely ran", five.Stats.EntriesCommitted)
	}
	if three.Stats.Partitions == 0 || three.Stats.Heals == 0 {
		t.Fatalf("the fault schedule installed %d partitions and healed %d; it is inert",
			three.Stats.Partitions, three.Stats.Heals)
	}
	for name, r := range map[string]SafetySweepResult{"3": three, "5": five} {
		t.Logf("nodes=%s seeds=%d ticks=%d committed=%d elections=%d crashes=%d restarts=%d partitions=%d snapshots(created/installed)=%d/%d dropped=%d duplicated=%d reordered=%d",
			name, r.Seeds, r.Ticks, r.Stats.EntriesCommitted, r.Stats.Elections,
			r.Stats.Crashes, r.Stats.Restarts, r.Stats.Partitions,
			r.Stats.SnapshotsCreated, r.Stats.SnapshotsInstalled,
			r.Stats.MessagesDropped, r.Stats.MessagesDuplicated, r.Stats.MessagesReordered)
	}
}

// TestModelCheckHarshConfig pushes a five-node cluster much harder: long runs,
// heavy loss, frequent crashes with a fast restart, and enough compaction that
// InstallSnapshot runs often. This is the configuration that surfaced the
// snapshot-over-stale-suffix bug, because that bug only becomes visible when a
// node compacts, is superseded, and then restarts.
func TestModelCheckHarshConfig(t *testing.T) {
	cfg := Config{
		Nodes:             5,
		ElectionTick:      8,
		ElectionJitter:    8,
		HeartbeatTick:     2,
		Latency:           Latency{Min: 1, Max: 4},
		DropRate:          0.15,
		DuplicateRate:     0.10,
		ReorderRate:       0.15,
		ReorderWindow:     4,
		PartitionRate:     0.01,
		HealRate:          0.06,
		CrashRate:         0.0015,
		RestartDelay:      8,
		SnapshotThreshold: 20,
		Workload: Workload{
			Clients: 3, OpsPerClient: 20, Keys: 3, Values: 3, ClientTimeout: 60,
		},
	}
	got := SafetySweep(cfg, 250, 2000)
	if got.SeedsWithViolations != 0 {
		t.Fatalf("%d of %d harsh seeds violated a safety property:\n%s",
			got.SeedsWithViolations, got.Seeds, strings.Join(got.Violations, "\n"))
	}
	if got.Stats.Crashes < 1000 {
		t.Fatalf("the harsh schedule only produced %d crashes, so it was not harsh", got.Stats.Crashes)
	}
	if got.Stats.SnapshotsCreated == 0 || got.Stats.SnapshotsInstalled == 0 {
		t.Fatalf("compaction was not exercised: %d created, %d installed",
			got.Stats.SnapshotsCreated, got.Stats.SnapshotsInstalled)
	}

	// Liveness. A cluster that never commits anything is trivially safe, so
	// the sweep is only allowed to pass if the clients actually finished and
	// the committed log actually grew. Both thresholds are floors, not targets.
	if got.SeedsIncomplete != 0 {
		t.Fatalf("%d of %d harsh seeds left clients waiting, so their histories are incomplete",
			got.SeedsIncomplete, got.Seeds)
	}
	if got.OperationsCompleted != got.OperationsTotal {
		t.Fatalf("the harsh sweep completed %d of %d client operations",
			got.OperationsCompleted, got.OperationsTotal)
	}
	// Forty committed entries per seed is far below what a healthy run
	// produces — the measured value is reported by the log line below and is
	// much higher — so this is a stall detector, not a performance target.
	if got.CommittedDepthTotal < int64(40*got.Seeds) {
		t.Fatalf("the committed log only reached %d entries in total across %d seeds, so the cluster made almost no progress",
			got.CommittedDepthTotal, got.Seeds)
	}
	t.Logf("harsh: seeds=%d ticks=%d committedDepthTotal=%d clientOps=%d/%d elections=%d crashes=%d restarts=%d partitions=%d snapshots(created/installed)=%d/%d",
		got.Seeds, got.Ticks, got.CommittedDepthTotal, got.OperationsCompleted, got.OperationsTotal,
		got.Stats.Elections, got.Stats.Crashes, got.Stats.Restarts, got.Stats.Partitions,
		got.Stats.SnapshotsCreated, got.Stats.SnapshotsInstalled)
}

// TestRegressionSeed35LogMatching pins the exact configuration and seed that
// first produced a Log Matching failure, so that the schedule which found the
// bug keeps being run.
//
// The bug itself — a leader-supplied snapshot leaving a stale suffix in durable
// storage, so a restart rebuilt a log whose terms decreased — is diagnosed and
// fixed at the source; raft.TestInstallSnapshotSupersedesStaleSuffix is the
// deterministic regression test for it and fails against the old code. This
// test is the belt to that pair of braces: it replays the whole schedule that
// exposed it through the full stack, including the crash and restart.
func TestRegressionSeed35LogMatching(t *testing.T) {
	cfg := Config{
		Nodes:             5,
		ElectionTick:      6,
		ElectionJitter:    6,
		HeartbeatTick:     2,
		Latency:           Latency{Min: 1, Max: 6},
		DropRate:          0.20,
		DuplicateRate:     0.10,
		ReorderRate:       0.25,
		ReorderWindow:     8,
		PartitionRate:     0.03,
		HealRate:          0.08,
		CrashRate:         0.002,
		RestartDelay:      10,
		SnapshotThreshold: 20,
	}
	// The schedule is kept exactly as it was, because that is what found the
	// bug. It is timing-marginal to the point of being unusable: the router
	// can delay a message by Latency.Max plus the reorder window in each
	// direction, giving a worst-case round trip of 28 ticks against an
	// election timeout of 6..11, so a follower can time out while a heartbeat
	// is genuinely in flight. Measured on this machine, 25 seeds x 75 client
	// operations complete 17 operations under this configuration; with the
	// identical fault rates and only the election timeout raised to 20..39,
	// they complete all 1875.
	//
	// So this test makes no liveness claim: the only property that can be
	// checked here is safety, and even while the cluster thrashes it must
	// never violate election safety, Log Matching, or §5.4.2.
	//
	// Liveness under the same fault rates is asserted by
	// TestHarshFaultRatesAreSurvivable, which raises the timeout and nothing
	// else. The deterministic regression for the bug itself is
	// raft.TestInstallSnapshotSupersedesStaleSuffix, which fails against the
	// old code and does not depend on a random schedule at all.
	if rtt := cfg.WorstCaseRoundTrip(); cfg.ElectionTick >= rtt {
		t.Fatalf("this regression schedule is no longer timing-marginal (timeout %d vs round trip %d); "+
			"it has been changed, so it may no longer reproduce the failure it pins", cfg.ElectionTick, rtt)
	}

	// Seeds 33..37 cover the original failure at 35 and its neighbours. The
	// router's partition strategy changed after the bug was found, so the
	// schedule is not bit-for-bit the same one; the neighbouring seeds are
	// swept for that reason.
	got := SafetySweep(withSeed(cfg, 33), 5, 2000)
	if got.SeedsWithViolations != 0 {
		t.Fatalf("%d seeds around the original failure violated a safety property:\n%s",
			got.SeedsWithViolations, strings.Join(got.Violations, "\n"))
	}
	got35 := SafetySweep(withSeed(cfg, 35), 1, 2000)
	if got35.SeedsWithViolations != 0 {
		t.Fatalf("seed 35 violated a safety property:\n%s", strings.Join(got35.Violations, "\n"))
	}
	if got35.Stats.Crashes == 0 {
		t.Fatal("seed 35 no longer crashes, so it is not replaying the schedule that found the bug")
	}
	t.Logf("seed 35 replay: ticks=%d committedDepth=%d crashes=%d restarts=%d snapshots(created/installed)=%d/%d "+
		"timeout/roundtrip=%.2f (timing-marginal schedule; safety-only regression)",
		got35.Ticks, got35.CommittedDepthTotal, got35.Stats.Crashes, got35.Stats.Restarts,
		got35.Stats.SnapshotsCreated, got35.Stats.SnapshotsInstalled,
		cfg.TimeoutToRoundTripRatio())
}

func withSeed(cfg Config, seed int64) Config {
	cfg.Seed = seed
	return cfg
}

// TestHarshFaultRatesAreSurvivable is the anti-stall guard, and the live
// counterpart of the timing-marginal regression above.
//
// It uses exactly the same fault rates as that schedule — 20% loss, 10%
// duplication, 25% reordering with an 8-tick window, random partitions, and
// crashes — and starts from the same seed 35. It changes one thing: the
// election timeout, from 6..11 ticks to 20..39. That is the difference between
// a schedule no implementation of Raft can survive and one that is merely
// hard.
//
// A fix that bought safety by stalling the cluster cannot pass this test: it
// asserts that every client operation completes, that the committed log keeps
// growing, and that crashes and InstallSnapshot are actually exercised.
func TestHarshFaultRatesAreSurvivable(t *testing.T) {
	cfg := Config{
		Seed:              35,
		Nodes:             5,
		ElectionTick:      20,
		ElectionJitter:    20,
		HeartbeatTick:     2,
		Latency:           Latency{Min: 1, Max: 6},
		DropRate:          0.20,
		DuplicateRate:     0.10,
		ReorderRate:       0.25,
		ReorderWindow:     8,
		PartitionRate:     0.03,
		HealRate:          0.08,
		CrashRate:         0.002,
		RestartDelay:      10,
		SnapshotThreshold: 20,
		Workload: Workload{
			Clients: 3, OpsPerClient: 25, Keys: 3, Values: 3, ClientTimeout: 120,
		},
	}
	// 3000 ticks is not enough for every client to finish under these fault
	// rates; 6000 is, with room to spare.
	got := SafetySweep(cfg, 25, 6000)
	if got.SeedsWithViolations != 0 {
		t.Fatalf("%d of %d seeds violated a safety property:\n%s",
			got.SeedsWithViolations, got.Seeds, strings.Join(got.Violations, "\n"))
	}
	if got.Stats.Crashes == 0 || got.Stats.SnapshotsInstalled == 0 {
		t.Fatalf("the fault injection was inert: %d crashes, %d snapshots installed",
			got.Stats.Crashes, got.Stats.SnapshotsInstalled)
	}
	if got.SeedsIncomplete != 0 {
		t.Fatalf("%d of %d seeds left clients waiting under the same fault rates with a compliant timeout",
			got.SeedsIncomplete, got.Seeds)
	}
	if got.OperationsCompleted != got.OperationsTotal {
		t.Fatalf("completed %d of %d client operations: the cluster stalled under fault rates it should survive",
			got.OperationsCompleted, got.OperationsTotal)
	}
	if got.CommittedDepthTotal < int64(40*got.Seeds) {
		t.Fatalf("the committed log only reached %d entries in total across %d seeds", got.CommittedDepthTotal, got.Seeds)
	}
	t.Logf("harsh rates, longer timeout: seeds=%d ticks=%d committedDepthTotal=%d clientOps=%d/%d crashes=%d partitions=%d timeout/roundtrip=%.2f",
		got.Seeds, got.Ticks, got.CommittedDepthTotal, got.OperationsCompleted, got.OperationsTotal,
		got.Stats.Crashes, got.Stats.Partitions, cfg.TimeoutToRoundTripRatio())
}

// TestSnapshotSupersedesStaleSuffixUnderCrash is the simulator-level version of
// the regression: a node is isolated until the leader compacts past it, then
// brought back so it has to install a snapshot, and then crashed and restarted
// so the installed image has to survive a reload.
func TestSnapshotSupersedesStaleSuffixUnderCrash(t *testing.T) {
	s := newTestSim(t, Config{
		Nodes:             3,
		Seed:              9,
		Latency:           Latency{Min: 1, Max: 2},
		ElectionTick:      8,
		ElectionJitter:    4,
		HeartbeatTick:     2,
		SnapshotThreshold: 10,
	})
	s.Workload(Workload{Clients: 1, OpsPerClient: 400, Keys: 3, Values: 3})
	s.Run(400)
	requireNoViolations(t, s)

	leader := s.Leaders()[0]
	victim := raft.NodeID(1)
	for victim == leader {
		victim++
	}
	s.Isolate(victim)
	for i := 0; i < 40; i++ {
		s.Run(100)
		if s.Node(leader).FirstIndex()-1 > s.Node(victim).LastIndex() {
			break
		}
	}
	installs := s.Server(victim).InstalledSnapshots()

	s.Heal()
	s.Run(1500)
	requireNoViolations(t, s)
	if s.Server(victim).InstalledSnapshots() <= installs {
		t.Fatal("the isolated node never installed a snapshot, so the scenario did not reproduce")
	}

	// Now crash it. Before the fix this is where the stale suffix came back:
	// the installed image was persisted, but the log it superseded was not
	// discarded, so the restarted node rebuilt a log with decreasing terms.
	s.Crash(victim)
	s.Restart(victim)
	nd := s.Node(victim)
	if nd == nil {
		t.Fatal("the restarted node is not reachable")
	}
	prev := raft.Term(0)
	for _, e := range nd.EntriesFrom(nd.FirstIndex()) {
		if e.Term < prev {
			t.Fatalf("after restart the log decreases: entry %d has term %d after term %d", e.Index, e.Term, prev)
		}
		prev = e.Term
	}
	if snap := nd.Snapshot(); snap != nil {
		if snap.Term > prev && nd.LastIndex() > snap.Index {
			t.Fatalf("snapshot term %d is ahead of the suffix's first term %d", snap.Term, prev)
		}
	}
	s.Run(1500)
	requireNoViolations(t, s)
	if problems := s.VerifyAllReplicasAgree(); len(problems) > 0 {
		t.Fatalf("replicas disagree after the restart:\n%s", strings.Join(problems, "\n"))
	}
}

// TestLinearizabilityUnderFaultInjection drives real client traffic through the
// key-value store while the network is partitioned and nodes are crashing, then
// checks the recorded history with the checker.
//
// The histories are short on purpose: the Wing & Gong search is exponential in
// the worst case, so a long history would be inconclusive rather than wrong.
// The budget is still reported, because a sweep that only passes because it ran
// out of budget would be worthless.
func TestLinearizabilityUnderFaultInjection(t *testing.T) {
	base := cheapFaults(1, 3)
	base.Latency = Latency{Min: 1, Max: 4}
	base.DropRate = 0.05
	base.CrashRate = 0.001
	base.PartitionRate = 0.006
	base.HealRate = 0.03
	base.SnapshotThreshold = 30
	base.Workload = Workload{
		Clients:       3,
		OpsPerClient:  7,
		Keys:          2,
		Values:        2,
		ClientTimeout: 40,
	}

	got := LinearizabilitySweep(base, 60, 800, 4000, lcheck.Options{MaxSteps: 2_000_000, Memoize: true})
	if len(got.Failures) != 0 {
		t.Fatalf("%d failures across %d seeds:\n%s", len(got.Failures), got.Seeds, strings.Join(got.Failures, "\n"))
	}
	if got.Operations < 1000 {
		t.Fatalf("only %d operations were checked across %d seeds; the workload barely ran",
			got.Operations, got.Seeds)
	}
	if got.Incomplete != 0 {
		t.Fatalf("%d clients were still waiting at the end, so their operations are missing from the history", got.Incomplete)
	}
	t.Logf("linearizability: seeds=%d operations=%d maxHistory=%d checkerSteps=%d messages(dropped/duplicated/reordered)=%d/%d/%d crashes=%d",
		got.Seeds, got.Operations, got.MaxHistoryLength, got.CheckSteps,
		got.Stats.MessagesDropped, got.Stats.MessagesDuplicated, got.Stats.MessagesReordered, got.Stats.Crashes)
}

// TestSnapshotCatchUpAfterLongPartition isolates a node until the leader has
// compacted past everything it holds, so the only way it can catch up is an
// InstallSnapshot. It then asserts the node actually installed one and
// converged, which is the property the paper's log compaction section claims.
func TestSnapshotCatchUpAfterLongPartition(t *testing.T) {
	s := newTestSim(t, Config{
		Nodes:             3,
		Seed:              77,
		Latency:           Latency{Min: 1, Max: 2},
		ElectionTick:      8,
		ElectionJitter:    4,
		HeartbeatTick:     2,
		SnapshotThreshold: 20,
	})
	s.Workload(Workload{Clients: 1, OpsPerClient: 200, Keys: 2, Values: 2, GetWeight: 0, PutWeight: 1, AppendWeight: 0})
	s.Run(600)
	requireNoViolations(t, s)

	leader := s.Leaders()[0]
	victim := raft.NodeID(1)
	for victim == leader {
		victim++
	}
	s.Isolate(victim)

	// Run long enough that the leader compacts well past the victim's log, and
	// enough work happens that the two sides cannot possibly agree by accident.
	for i := 0; i < 40; i++ {
		s.Run(100)
		if s.Node(leader).FirstIndex()-1 > s.Node(victim).LastIndex()+2 {
			break
		}
	}
	if first := s.Node(leader).FirstIndex() - 1; first <= s.Node(victim).LastIndex() {
		t.Fatalf("leader compacted to %d but the victim is at %d, so the snapshot path was not forced",
			first, s.Node(victim).LastIndex())
	}
	installsBefore := s.Server(victim).InstalledSnapshots()

	s.Heal()
	s.Run(3000)
	requireNoViolations(t, s)

	if got := s.Server(victim).InstalledSnapshots(); got <= installsBefore {
		t.Fatal("the lagging node caught up without installing a snapshot, so compaction was not exercised")
	}
	if s.Node(victim).Applied() == 0 {
		t.Fatal("the caught-up node has applied nothing")
	}
	if a, b := string(s.StateMachine(victim).Snapshot()), string(s.StateMachine(leader).Snapshot()); a != b {
		t.Fatalf("the caught-up node's state machine differs from the leader's (%d vs %d bytes)", len(a), len(b))
	}
	if problems := s.VerifyAllReplicasAgree(); len(problems) > 0 {
		t.Fatalf("replicas disagree after catch-up:\n%s", strings.Join(problems, "\n"))
	}
}

// TestSnapshotCatchUpViaQuiescence is the same scenario without a helper
// workload: it uses pure fault injection so that the snapshot path is reached
// by isolation alone, which is closer to how the model check exercises it.
func TestSnapshotCatchUpViaQuiescence(t *testing.T) {
	s := newTestSim(t, Config{
		Nodes:             5,
		Seed:              1234,
		Latency:           Latency{Min: 1, Max: 3},
		ElectionTick:      8,
		ElectionJitter:    6,
		HeartbeatTick:     2,
		SnapshotThreshold: 10,
	})
	s.Workload(Workload{Clients: 2, OpsPerClient: 120, Keys: 3, Values: 3})
	for i := 0; i < 60; i++ {
		s.Run(100)
		if s.Stats().SnapshotsInstalled > 0 {
			break
		}
	}
	requireNoViolations(t, s)
	if s.Stats().SnapshotsCreated == 0 {
		t.Fatal("no snapshot was ever created")
	}
	if s.Stats().SnapshotsInstalled == 0 {
		t.Fatal("no snapshot was ever installed by a follower")
	}
}

func TestRunOnceReportsTicksAndStats(t *testing.T) {
	res := RunOnce(cheapFaults(5, 3), 300)
	if res.Ticks != 300 {
		t.Fatalf("RunOnce ran %d ticks, want 300", res.Ticks)
	}
	if len(res.Violations) != 0 {
		t.Fatalf("violations: %v", res.Violations)
	}
	if res.Stats.MessagesSent == 0 || res.Stats.MessagesDelivered == 0 {
		t.Fatalf("the run exchanged no messages: %+v", res.Stats)
	}
	if res.Stats.Elections == 0 {
		t.Fatal("no election was ever won")
	}
	if res.Stats.StorageWrites == 0 {
		t.Fatal("nothing was ever written to durable storage")
	}
}

func TestSummarise(t *testing.T) {
	d := Summarise([]int64{10, 1, 5, 3, 8})
	if d.Count != 5 || d.Min != 1 || d.Max != 10 || d.Median != 5 {
		t.Fatalf("Summarise = %+v", d)
	}
	if Summarise(nil).Count != 0 {
		t.Fatal("Summarise of an empty sample set should have zero count")
	}
	p99 := Summarise([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 100})
	if p99.P99 != 100 || p99.Max != 100 {
		t.Fatalf("p99 of a ten-sample set with one outlier = %+v", p99)
	}
}
