// Command graftbench produces the measured numbers quoted in the README.
//
// Everything it prints is measured on the machine that runs it and written to
// results/ in the same pass, so no figure in the README can drift away from the
// artifact that produced it. Two kinds of number appear side by side:
//
//   - Virtual-time numbers (throughput per simulated second, election latency
//     in ticks, client-observed latency in ticks). The simulator's clock is
//     discrete and one tick is one millisecond of configured network behaviour,
//     so these are properties of the cluster and the fault model, not of the
//     host. They reproduce exactly from the seed.
//
//   - Wall-clock numbers (how long the host took to produce the above). These
//     reproduce only on the same machine, and are labelled as such.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/eigendash/graft/lcheck"
	"github.com/eigendash/graft/sim"
)

func main() {
	out := flag.String("out", "results", "directory to write results into")
	seeds := flag.Int("seeds", 200, "seeds per model-check sweep")
	linSeeds := flag.Int("linseeds", 60, "seeds for the linearizability sweep")
	electionSamples := flag.Int("elections", 200, "election-time samples per cluster size")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "graftbench:", err)
		os.Exit(1)
	}
	r := &report{}
	r.Section("environment")
	r.Printf("go_version       %s", runtime.Version())
	r.Printf("goos/goarch      %s/%s", runtime.GOOS, runtime.GOARCH)
	r.Printf("cpus             %d", runtime.NumCPU())
	r.Printf("started          %s", time.Now().UTC().Format(time.RFC3339))
	r.Printf("sim_seeds        model=%d linearizability=%d elections=%d", *seeds, *linSeeds, *electionSamples)

	r.Section("model check: 3 nodes, cheap faults")
	cheap3 := cheapFaults(1, 3)
	r.Safety("sweep", cheap3, *seeds, 1200)

	r.Section("model check: 5 nodes, cheap faults")
	cheap5 := cheapFaults(90001, 5)
	r.Safety("sweep", cheap5, *seeds, 1200)

	r.Section("model check: 5 nodes, harsh faults, 250 seeds")
	harsh := harshConfig()
	r.Safety("sweep", harsh, 250, 2000)

	r.Section("model check: same fault rates, viable election timeout (seed 35..59)")
	survivable := survivableConfig()
	r.Safety("sweep", survivable, 25, 6000)

	r.Section("timing bisection: the same fault rates under different election timeouts")
	r.TimingBisection()

	r.Section("linearizability under fault injection")
	lin := linearizabilityConfig()
	r.Linearizability(lin, *linSeeds, 800, 4000)

	r.Section("election timing, in virtual ticks (1 tick = 1ms)")
	for _, n := range []int{3, 5, 7} {
		e := sim.MeasureElections(n, *electionSamples, sim.Config{
			Nodes: n, ElectionTick: 10, ElectionJitter: 10, HeartbeatTick: 2,
			Latency: sim.Latency{Min: 1, Max: 3},
		})
		r.Election(e)
	}

	r.Section("kv throughput and latency by cluster size and hop latency")
	// Two delay models. Under a uniform delay the commit path is a single
	// round trip whatever the quorum size, so throughput is flat in the number
	// of nodes; that is a real property of the model, not a measurement
	// artefact, and the jittered run shows the cost a real network adds.
	spreads := []struct {
		name string
		f    func(hop int) sim.Latency
	}{
		{"uniform", func(hop int) sim.Latency { return sim.Latency{Min: hop, Max: hop} }},
		{"jittered", func(hop int) sim.Latency { return sim.Latency{Min: hop, Max: 2 * hop} }},
	}
	for _, spread := range spreads {
		r.Printf("-- delay model: %s --", spread.name)
		for _, n := range []int{3, 5, 7} {
			for _, hop := range []int{1, 10, 50} {
				// Raft requires broadcastTime << electionTimeout, so the timeout has
				// to scale with the hop latency: a round trip is 2*hop ticks, the
				// leader heartbeats once per round trip, and the election timeout is
				// ten round trips. Holding the timeout fixed while the latency grows
				// would measure the timing violation, not the cluster.
				rtt := 2 * hop
				work := sim.Workload{
					Clients: 12, OpsPerClient: 200, Keys: 8, Values: 6,
					ClientTimeout: 20 * rtt,
				}
				cfg := sim.Config{
					Nodes:        n,
					ElectionTick: 10 * rtt, ElectionJitter: 10 * rtt, HeartbeatTick: hop,
					Latency:           spread.f(hop),
					MaxEntriesPerMsg:  64,
					MaxInflight:       64,
					SnapshotThreshold: 0,
				}
				wall := time.Now()
				res := sim.MeasureKV(cfg, work, 40_000_000)
				elapsed := time.Since(wall)
				r.Throughput(spread.name, res, elapsed)
			}
		}
	}

	r.Section("determinism")
	r.Determinism()

	r.Section("limitations exercised by the harness itself")
	r.Printf("lcheck_budget_per_history  %d candidate operations (DefaultMaxSteps)", lcheck.DefaultMaxSteps)
	r.Printf("note                      lcheck is exponential in the worst case; histories are kept short on purpose")

	text := r.String()
	if err := os.WriteFile(filepath.Join(*out, "timing-bisection.txt"), []byte(r.bisection.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "graftbench:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(*out, "graftbench.txt"), []byte(text), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "graftbench:", err)
		os.Exit(1)
	}
	summary, err := json.MarshalIndent(r.summary, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "graftbench:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(*out, "summary.json"), append(summary, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "graftbench:", err)
		os.Exit(1)
	}
	fmt.Print(text)
	fmt.Println("wrote results/timing-bisection.txt")
	if r.failed {
		fmt.Fprintln(os.Stderr, "graftbench: at least one measurement was not a pass")
		os.Exit(1)
	}
	fmt.Printf("wrote %s and %s\n", filepath.Join(*out, "graftbench.txt"), filepath.Join(*out, "summary.json"))
}

// --- configuration -----------------------------------------------------------

func cheapFaults(seed int64, nodes int) sim.Config {
	return sim.Config{
		Seed: seed, Nodes: nodes,
		ElectionTick: 8, ElectionJitter: 8, HeartbeatTick: 2,
		Latency:  sim.Latency{Min: 1, Max: 3},
		DropRate: 0.08, DuplicateRate: 0.03, ReorderRate: 0.08, ReorderWindow: 4,
		PartitionRate: 0.015, HealRate: 0.04,
		CrashRate: 0.002, RestartDelay: 15,
		SnapshotThreshold: 40,
		// A workload, so the sweep asserts liveness as well as safety. A
		// cluster that never commits anything is trivially safe.
		Workload: sim.Workload{Clients: 2, OpsPerClient: 12, Keys: 3, Values: 3, ClientTimeout: 200},
	}
}

func harshConfig() sim.Config {
	return sim.Config{
		Seed: 0, Nodes: 5,
		ElectionTick: 8, ElectionJitter: 8, HeartbeatTick: 2,
		Latency:  sim.Latency{Min: 1, Max: 4},
		DropRate: 0.15, DuplicateRate: 0.10, ReorderRate: 0.15, ReorderWindow: 4,
		PartitionRate: 0.01, HealRate: 0.06,
		CrashRate: 0.0015, RestartDelay: 8,
		SnapshotThreshold: 20,
		Workload:          sim.Workload{Clients: 3, OpsPerClient: 20, Keys: 3, Values: 3, ClientTimeout: 60},
	}
}

func survivableConfig() sim.Config {
	return sim.Config{
		Seed: 35, Nodes: 5,
		ElectionTick: 20, ElectionJitter: 20, HeartbeatTick: 2,
		Latency:  sim.Latency{Min: 1, Max: 6},
		DropRate: 0.20, DuplicateRate: 0.10, ReorderRate: 0.25, ReorderWindow: 8,
		PartitionRate: 0.03, HealRate: 0.08,
		CrashRate: 0.002, RestartDelay: 10,
		SnapshotThreshold: 20,
		Workload:          sim.Workload{Clients: 3, OpsPerClient: 25, Keys: 3, Values: 3, ClientTimeout: 120},
	}
}

func linearizabilityConfig() sim.Config {
	cfg := cheapFaults(1, 3)
	cfg.Latency = sim.Latency{Min: 1, Max: 4}
	cfg.DropRate = 0.05
	cfg.CrashRate = 0.001
	cfg.PartitionRate = 0.006
	cfg.HealRate = 0.03
	cfg.SnapshotThreshold = 30
	cfg.Workload = sim.Workload{Clients: 3, OpsPerClient: 7, Keys: 2, Values: 2, ClientTimeout: 40}
	return cfg
}

// --- reporting ---------------------------------------------------------------

type report struct {
	b         strings.Builder
	bisection strings.Builder
	summary   []map[string]any
	failed    bool
}

func (r *report) Section(name string) {
	fmt.Fprintf(&r.b, "\n== %s ==\n", name)
}

func (r *report) Printf(format string, args ...any) {
	fmt.Fprintf(&r.b, format+"\n", args...)
}

func (r *report) record(kind string, fields map[string]any) {
	fields["kind"] = kind
	r.summary = append(r.summary, fields)
}

func (r *report) String() string { return r.b.String() }

func (r *report) Safety(name string, cfg sim.Config, seeds, ticks int) {
	wall := time.Now()
	got := sim.SafetySweep(cfg, seeds, ticks)
	elapsed := time.Since(wall)
	verdict := "PASS"
	if got.SeedsWithViolations != 0 || got.SeedsIncomplete != 0 ||
		(got.OperationsTotal > 0 && got.OperationsCompleted != got.OperationsTotal) {
		verdict = "FAIL"
		r.failed = true
	}
	r.Printf("nodes                    %d", cfg.Nodes)
	r.Printf("seeds                    %d", got.Seeds)
	r.Printf("ticks_per_seed           %d", ticks)
	r.Printf("seeds_with_violations    %d", got.SeedsWithViolations)
	r.Printf("committed_log_depth      %d (sum over seeds; %d entries/seed)",
		got.CommittedDepthTotal, got.CommittedDepthTotal/int64(got.Seeds))
	r.Printf("client_operations        %d/%d", got.OperationsCompleted, got.OperationsTotal)
	r.Printf("elections                %d", got.Stats.Elections)
	r.Printf("crashes/restarts         %d/%d", got.Stats.Crashes, got.Stats.Restarts)
	r.Printf("partitions/heals         %d/%d", got.Stats.Partitions, got.Stats.Heals)
	r.Printf("snapshots_created        %d", got.Stats.SnapshotsCreated)
	r.Printf("snapshots_installed      %d", got.Stats.SnapshotsInstalled)
	r.Printf("messages                 sent=%d delivered=%d dropped=%d duplicated=%d reordered=%d",
		got.Stats.MessagesSent, got.Stats.MessagesDelivered, got.Stats.MessagesDropped,
		got.Stats.MessagesDuplicated, got.Stats.MessagesReordered)
	r.Printf("storage_writes           %d", got.Stats.StorageWrites)
	r.Printf("wall_clock               %s", elapsed)
	r.Printf("verdict                  %s", verdict)
	if len(got.Failures) > 0 {
		for i, f := range got.Failures {
			if i >= 5 {
				r.Printf("failure                  ... and %d more", len(got.Failures)-i)
				break
			}
			r.Printf("failure                  %s", f)
		}
	}
	r.record("safety_sweep", map[string]any{
		"nodes": cfg.Nodes, "seeds": got.Seeds, "ticks_per_seed": ticks,
		"seeds_with_violations": got.SeedsWithViolations,
		"committed_log_depth":   got.CommittedDepthTotal,
		"operations_completed":  got.OperationsCompleted,
		"operations_total":      got.OperationsTotal,
		"elections":             got.Stats.Elections,
		"crashes":               got.Stats.Crashes,
		"partitions":            got.Stats.Partitions,
		"snapshots_created":     got.Stats.SnapshotsCreated,
		"snapshots_installed":   got.Stats.SnapshotsInstalled,
		"messages_sent":         got.Stats.MessagesSent,
		"messages_delivered":    got.Stats.MessagesDelivered,
		"messages_dropped":      got.Stats.MessagesDropped,
		"storage_writes":        got.Stats.StorageWrites,
		"messages_duplicated":   got.Stats.MessagesDuplicated,
		"messages_reordered":    got.Stats.MessagesReordered,
		"wall_clock_seconds":    elapsed.Seconds(),
		"verdict":               verdict,
	})
}

func (r *report) Linearizability(cfg sim.Config, seeds, faultTicks, drainTicks int) {
	opts := lcheck.Options{MaxSteps: 2_000_000, Memoize: true}
	wall := time.Now()
	got := sim.LinearizabilitySweep(cfg, seeds, faultTicks, drainTicks, opts)
	elapsed := time.Since(wall)
	verdict := "PASS"
	if len(got.Failures) != 0 {
		verdict = "FAIL"
		r.failed = true
	}
	r.Printf("seeds                    %d", got.Seeds)
	r.Printf("fault_ticks/drain_ticks  %d/%d", faultTicks, drainTicks)
	r.Printf("histories_checked        %d", got.Seeds)
	r.Printf("operations_checked       %d", got.Operations)
	r.Printf("longest_history          %d", got.MaxHistoryLength)
	r.Printf("checker_candidate_steps  %d", got.CheckSteps)
	r.Printf("seeds_not_linearizable   %d", got.SeedsNotLinearizable)
	r.Printf("clients_left_waiting     %d", got.Incomplete)
	r.Printf("elections                %d", got.Stats.Elections)
	r.Printf("crashes                  %d", got.Stats.Crashes)
	r.Printf("messages                 dropped=%d duplicated=%d reordered=%d",
		got.Stats.MessagesDropped, got.Stats.MessagesDuplicated, got.Stats.MessagesReordered)
	r.Printf("wall_clock               %s", elapsed)
	r.Printf("verdict                  %s", verdict)
	for i, f := range got.Failures {
		if i >= 5 {
			r.Printf("failure                  ... and %d more", len(got.Failures)-i)
			break
		}
		r.Printf("failure                  %s", f)
	}
	r.record("linearizability", map[string]any{
		"seeds": got.Seeds, "operations_checked": got.Operations,
		"longest_history": got.MaxHistoryLength, "checker_steps": got.CheckSteps,
		"seeds_not_linearizable": got.SeedsNotLinearizable,
		"clients_left_waiting":   got.Incomplete,
		"crashes":                got.Stats.Crashes,
		"wall_clock_seconds":     elapsed.Seconds(),
		"verdict":                verdict,
	})
}

// originalTimingConfig is the configuration whose seed-35 run first exposed the
// snapshot bug, kept exactly as it was: 20% loss, 10% duplication, 25%
// reordering with an 8-tick window, partitions and crashes, against an election
// timeout of 6..11 ticks. The router can delay a message by up to 14 ticks in
// each direction, so the worst-case round trip is 28 ticks and a follower can
// time out while a heartbeat is genuinely in flight.
func originalTimingConfig(seed int64) sim.Config {
	return sim.Config{
		Seed: seed, Nodes: 5,
		ElectionTick: 6, ElectionJitter: 6, HeartbeatTick: 2,
		Latency:  sim.Latency{Min: 1, Max: 6},
		DropRate: 0.20, DuplicateRate: 0.10, ReorderRate: 0.25, ReorderWindow: 8,
		PartitionRate: 0.03, HealRate: 0.08,
		CrashRate: 0.002, RestartDelay: 10,
		SnapshotThreshold: 20,
	}
}

// TimingBisection is the evidence behind the claim that the original
// configuration was timing-marginal rather than stalled. Every row keeps the
// fault rates identical and changes one thing, so the effect of the change is
// attributable.
func (r *report) TimingBisection() {
	const seeds, ticks = 25, 3000
	work := sim.Workload{Clients: 3, OpsPerClient: 25, Keys: 3, Values: 3, ClientTimeout: 60}
	base := originalTimingConfig(0)

	withTimeout := func(et, jitter int) sim.Config {
		c := base
		c.ElectionTick, c.ElectionJitter = et, jitter
		return c
	}
	noLoss := func() sim.Config {
		c := base
		c.DropRate, c.DuplicateRate, c.ReorderRate = 0, 0, 0
		return c
	}

	rows := []sim.BisectionRow{
		sim.MeasureBisection("original et=6+j6 (worst-case RTT 28)", base, seeds, ticks, work),
		sim.MeasureBisection("same faults, et=12+j12", withTimeout(12, 12), seeds, ticks, work),
		sim.MeasureBisection("same faults, et=20+j20", withTimeout(20, 20), seeds, ticks, work),
		sim.MeasureBisection("original et=6+j6, zero loss/dup/reorder", noLoss(), seeds, ticks, work),
		sim.MeasureBisection("new harsh et=8+j8, lat<=4, reorder window 4", harshConfig(), seeds, ticks, work),
	}
	r.Printf("seeds=%d ticks_per_seed=%d workload=%d clients x %d ops",
		seeds, ticks, work.Clients, work.OpsPerClient)
	r.Printf("")
	r.Printf("%-46s %-13s %-11s %-9s %-11s %s",
		"configuration", "client ops", "committed", "leaderless", "quorumless", "elections")
	for _, row := range rows {
		ops := fmt.Sprintf("%d/%d", row.ClientOps, row.ClientOpsTotal)
		verdict := "PASS"
		if row.ClientOps != row.ClientOpsTotal {
			verdict = "STALLED"
		}
		line := fmt.Sprintf("%-46s %-13s %-11d %-9.3f %-11.3f %d   %s",
			row.Name, ops, row.CommittedDepth,
			row.LeaderlessFraction, row.QuorumlessFraction, row.Elections, verdict)
		r.Printf("%s", line)
		if r.bisection.Len() == 0 {
			fmt.Fprintf(&r.bisection, "timing bisection: the same fault rates under different election timeouts\n")
			fmt.Fprintf(&r.bisection, "seeds=%d ticks_per_seed=%d workload=%d clients x %d ops\n\n",
				seeds, ticks, work.Clients, work.OpsPerClient)
			fmt.Fprintf(&r.bisection, "%-46s %-13s %-11s %-9s %-11s %s\n",
				"configuration", "client ops", "committed", "leaderless", "quorumless", "elections")
		}
		fmt.Fprintf(&r.bisection, "%s\n", line)
		r.record("timing_bisection", map[string]any{
			"configuration":       row.Name,
			"seeds":               row.Seeds,
			"ticks":               row.Ticks,
			"client_ops":          row.ClientOps,
			"client_ops_total":    row.ClientOpsTotal,
			"committed_depth":     row.CommittedDepth,
			"leaderless_fraction": row.LeaderlessFraction,
			"quorumless_fraction": row.QuorumlessFraction,
			"elections":           row.Elections,
			"election_tick":       row.Config.ElectionTick,
			"election_jitter":     row.Config.ElectionJitter,
			"worst_case_rtt":      row.Config.WorstCaseRoundTrip(),
			"drop_rate":           row.Config.DropRate,
			"duplicate_rate":      row.Config.DuplicateRate,
			"reorder_rate":        row.Config.ReorderRate,
			"verdict":             verdict,
		})
	}
}

func (r *report) Election(e sim.ElectionStats) {
	r.Printf("nodes=%d samples=%d cold: min=%d median=%d p90=%d p99=%d max=%d | failover: min=%d median=%d p90=%d p99=%d max=%d",
		e.Nodes, e.Samples,
		e.Cold.Min, e.Cold.Median, e.Cold.P90, e.Cold.P99, e.Cold.Max,
		e.Failover.Min, e.Failover.Median, e.Failover.P90, e.Failover.P99, e.Failover.Max)
	r.record("election_stats", map[string]any{
		"nodes": e.Nodes, "samples": e.Samples,
		"cold_min": e.Cold.Min, "cold_median": e.Cold.Median, "cold_p90": e.Cold.P90,
		"cold_p99": e.Cold.P99, "cold_max": e.Cold.Max,
		"failover_min": e.Failover.Min, "failover_median": e.Failover.Median,
		"failover_p90": e.Failover.P90, "failover_p99": e.Failover.P99, "failover_max": e.Failover.Max,
	})
}

func (r *report) Throughput(spread string, res sim.ThroughputResult, wall time.Duration) {
	verdict := "PASS"
	if !res.Completed {
		verdict = "FAIL (did not finish)"
		r.failed = true
	}
	r.Printf("delay=%-8s nodes=%d hop_latency=%dms clients=%d ops=%d election_ticks=%d measured_ticks=%d ops_per_virtual_second=%.0f "+
		"client_latency_ms(median/p99/max)=%d/%d/%d wall_clock=%s %s",
		spread, res.Nodes, res.HopLatency, res.Clients, res.Operations, res.ElectionTicks, res.VirtualTicks,
		res.OpsPerVirtualSecond(),
		res.Latency.Median, res.Latency.P99, res.Latency.Max, wall, verdict)
	r.record("throughput", map[string]any{
		"delay_model": spread,
		"nodes":       res.Nodes, "hop_latency_ms": res.HopLatency, "clients": res.Clients,
		"operations": res.Operations, "election_ticks": res.ElectionTicks,
		"virtual_ticks":            res.VirtualTicks,
		"ops_per_virtual_second":   res.OpsPerVirtualSecond(),
		"client_latency_median_ms": res.Latency.Median,
		"client_latency_p99_ms":    res.Latency.P99,
		"client_latency_max_ms":    res.Latency.Max,
		"wall_clock_seconds":       wall.Seconds(),
		"completed":                res.Completed,
		"verdict":                  verdict,
	})
}

// Determinism runs one schedule twice and compares the event logs byte for
// byte. It is the property that makes every other failure here reproducible.
func (r *report) Determinism() {
	cfg := sim.Config{
		Nodes: 5, Seed: 424242,
		ElectionTick: 8, ElectionJitter: 8, HeartbeatTick: 2,
		Latency:  sim.Latency{Min: 1, Max: 5},
		DropRate: 0.10, DuplicateRate: 0.05, ReorderRate: 0.10, ReorderWindow: 4,
		PartitionRate: 0.02, HealRate: 0.05, CrashRate: 0.004, RestartDelay: 10,
		Trace: true, Invariants: true,
		Workload: sim.Workload{Clients: 4, OpsPerClient: 15, Keys: 3, Values: 3, ClientTimeout: 40},
	}
	run := func() *sim.Sim {
		s, err := sim.New(cfg)
		if err != nil {
			panic(err)
		}
		s.Run(1200)
		return s
	}
	a, b := run(), run()
	identical := a.EventDigest() == b.EventDigest() && len(a.Events()) == len(b.Events())
	verdict := "PASS"
	if !identical {
		verdict = "FAIL"
		r.failed = true
	}
	r.Printf("events_recorded          %d", len(a.Events()))
	r.Printf("digest_run_a             %#016x", a.EventDigest())
	r.Printf("digest_run_b             %#016x", b.EventDigest())
	r.Printf("identical                %v", identical)
	r.Printf("verdict                  %s", verdict)
	r.record("determinism", map[string]any{
		"events": len(a.Events()), "digest_a": a.EventDigest(), "digest_b": b.EventDigest(),
		"identical": identical, "verdict": verdict,
	})
}
