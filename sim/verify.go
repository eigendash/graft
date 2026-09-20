package sim

import (
	"sort"

	"github.com/eigendash/graft/lcheck"
	"github.com/eigendash/graft/raft"
)

// RunOnce executes one simulation and returns what it observed. It is the unit
// of work the sweeps are built from, and it deliberately contains no timing:
// the simulator measures virtual ticks, and a caller that wants wall clock
// takes it outside.
func RunOnce(cfg Config, ticks int) RunResult {
	s, err := New(cfg)
	if err != nil {
		return RunResult{Seed: cfg.Seed, Nodes: cfg.Nodes, Violations: []string{err.Error()}}
	}
	s.Run(ticks)
	res := RunResult{
		Seed:       cfg.Seed,
		Nodes:      cfg.Nodes,
		Ticks:      s.Now(),
		Stats:      s.Stats(),
		Violations: s.Violations(),
	}
	if cfg.Workload.Clients > 0 {
		res.Workload = s.WorkloadProgress()
		res.History = s.History()
	}
	return res
}

// RunResult summarises one simulation.
type RunResult struct {
	Seed       int64
	Nodes      int
	Ticks      int64
	Violations []string
	Stats      Stats
	Workload   WorkloadState
	History    lcheck.History
}

// SafetySweepResult aggregates a sweep whose only assertion is the safety
// invariants.
type SafetySweepResult struct {
	Seeds               int
	SeedsWithViolations int
	Ticks               int64
	Stats               Stats
	Violations          []string
	// OperationsCompleted and OperationsTotal make the sweep's liveness claim
	// explicit. A sweep with no workload has zero of both, so a test that
	// wants to assert liveness must assert on these and not merely on the
	// absence of violations: a cluster that commits nothing is trivially safe.
	OperationsCompleted int
	OperationsTotal     int
	SeedsIncomplete     int
	// CommittedDepthTotal is the sum over seeds of the length of the committed
	// log. It is the liveness measure that cannot be inflated by replication
	// factor or catch-up, unlike a sum of per-node apply counts.
	CommittedDepthTotal int64
	// Failures records which seeds failed, so a failure can be replayed by
	// running that one seed with tracing on.
	Failures []int64
}

// SafetySweep runs Configuration.Seed, Seed+1, ... over a copy of base and
// reports the aggregate. Every run's invariants are checked after every tick,
// so a single lost or reordered committed entry fails the sweep.
func SafetySweep(base Config, seeds, ticks int) SafetySweepResult {
	var out SafetySweepResult
	out.Seeds = seeds
	for i := 0; i < seeds; i++ {
		cfg := base
		cfg.Seed = base.Seed + int64(i)
		cfg.Invariants = true
		// Traces are only useful when something goes wrong, and they cost an
		// allocation per message; the sweep runs without them.
		cfg.Trace = false
		res := RunOnce(cfg, ticks)
		out.Ticks += res.Ticks
		addStats(&out.Stats, res.Stats)
		if len(res.Violations) > 0 {
			out.SeedsWithViolations++
			out.Failures = append(out.Failures, cfg.Seed)
			for _, v := range res.Violations {
				out.Violations = append(out.Violations, "seed "+itoa(int64(cfg.Seed))+": "+v)
			}
		}
		out.CommittedDepthTotal += int64(res.Stats.MaxCommit)
		out.OperationsCompleted += res.Workload.Completed
		out.OperationsTotal += res.Workload.Total
		if res.Workload.Total > 0 && res.Workload.Completed != res.Workload.Total {
			out.SeedsIncomplete++
		}
	}
	return out
}

// LinearizabilitySweepResult aggregates a sweep that checks every recorded
// history with the linearizability checker.
type LinearizabilitySweepResult struct {
	Seeds                int
	SeedsWithViolations  int
	SeedsNotLinearizable int
	Ticks                int64
	Operations           int
	Incomplete           int
	CheckSteps           int
	MaxHistoryLength     int
	Stats                Stats
	Failures             []string
}

// LinearizabilitySweep runs a client workload under fault injection, then
// heals the cluster and lets the clients finish, and checks the resulting
// history. Operations left outstanding at the end are counted separately: a
// history with holes cannot be checked, so a sweep that leaves clients hanging
// reports itself as failed rather than as passing.
func LinearizabilitySweep(base Config, seeds, faultTicks, drainTicks int, opts lcheck.Options) LinearizabilitySweepResult {
	var out LinearizabilitySweepResult
	out.Seeds = seeds
	for i := 0; i < seeds; i++ {
		cfg := base
		cfg.Seed = base.Seed + int64(i)
		cfg.Invariants = true
		cfg.Trace = false
		s, err := New(cfg)
		if err != nil {
			out.Failures = append(out.Failures, "seed "+itoa(cfg.Seed)+": "+err.Error())
			continue
		}
		s.Run(faultTicks)
		// Faults stop here. Everything after is the "does it recover and can
		// the clients finish" phase, which is what makes a complete history
		// possible at all.
		s.Heal()
		for _, n := range s.nodes {
			if n.down {
				s.Restart(n.id)
			}
		}
		done := false
		for t := 0; t < drainTicks; t++ {
			s.Step()
			if p := s.WorkloadProgress(); p.Completed == p.Total {
				done = true
				break
			}
		}
		out.Ticks += s.Now()
		addStats(&out.Stats, s.Stats())
		if v := s.Violations(); len(v) > 0 {
			out.SeedsWithViolations++
			for _, m := range v {
				out.Failures = append(out.Failures, "seed "+itoa(cfg.Seed)+" invariant: "+m)
			}
		}
		prog := s.WorkloadProgress()
		if !done || prog.Stalled != 0 {
			out.Incomplete += prog.Stalled
			out.Failures = append(out.Failures, "seed "+itoa(cfg.Seed)+
				": workload finished with "+itoa(int64(prog.Stalled))+" clients still waiting")
		}
		h := s.History()
		out.Operations += len(h)
		if len(h) > out.MaxHistoryLength {
			out.MaxHistoryLength = len(h)
		}
		check, err := lcheck.Check(h, lcheck.NewKVModel(), opts)
		out.CheckSteps += check.Steps
		if err != nil {
			out.Failures = append(out.Failures, "seed "+itoa(cfg.Seed)+": checker: "+err.Error())
			continue
		}
		if !check.Linearizable {
			out.SeedsNotLinearizable++
			out.Failures = append(out.Failures, "seed "+itoa(cfg.Seed)+" is not linearizable: "+check.Explanation)
		}
	}
	return out
}

func addStats(dst *Stats, src Stats) {
	dst.Ticks += src.Ticks
	dst.MessagesSent += src.MessagesSent
	dst.MessagesDropped += src.MessagesDropped
	dst.MessagesDuplicated += src.MessagesDuplicated
	dst.MessagesReordered += src.MessagesReordered
	dst.MessagesDelivered += src.MessagesDelivered
	dst.MessagesToDead += src.MessagesToDead
	dst.Elections += src.Elections
	dst.Crashes += src.Crashes
	dst.Restarts += src.Restarts
	dst.SnapshotsCreated += src.SnapshotsCreated
	dst.SnapshotsInstalled += src.SnapshotsInstalled
	dst.EntriesCommitted += src.EntriesCommitted
	dst.StorageWrites += src.StorageWrites
	dst.Partitions += src.Partitions
	dst.Heals += src.Heals
	dst.LeaderlessTicks += src.LeaderlessTicks
	dst.QuorumlessTicks += src.QuorumlessTicks
	if src.MaxCommit > dst.MaxCommit {
		dst.MaxCommit = src.MaxCommit
	}
}

// ElectionStats is the distribution of election times, in ticks, for one
// cluster size.
type ElectionStats struct {
	Nodes    int
	Samples  int
	Cold     Distribution
	Failover Distribution
}

// Distribution is a small histogram summary. It is computed from every sample,
// not estimated.
type Distribution struct {
	Count  int
	Min    int64
	Median int64
	P90    int64
	P99    int64
	Max    int64
	Mean   int64
}

// Summarise reduces a sample set to a distribution.
func Summarise(samples []int64) Distribution {
	if len(samples) == 0 {
		return Distribution{}
	}
	s := append([]int64(nil), samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var sum int64
	for _, v := range s {
		sum += v
	}
	return Distribution{
		Count:  len(s),
		Min:    s[0],
		Median: percentile(s, 50),
		P90:    percentile(s, 90),
		P99:    percentile(s, 99),
		Max:    s[len(s)-1],
		Mean:   sum / int64(len(s)),
	}
}

// percentile uses the nearest-rank definition: the p-th percentile is the
// ceil(p*n/100)-th smallest sample. Unlike an interpolating definition it
// always returns a value that was actually observed, which is what these
// numbers are supposed to be.
func percentile(sorted []int64, p int) int64 {
	rank := (p*len(sorted) + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// MeasureElections reports how long a cluster takes to elect its first leader
// and how long failover takes once the sitting leader is cut off.
//
// The failover measurement isolates the leader rather than crashing it, so the
// number is the election timeout path alone and does not include the crash
// detection a real deployment would need.
func MeasureElections(nodes, samples int, cfg Config) ElectionStats {
	stats := ElectionStats{Nodes: nodes, Samples: samples}
	var coldSamples, failoverSamples []int64
	for i := 0; i < samples; i++ {
		cfg.Nodes = nodes
		cfg.Seed = cfg.Seed + 1
		cfg.Invariants = true
		cfg.Trace = false
		cfg.Workload = Workload{}
		s, err := New(cfg)
		if err != nil {
			continue
		}
		start := s.Now()
		first := raft.NodeID(0)
		for s.Now()-start < 2000 && first == 0 {
			s.Step()
			if l := s.Leaders(); len(l) > 0 {
				first = l[0]
			}
		}
		if first == 0 {
			continue
		}
		coldSamples = append(coldSamples, s.Now()-start)

		// Let the cluster settle so the failover measurement is not polluted
		// by a leader that had not finished replicating.
		s.Run(50)
		if l := s.Leaders(); len(l) != 1 {
			continue
		}
		leader := s.Leaders()[0]
		s.Isolate(leader)
		start = s.Now()
		next := raft.NodeID(0)
		for s.Now()-start < 2000 && next == 0 {
			s.Step()
			for _, id := range s.Leaders() {
				if id != leader {
					next = id
					break
				}
			}
		}
		if next == 0 {
			continue
		}
		failoverSamples = append(failoverSamples, s.Now()-start)
	}
	stats.Cold = Summarise(coldSamples)
	stats.Failover = Summarise(failoverSamples)
	return stats
}

// ThroughputResult measures a client workload against a healthy cluster.
type ThroughputResult struct {
	Nodes      int
	HopLatency int
	Operations int
	Clients    int
	// ElectionTicks is when the first leader appeared, or -1 if none did. It is
	// reported but excluded from VirtualTicks.
	ElectionTicks int64
	VirtualTicks  int64
	// Completed reports whether every client finished inside the budget. A
	// throughput number from a run that did not finish is meaningless, so
	// callers must check this.
	Completed bool
	Latency   Distribution
	Stats     Stats
	Elections int64
	Snapshots int64
}

// OpsPerVirtualSecond is the throughput the configuration implies. One
// simulated tick is one millisecond of configured network behaviour, so this
// number is a property of the cluster and the fault model rather than of the
// machine that produced it.
func (r ThroughputResult) OpsPerVirtualSecond() float64 {
	if r.VirtualTicks == 0 {
		return 0
	}
	return float64(r.Operations) * 1000 / float64(r.VirtualTicks)
}

// MeasureKV runs a client workload against a fault-free cluster until every
// client finishes or the tick budget runs out.
//
// The election that establishes the first leader is excluded from the measured
// window. Including it would make throughput a function of how long the cluster
// took to start rather than of how fast it serves, and the two are not
// comparable across configurations with different hop latencies.
func MeasureKV(cfg Config, w Workload, maxTicks int) ThroughputResult {
	cfg.Workload = w
	s, err := New(cfg)
	if err != nil {
		return ThroughputResult{Nodes: cfg.Nodes, HopLatency: cfg.Latency.Min, Clients: w.Clients}
	}
	started := int64(-1)
	for i := 0; i < maxTicks; i++ {
		if started < 0 && len(s.Leaders()) > 0 {
			started = s.Now()
		}
		if started >= 0 {
			if p := s.WorkloadProgress(); p.Completed == p.Total {
				break
			}
		}
		s.Step()
	}
	prog := s.WorkloadProgress()
	var window int64
	if started < 0 {
		window = s.Now()
	} else {
		window = s.Now() - started
	}
	return ThroughputResult{
		Nodes:         cfg.Nodes,
		HopLatency:    cfg.Latency.Min,
		Operations:    prog.Completed,
		Clients:       w.Clients,
		ElectionTicks: started,
		VirtualTicks:  window,
		Completed:     prog.Total > 0 && prog.Completed == prog.Total,
		Latency:       Summarise(s.LatencySamples()),
		Stats:         s.Stats(),
		Elections:     s.Stats().Elections,
		Snapshots:     s.Stats().SnapshotsCreated,
	}
}

// MeasureThroughput runs one workload against a fault-free cluster and reports
// how much work it completed per unit of virtual time.
//
// Virtual ticks are the right unit here: the simulator's clock advances in
// simulated milliseconds, so "operations per virtual second" is the latency the
// configuration implies, independent of how fast the host happens to be. The
// wall-clock cost of producing that number is measured by the caller.
func MeasureThroughput(cfg Config, ticks int, w Workload) ThroughputResult {
	cfg.Workload = w
	s, err := New(cfg)
	if err != nil {
		return ThroughputResult{Nodes: cfg.Nodes, HopLatency: cfg.Latency.Min}
	}
	s.Run(ticks)
	prog := s.WorkloadProgress()
	res := ThroughputResult{
		Nodes:        cfg.Nodes,
		HopLatency:   cfg.Latency.Min,
		Operations:   prog.Completed,
		VirtualTicks: s.Now(),
		Latency:      Summarise(s.LatencySamples()),
		Stats:        s.Stats(),
		Elections:    s.Stats().Elections,
		Snapshots:    s.Stats().SnapshotsCreated,
	}
	return res
}

// BisectionRow is one configuration's measured progress under a fault schedule.
//
// It exists so that the claim "this cluster was not stalled, it was
// timing-marginal" is a measurement rather than an assertion. The two fractions
// separate the two possible diagnoses: a high leaderless fraction with a low
// quorumless fraction means a majority was reachable and the cluster still
// could not agree, which is the signature of an election timeout that does not
// clear the network round trip.
type BisectionRow struct {
	Name   string
	Config Config
	Seeds  int
	Ticks  int64
	// ClientOps and ClientOpsTotal are the workload's completion, so a row that
	// completed nothing cannot be mistaken for a quiet success.
	ClientOps      int
	ClientOpsTotal int
	// CommittedDepth is the committed log length summed over seeds.
	CommittedDepth int64
	// LeaderlessFraction is the share of ticks on which no live node believed
	// it was leader, and QuorumlessFraction the share on which no connected
	// component of live nodes held a majority.
	LeaderlessFraction float64
	QuorumlessFraction float64
	Elections          int64
	SnapshotsCreated   int64
}

// MeasureBisection runs one configuration across seeds and reports how much
// progress it made and how available the cluster was.
func MeasureBisection(name string, cfg Config, seeds, ticks int, w Workload) BisectionRow {
	row := BisectionRow{Name: name, Config: cfg, Seeds: seeds}
	cfg.Invariants = true
	cfg.Trace = false
	cfg.Workload = w
	var leaderless, quorumless int64
	for i := 0; i < seeds; i++ {
		c := cfg
		c.Seed = cfg.Seed + int64(i)
		s, err := New(c)
		if err != nil {
			continue
		}
		s.Run(ticks)
		row.Ticks += s.Now()
		row.ClientOps += s.WorkloadProgress().Completed
		row.ClientOpsTotal += s.WorkloadProgress().Total
		row.CommittedDepth += int64(s.Stats().MaxCommit)
		leaderless += s.Stats().LeaderlessTicks
		quorumless += s.Stats().QuorumlessTicks
		row.Elections += s.Stats().Elections
		row.SnapshotsCreated += s.Stats().SnapshotsCreated
	}
	if row.Ticks > 0 {
		row.LeaderlessFraction = float64(leaderless) / float64(row.Ticks)
		row.QuorumlessFraction = float64(quorumless) / float64(row.Ticks)
	}
	return row
}
