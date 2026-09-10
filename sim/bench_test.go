package sim

import (
	"fmt"
	"testing"

	"github.com/eigendash/graft/lcheck"
)

// benchWorkload is the fixed client shape used by the benchmarks. Twelve
// clients give the leader enough in-flight work to pipeline, and the keyspace
// is small enough that operations contend for the same keys.
func benchWorkload(clients int, ops int) Workload {
	return Workload{Clients: clients, OpsPerClient: ops, Keys: 8, Values: 6, ClientTimeout: 4000}
}

// benchConfig returns a fault-free configuration whose election timeout scales
// with the hop latency. Holding the timeout fixed while the latency grows would
// break Raft §5.6 and measure a cluster that cannot keep a leader, which is a
// different experiment than the one this benchmark claims to run.
func benchConfig(nodes, hop int) Config {
	rtt := 2 * hop
	return Config{
		Nodes:            nodes,
		ElectionTick:     10 * rtt,
		ElectionJitter:   10 * rtt,
		HeartbeatTick:    hop,
		Latency:          Latency{Min: hop, Max: hop},
		MaxEntriesPerMsg: 64,
		MaxInflight:      64,
	}
}

// BenchmarkTick measures the host cost of one simulated tick. The cluster is
// fault-free, so this is the cost of the protocol itself: heartbeats,
// replication bookkeeping and the per-tick invariant checks.
func BenchmarkTick(b *testing.B) {
	for _, nodes := range []int{3, 5, 7} {
		b.Run(fmt.Sprintf("nodes=%d/idle", nodes), func(b *testing.B) {
			s, err := New(benchConfig(nodes, 1))
			if err != nil {
				b.Fatal(err)
			}
			s.Run(200) // elect and settle before measuring
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Step()
			}
		})
		b.Run(fmt.Sprintf("nodes=%d/with-log", nodes), func(b *testing.B) {
			cfg := benchConfig(nodes, 1)
			cfg.SnapshotThreshold = 100
			s, err := New(cfg)
			if err != nil {
				b.Fatal(err)
			}
			// Give it a log to replicate and keep alive, so the tick cost
			// includes real AppendEntries traffic rather than only heartbeats.
			for i := 0; i < 5000; i++ {
				for _, id := range s.Leaders() {
					if _, _, err := s.Server(id).Node().Propose(proposal(i)); err != nil {
						b.Fatal(err)
					}
				}
				s.Step()
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Step()
			}
		})
	}
}

// BenchmarkKVOperation measures the host cost of one simulated client
// operation, and reports the throughput the configuration implies in virtual
// time. The two numbers are different quantities and both matter: ns/op is the
// CPU cost of producing the result, ops/virtual-s is the latency the modelled
// cluster would have.
func BenchmarkKVOperation(b *testing.B) {
	for _, nodes := range []int{3, 5, 7} {
		for _, hop := range []int{1, 10, 50} {
			b.Run(fmt.Sprintf("nodes=%d/hop=%dms", nodes, hop), func(b *testing.B) {
				const clients = 12
				per := (b.N + clients - 1) / clients
				if per < 1 {
					per = 1
				}
				b.StopTimer()
				cfg := benchConfig(nodes, hop)
				cfg.Workload = benchWorkload(clients, per)
				b.StartTimer()
				res := MeasureKV(cfg, cfg.Workload, 4_000_000_000)
				b.StopTimer()
				if !res.Completed {
					b.Fatalf("the workload did not finish: %d of %d operations in %d ticks",
						res.Operations, clients*per, res.VirtualTicks)
				}
				b.ReportMetric(res.OpsPerVirtualSecond(), "ops/virtual-s")
				b.ReportMetric(float64(res.Latency.Median), "median-latency-ms")
				b.ReportMetric(float64(res.Latency.P99), "p99-latency-ms")
			})
		}
	}
}

// BenchmarkSafetySweep measures the cost of one swept seed, including the
// per-tick invariant checks that make the sweep meaningful. Turning the checks
// off would make this faster and worthless.
func BenchmarkSafetySweep(b *testing.B) {
	for i := 0; i < b.N; i++ {
		cfg := cheapFaults(int64(i), 5)
		cfg.Invariants = true
		_ = RunOnce(cfg, 1200)
	}
}

// BenchmarkLinearizabilityCheck measures the checker on histories built the
// same way the simulator builds them: a real sequential execution with random
// overlapping intervals.
func BenchmarkLinearizabilityCheck(b *testing.B) {
	for _, n := range []int{8, 16, 24} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			hist := generateBenchHistory(n, 4, 3)
			opts := lcheck.Options{MaxSteps: 2_000_000, Memoize: true}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := lcheck.Check(hist, lcheck.NewKVModel(), opts)
				if err != nil {
					b.Fatal(err)
				}
				if !res.Linearizable {
					b.Fatalf("the generated history is not linearizable: %s", res.Explanation)
				}
			}
		})
	}
}

// generateBenchHistory mirrors lcheck's own generator: execute operations
// sequentially, then choose intervals that keep that order real-time consistent
// but let neighbouring clients overlap.
func generateBenchHistory(n, clients, keys int) lcheck.History {
	r := newBenchRand(20241007)
	data := map[string]string{}
	var h lcheck.History
	last := map[lcheck.ClientID]int64{}
	prevInvoke := int64(0)
	for i := 0; i < n; i++ {
		c := lcheck.ClientID(r.intn(clients))
		k := string(rune('a' + r.intn(keys)))
		op := lcheck.Operation{Client: c}
		switch r.intn(10) {
		case 0, 1, 2, 3:
			v := string(rune('0' + r.intn(6)))
			data[k] = v
			op.Input = lcheck.Input{Kind: lcheck.KindPut, Key: k, Val: v}
			op.Output = lcheck.Output{}
		case 4, 5, 6:
			v := string(rune('0' + r.intn(6)))
			data[k] += v
			op.Input = lcheck.Input{Kind: lcheck.KindAppend, Key: k, Val: v}
			op.Output = lcheck.Output{}
		default:
			v, ok := data[k]
			op.Input = lcheck.Input{Kind: lcheck.KindGet, Key: k}
			op.Output = lcheck.Output{Val: v, Found: ok}
		}
		start := prevInvoke
		if l := last[c]; l > start {
			start = l
		}
		if i > 0 && r.intn(2) == 0 {
			prev := h[i-1]
			if prev.Client != c && prev.Complete > prev.Invoke {
				if o := prev.Invoke + r.int63n(prev.Complete-prev.Invoke); o > start {
					start = o
				}
			}
		}
		op.Invoke, op.Complete = start, start+1+r.int63n(8)
		last[c] = op.Complete
		prevInvoke = start
		h = append(h, op)
	}
	return h
}

// benchRand is a tiny deterministic generator, so the benchmark history does
// not depend on the standard library's generator version.
type benchRand struct{ state uint64 }

func newBenchRand(seed uint64) *benchRand {
	return &benchRand{state: seed*6364136223846793005 + 1442695040888963407}
}

func (r *benchRand) next() uint64 {
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	return r.state
}

func (r *benchRand) intn(n int) int       { return int(r.next() % uint64(n)) }
func (r *benchRand) int63n(n int64) int64 { return int64(r.next() % uint64(n)) }

func proposal(i int) []byte {
	return []byte(fmt.Sprintf("cmd-%08d", i))
}
