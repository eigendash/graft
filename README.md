# graft

A Raft implementation in Go whose consensus core contains **no I/O and no
wall-clock reads**, plus a deterministic fault-injection simulator that drives
real verification: a model check over hundreds of seeded fault schedules and a
Wing & Gong linearizability checker over histories recorded while the network is
being partitioned and nodes are crashing.

The simulator is not a demo. It found **two genuine Raft safety violations** in
this codebase. Both are named, mechanised and pinned by regression tests below.

---

## 1. The one design decision everything follows from

`raft.Node` cannot read a clock, open a socket or touch a disk. Every source of
nondeterminism enters through an interface the caller supplies:

| What | How it enters | Why it matters |
|---|---|---|
| Time | `Tick()` | The core counts ticks and has no idea how long one lasts. Election timeouts are counts. There is no `time.Now()` to mock and therefore no monkey-patching. |
| Randomness | `Config.Rand`, a one-method `Source` interface | The whole nondeterminism surface of Raft is "how many ticks until the next election". `math/rand.Rand` satisfies `Source`, so one seeded generator per node makes the entire cluster reproducible. |
| Durability | `Config.Storage` | The core calls it synchronously, in the order Raft requires (term and vote before a vote reply, entries before an acknowledgement), and never learns what is behind it. |
| The network | `Step(Message)` inbound, `Ready()` outbound | Messages can be delayed, dropped, duplicated or reordered arbitrarily, because delivery is entirely the caller's decision. |

That constraint is what makes the simulator trustworthy rather than decorative.
A simulator for a normal Raft implementation has to *guess* what the code does
with time, and the guess is where the bugs hide. Here the code literally cannot
do anything else, and `raft/noio_test.go` enforces it at the source level: it
parses every non-test file in the package and fails on an import of `time`,
`os`, `net`, `io`, `bufio`, `sync`, `math/rand` or `crypto/rand`, on any call to
`time.X` or `rand.X`, and on any `go` statement.

The simulator supplies the four interfaces and then owns everything else: a
virtual clock that advances one tick at a time, a message router with
configurable latency, loss, duplication and reordering, partition primitives,
and storage that outlives a simulated crash while the node itself does not.

### Why the simulator can be trusted

- **One seeded generator for the schedule.** Every random decision — election
  jitter, latency, loss, duplication, reordering, partition shape, crash timing,
  client operation choice — is drawn from a single `rand.Rand` seeded from
  `Config.Seed`, and every collection is iterated in a fixed order.
- **No goroutines anywhere.** The whole simulation is one loop, so there is no
  scheduler nondeterminism to explain away.
- **Determinism is asserted, not assumed.** `TestDeterminism` runs an identical
  fault-heavy schedule twice and compares the two event logs line by line, then
  compares their FNV digests, and finally checks that a *different* seed
  produces a different log (otherwise the generator is not reaching the
  schedule at all). `results/graftbench.txt` records the same check from the
  benchmarking tool: 4505 events, digest `0xcb590e69d91a3023` twice.

A failure found at seed 207 of a 250-seed sweep is therefore reproducible by
passing one integer.

---

## 2. Layout

```
raft/   consensus core: no I/O, no clock. Log, election, AppendEntries with
        conflict backtracking, §5.4.2 commit restriction, log compaction via
        snapshot + InstallSnapshot, single-server membership changes.
sim/    deterministic simulator: virtual clock, message router, partitions,
        crash/restart, the safety invariants, and the client workload driver.
kv/     linearizable key-value store on the replicated log, with per-client
        sessions so retries are idempotent. Reads go through the log.
lcheck/ Wing & Gong linearizability checker over a key-value and a register
        sequential specification.
cmd/graftbench/  produces every number in results/.
```

---

## 3. Safety properties, and the two bugs they found

The model check asserts four properties after **every simulated tick**:

1. **Election safety** — at most one leader per term.
2. **Log Matching** — if two logs hold an entry at the same index with the same
   term, the entries are identical and so is every preceding entry. Checked
   incrementally by fingerprinting each new `(index, term)` together with its
   predecessor's term; agreement on every pair implies agreement on every
   prefix.
3. **Only committed entries are applied** — `applied <= committed <= lastIndex`
   on every node, and every applied entry is a committed one.
4. **§5.4.2** — a leader may only advance its commit index onto an entry of its
   own term. Checked from the outside, at the moment the advance happens.

Additional structural checks: committed entries never change term, a node never
applies entries out of sequence, and a log's terms never decrease.

### Bug 1 — a leader's snapshot left a stale suffix in durable storage

Found at seed 35 of the harsh sweep:

```
Log Matching: index 2 term 61 is held with different content by different nodes
```

`Storage` had one operation (`SaveSnapshot`) for two semantically different
things:

- **local compaction** after `CreateSnapshot` — entries above the image are
  still part of this node's log and must be kept;
- **installing a leader's image** — the leader sent it *precisely because* the
  local suffix is wrong, so the whole local log must be discarded.

Sharing one operation meant a leader-supplied `InstallSnapshot` left a stale
uncommitted suffix on disk. The in-memory path was correct, which is why only a
crash exposed it: on restart `NewNode` rebuilt the log as

```
snapshot(index=2, term=9) + entry(index=3, term=2)
```

— a log whose terms *decrease*. Two nodes then held different entries at the
same `(index, term)`.

**Fix.** Split the durability contract into `Storage.SaveSnapshot` (local
compaction, keeps entries above the image) and `Storage.InstallSnapshot`
(leader-supplied, invalidates the entire local log), and add
`Node.checkTermOrder`, which asserts before every append that terms never
decrease.

**Pinned by** `raft.TestInstallSnapshotSupersedesStaleSuffix`. Reverting the fix
and re-running the test produces `a restart resurrected a stale suffix: last
index 4, want 2`; the transcript is in `results/regressions.txt`, regenerated by
`scripts/prove-regressions.sh` / `make prove-regressions`.

### Bug 2 — an in-flight `AppendEntries` could be rewritten after it was sent

Found at seed 207 of the harsh sweep, by the term-order assertion added for
bug 1:

```
log terms would decrease: entry 52 has term 49 after term 50 at index 50
```

`sendAppend` built the outbound `Message.Entries` as a **sub-slice of the
leader's log**. A message sits on the wire for several ticks, and a node that
loses leadership truncates its log and appends replacements *into exactly that
memory*. The in-flight message then read back a newer prefix followed by a stale
suffix — a batch whose terms decrease. The follower correctly refused it and the
refusal surfaced as a Log Matching failure.

A real transport serialises the message and is immune. Here the message is
passed by reference, so the core must not hand out aliases into mutable memory.

**Fix.** Copy the entries in `sendAppend`; that copy stands in for the
serialisation a real transport performs.

**Pinned by** `raft.TestOutboundAppendEntriesDoesNotAliasTheLog`. Removing the
copy and re-running the test produces `an in-flight AppendEntries changed after
it was sent: entry 2 was {term:1 index:3 data:c} and is now {term:3 index:3
data:X}`; the transcript is in `results/regressions.txt`.

Both bugs were reachable only across a simulated crash or a delayed message,
which is exactly what the simulator exists to produce. Neither is visible to
`go vet`, to the race detector, or to a happy-path test.

### Bug 3 and 4 — in the simulator itself

Two further bugs were found in the harness, and are recorded because a
verification tool that is wrong is worse than none:

- Nodes were registered one at a time during construction, so node 1 started
  with a voter set of `[1]`, won an election against a quorum of one, and
  reported a second leader in term 1 once the rest of the cluster appeared.
  The election-safety check caught it immediately.
- A client's request counter lived in the command struct, which was cleared
  between operations, so a fresh request could collide with a cached answer
  from an earlier one. The linearizability checker rejected the resulting
  history.

### Bug 5 — timing-marginal configuration masquerading as a stall

The original harsh sweep configuration stalled at **477 committed entries** and
17 of 1875 client operations, with zero safety violations. That is not a pass:
a cluster that never commits anything is trivially safe.

Bisecting with **identical fault rates** and changing only the election timeout
(`results/timing-bisection.txt`, regenerated by `cmd/graftbench`: 25 seeds ×
3000 ticks, workload of 3 clients × 25 operations):

| configuration | client ops | committed log depth | leaderless ticks | quorumless ticks | elections |
|---|---|---|---|---|---|
| original `ElectionTick 6, Jitter 6` (worst-case RTT 28) | 17 / 1875 | 48 | 0.940 | 0.009 | 708 |
| same faults, `ElectionTick 12, Jitter 12` | 1669 / 1875 | 2670 | 0.697 | 0.007 | 921 |
| same faults, `ElectionTick 20, Jitter 20` | 1875 / 1875 | 2652 | 0.481 | 0.008 | 592 |
| original `6/j6` with zero loss, duplication and reordering | 820 / 1875 | 1696 | 0.805 | 0.008 | 2020 |
| new harsh `8/j8`, latency ≤ 4, reorder window 4 | 1875 / 1875 | 3096 | 0.326 | 0.002 | 1322 |

"Leaderless" is the share of ticks on which no live node believed it was leader;
"quorumless" the share on which no connected component of live nodes held a
majority. The two separate the diagnoses, and the second column is the decisive
one: the quorumless fraction is **under 1% in every row**, so a majority was
almost always reachable and the cluster simply could not agree. Even with loss,
duplication and reordering switched off entirely, the original timeout leaves
the cluster leaderless for **80.5%** of ticks and completes fewer than half the
operations. The fault rates were never the problem.

The original configuration's worst-case round trip is `2 * (6 + 8) = 28` ticks
against an election timeout of 6..11 ticks, which violates Raft §5.6's
`broadcastTime << electionTimeout`.

I deleted an analytic `TimingFeasible` oracle I had written for this, because it
turned out to flag configurations that empirically work. Liveness claims in this
project are measured, never derived.

**Pinned by** `sim.TestRegressionSeed35LogMatching` (seeds 33..37 of the
original timing-marginal schedule, safety only, and the test asserts the
schedule is *still* timing-marginal so it cannot be silently made easy) and
`sim.TestHarshFaultRatesAreSurvivable` (the same seed and the same fault rates
with a viable timeout, asserting that every client operation completes, that the
committed log keeps growing, and that crashes and `InstallSnapshot` are both
exercised — so a fix that bought safety by stalling cannot pass).

---

## 4. Measured results

Every number in this section is copied from a file in `results/`. The files are
regenerated by `make results`.

- `results/graftbench.txt` and `results/summary.json` — model check,
  linearizability, election timing, throughput, determinism
- `results/bench.txt` — Go benchmarks (`go test -bench`)
- `results/tests.txt` — the full `go test -race -count=1 -v ./...` transcript
- `results/vet.txt` — `go vet ./...` (exit 0)
- `results/timing-bisection.txt` — the Bug 5 bisection, one configuration per row
- `results/regressions.txt` — proof that each regression test fails against the
  code it was written for, produced by reverting each fix in turn

Environment, from the header of `results/graftbench.txt`: `go1.27.1`,
`darwin/arm64`, 12 CPUs, run started `2026-10-07T13:35:52Z`.

### 4.1 Model check — safety

| sweep | nodes | seeds | ticks/seed | violations | committed log depth | client ops | verdict |
|---|---|---|---|---|---|---|---|
| cheap faults | 3 | 200 | 1200 | **0** | 7421 | 4800/4800 | PASS |
| cheap faults | 5 | 200 | 1200 | **0** | 8086 | 4800/4800 | PASS |
| harsh faults | 5 | 250 | 2000 | **0** | 23190 | 15000/15000 | PASS |
| harsh rates, viable timeout | 5 | 25 | 6000 | **0** | 3027 | 1875/1875 | PASS |

Totals across the four sweeps: **675 seeds, 1,130,000 simulated ticks, 41,724
committed log entries, 26,475 completed client operations, 0 safety
violations, 0 incomplete workloads.** Those totals cover exactly the four
`safety_sweep` entries above, which is what `results/summary.json` labels them
as; the linearizability sweep is a different kind of run and is counted
separately in §4.2, so the arithmetic is checkable against the JSON. "Committed log depth" is the length of the
committed log summed over seeds — the measure that cannot be inflated by
replication factor or catch-up.

The fault schedule that produced those numbers, summed:

| | count |
|---|---|
| elections won | 15,971 |
| crashes / restarts | 9,055 / 8,988 |
| partitions installed / healed | 16,661 |
| snapshots created / installed | 8,077 / 9,754 |
| messages sent / delivered | 2,642,255 / 2,598,342 |
| messages dropped / duplicated / reordered | 569,968 / 176,246 / 346,010 |
| durable storage writes | 510,655 |

### 4.2 Linearizability under fault injection

60 seeded runs of a 3-client key-value workload under random partitions, 5%
message loss, duplication, reordering and crashes.

| quantity | value |
|---|---|
| histories checked | 60 |
| operations checked | 1260 |
| longest history | 21 operations |
| candidate operations tried by the checker | 1344 |
| non-linearizable histories | **0** |
| clients left waiting at the end | **0** |
| crashes during the runs | 161 |
| messages dropped / duplicated / reordered | 9752 / 2309 / 6563 |

The checker rejects as well as accepts. `lcheck.TestCheckRejectsNonLinearizableRegister`
and `...KV` feed it hand-written illegal histories — a read that misses a
completed write, a read that ignores two completed writes, a client that reads
its own stale write, an `Append` order contradicted by a later read — and
assert rejection. `TestCheckWitnessIsAValidLinearization` replays the returned
witness through the model and checks it respects real time.
`TestCheckRespectsBudget` asserts that an exhausted search reports *unknown*
rather than "linearizable".

### 4.3 Election timing, in virtual ticks (1 tick = 1 ms)

200 samples per cluster size, no faults. "Cold" is from process start to the
first leader; "failover" isolates the sitting leader and measures until a
replacement appears.

| nodes | cold min | cold median | cold p90 | cold p99 | cold max | failover min | failover median | failover p90 | failover p99 | failover max |
|---|---|---|---|---|---|---|---|---|---|---|
| 3 | 12 | 15 | 19 | 29 | 36 | 12 | 17 | 35 | 64 | 81 |
| 5 | 12 | 15 | 17 | 30 | 36 | 11 | 16 | 29 | 44 | 52 |
| 7 | 12 | 14 | 18 | 27 | 36 | 12 | 15 | 25 | 34 | 68 |

Percentiles use the nearest-rank definition, so every value reported is a value
that was actually observed. The configured election timeout is `10 + rand(10)`
ticks, so a median of 14–15 and a p99 of 27–30 is the expected shape; the
failover tails are longer because the new leader has to gather a quorum from
scratch.

### 4.4 Throughput and latency by cluster size and hop latency

12 concurrent clients, 200 operations each (2400 operations per run), fault-free,
one client operation outstanding at a time. The election that establishes the
first leader is excluded from the measured window. Latencies are what the
clients observed, in simulated milliseconds.

**Uniform hop delay** (`Latency{Min: hop, Max: hop}`):

| nodes | hop | ops | election ticks | measured ticks | ops per virtual second | median latency | p99 latency |
|---|---|---|---|---|---|---|---|
| 3 | 1 ms | 2400 | 23 | 601 | 3993 | 2 | 2 |
| 3 | 10 ms | 2400 | 228 | 4219 | 569 | 20 | 20 |
| 3 | 50 ms | 2400 | 1108 | 20299 | 118 | 100 | 100 |
| 5 | 1 ms | 2400 | 23 | 601 | 3993 | 2 | 2 |
| 5 | 10 ms | 2400 | 228 | 4219 | 569 | 20 | 20 |
| 5 | 50 ms | 2400 | 1108 | 20299 | 118 | 100 | 100 |
| 7 | 1 ms | 2400 | 23 | 601 | 3993 | 2 | 2 |
| 7 | 10 ms | 2400 | 228 | 4219 | 569 | 20 | 20 |
| 7 | 50 ms | 2400 | 1108 | 20299 | 118 | 100 | 100 |

Throughput is **flat in the number of nodes** here, and that is a real property
of the model rather than a measurement artefact: with a uniform delay the commit
path is a single round trip whatever the quorum size, and the pipeline depth is
set by the 12 clients.

**Jittered hop delay** (`Latency{Min: hop, Max: 2*hop}`), which is what makes
the quorum size visible, because the commit waits on the *slowest* of the
followers it needs:

| nodes | hop | ops | ops per virtual second | median latency | p99 latency |
|---|---|---|---|---|---|
| 3 | 1 ms | 2400 | 3283 | 2 | 3 |
| 5 | 1 ms | 2400 | 3121 | 2 | 3 |
| 7 | 1 ms | 2400 | 3046 | 3 | 3 |
| 3 | 10 ms | 2400 | 428 | 25 | 34 |
| 5 | 10 ms | 2400 | 422 | 26 | 33 |
| 7 | 10 ms | 2400 | 419 | 27 | 32 |
| 3 | 50 ms | 2400 | 88 | 128 | 169 |
| 5 | 50 ms | 2400 | 86 | 133 | 166 |
| 7 | 50 ms | 2400 | 86 | 135 | 160 |

Going from 3 to 7 nodes costs 7% of throughput at 1 ms hops and 2% at 50 ms.
The election timeout is scaled with the hop latency in these runs
(`10 * round-trip`), because holding it fixed while latency grows would measure
a §5.6 timing violation rather than the cluster.

### 4.5 Go benchmarks (`results/bench.txt`)

Host costs. Wall clock, one machine, `-benchtime=1s -count=5`; the table quotes
the **fastest of the five runs** for each benchmark, and all five runs are in
`results/bench.txt`. The host was heavily oversubscribed while these ran — the
header of that file records `load averages: 44.96 28.90 17.32` on 12 CPUs — so
every figure here is compressed by contention and the slowest run of the same
benchmark is up to 2.3× the fastest. Read them as orders of magnitude and as
ratios within a single run, not as precise costs.

| benchmark | best ns/op | slowest/fastest | notes |
|---|---|---|---|
| `raft/StepAppendEntries/batch=1` | 98.1 | 1.3× | follower accepting a 1-entry AppendEntries |
| `raft/StepAppendEntries/batch=16` | 168.0 | 1.3× | |
| `raft/StepAppendEntries/batch=64` | 406.3 | 1.5× | 1103 MB/s of payload at its best |
| `raft/ProposeOnLeader` | 4610 | 1.4× | accept, persist and package one proposal, 3 nodes |
| `raft/TruncateAndAppend` | 33.8 | 1.6× | atomic conflict resolution |
| `sim/Tick/nodes=3/idle` | 1034 | 1.2× | one simulated tick, fault-free |
| `sim/Tick/nodes=3/with-log` | 3855 | 1.2× | with a 5000-entry log in flight |
| `sim/Tick/nodes=5/idle` | 2262 | 1.2× | |
| `sim/Tick/nodes=7/idle` | 2815 | 1.0× | |
| `sim/Tick/nodes=7/with-log` | 5422 | 1.2× | |
| `sim/KVOperation/nodes=3/hop=1ms` | 4142 | 1.2× | per simulated client operation, incl. history recording |
| `sim/KVOperation/nodes=7/hop=1ms` | 14856 | 1.5× | |
| `sim/KVOperation/nodes=7/hop=50ms` | 28363 | 1.3× | |
| `sim/SafetySweep` | 3,348,642 | 1.5× | one swept seed, 5 nodes, 1200 ticks, invariants on |
| `lcheck/Check/kv/ops=8` | 4909 | 2.3× | |
| `lcheck/Check/kv/ops=32` | 32215 | 1.1× | |
| `lcheck/CheckRegister/ops=8` | 1910 | 1.6× | |
| `lcheck/LinearizabilityCheck/ops=8` | 6171 | 1.3× | generated history, 4 clients |
| `lcheck/LinearizabilityCheck/ops=24` | 34302 | 1.7× | |

Ratios within the table are more trustworthy than absolute values. One simulated
tick costs 2.7× more at 7 nodes than at 3, which is the expected shape for a
fan-out of heartbeats plus a longer invariant scan. Per simulated client
operation, 7 nodes cost 3.6× what 3 nodes cost at 1 ms hops. The `KVOperation`
benchmarks also report the *virtual-time* throughput the configuration implies,
which is host-independent: 4000 ops per simulated second at 1 ms hops, 571 at
10 ms, 119 at 50 ms, flat in node count under a uniform delay.

Comparing the two tick rows gives the shape of the workload cost: an idle 3-node
tick is 1034 ns, and the same tick under load is about 17 µs — derived from
4142 ns per client operation at the four operations per tick the configuration
sustains — because each proposal is encoded, durably written, packaged per
follower and applied.

### 4.6 Determinism

From `results/graftbench.txt`: 4505 events recorded, digest
`0xcb590e69d91a3023` on both runs, byte-identical.

### 4.7 Reproducing all of it

`results/bench.txt` and `results/tests.txt` each begin with the command that
produced them, in portable form; the environment that matters to a reader (Go
version, GOOS/GOARCH, CPU count, seed counts) is recorded in the header of
`results/graftbench.txt`.

`results/tests.txt` is the transcript of `go test -race -count=1 -v ./...`:
**71 test functions (84 including subtests), 0 skips, 0 failures**, all four
packages `ok`. `results/vet.txt` records `go vet ./... exit=0`.

---

## 5. Limitations

Concrete gaps, not hedging.

**Membership**
- **No joint consensus.** Only single-server add/remove, and the implementation
  does not reject a second change appended before the first commits. It is safe
  only because the caller is expected to make one change at a time; nothing
  enforces that.
- Configuration entries take effect **when appended, not when committed** (the
  Raft dissertation's §4.1 rule). If an appended change is later truncated, the
  membership is not rolled back. There is no test for that path.

**Availability and timing**
- **No PreVote.** A partitioned node's term runs away; when it rejoins it forces
  a re-election and can depose a healthy leader. `sim.TestBacktrackingCatchesUpLaggingFollower`
  has to tolerate a re-election for exactly this reason.
- **No leader leases and no ReadIndex.** Every read goes through the log, so a
  read costs a log entry. `CheckQuorum` is a crude substitute that makes an
  isolated leader step down, and it is off by default.
- The election-timeout/latency relationship is **not enforced by the code**. A
  misconfiguration produces a leaderless cluster that commits nothing rather
  than an error at startup. I tried an analytic check for this and deleted it
  because it was wrong; there is no replacement.

**The checker**
- `lcheck` implements Wing & Gong's backtracking search with memoisation of
  refuted states. It is **exponential in the worst case**, so histories are
  bounded: the sweep's longest is 21 operations, and the throughput workload
  (2400 operations) is far outside what it can check. A budget overrun returns
  `ErrBudgetExhausted` and `Linearizable: false`, which callers must treat as
  *unknown*; nothing prevents a caller from misreading it.
- It does not decompose by key (no P-compositional factoring), does not run in
  parallel, and has no incremental mode.
- It only checks histories whose operations all completed. Operations still
  outstanding when a run ends are excluded, and the sweeps treat any such
  operation as a failure rather than silently dropping it.

**The fault model**
- Message loss, duplication, reordering and delay are modelled; **corruption is
  not**. There is no partial write, no torn write, no `fsync` failure and no bit
  rot. Given that the bug this project actually found was a durability bug, this
  is the most significant gap: `Storage` either records a write or it does not.
- Partitions are checked **at send time**, so a message already in flight can
  cross a partition boundary that is installed afterwards. No test covers that
  ambiguity.
- Faults are independent. There is no correlated failure (a whole rack, a
  shared switch), no clock skew, and no non-uniform timeout distribution beyond
  the uniform jitter.
- `MemoryStorage` is not a disk. The durability *ordering* is asserted by
  construction — the core's calls are synchronous — but nothing verifies it
  against a crash-consistent filesystem.

**Concurrency and integration**
- The node is single-goroutine by contract. `Ready`/`Advance` misuse is a
  documented precondition, not a runtime check, and there is no test that
  concurrent use is detected.
- The log lives entirely in memory; only snapshots bound it. There is no paging
  and no cap on the number of entries kept above the snapshot.
- `sim` depends on `math/rand`'s legacy `NewSource` for reproducibility. It is
  documented as stable, but a future Go change would silently alter every
  schedule in this repository.
- No fuzzing (`go test -fuzz`) is wired up, including for the two binary
  decoders (`kv.DecodeCommand`, `raft.DecodeConfChange`), which are hand-written
  and only covered by hand-written malformed-input tests.
- The simulator's Log Matching check is incremental: a violation that is
  entirely created and erased between two consecutive ticks would be missed.
- Benchmarks are single-machine, single-run. The wall-clock numbers only mean
  anything on this host; the virtual-time numbers reproduce exactly anywhere.

---

## 6. What I'd do next

Ordered by what would remove the most risk per unit of work.

1. **A crash-consistent on-disk `Storage` with a fault injector.** This is the
   direct follow-up to bug 1. Implement the real thing (append-only segments, a
   checksum per record, `fsync` at the documented points), then add a wrapper
   that can tear the last write, drop an `fsync`, or corrupt a record, and
   assert that recovery yields a prefix of the log that was acknowledged. The
   interface split between `SaveSnapshot` and `InstallSnapshot` introduced here
   is exactly the seam that needs this testing.
2. **PreVote and CheckQuorum on by default**, with a test that a node rejoining
   after a long partition does not depose a healthy leader. That turns the
   known liveness weakness into a checked property.
3. **Joint consensus**, with a test that two overlapping configuration changes
   are either serialised or rejected. Store `C_old,new` as its own entry type so
   it survives compaction.
4. **ReadIndex with a lease**, and a linearizability test of reads under
   partition. Reads currently cost a log entry each; this would also make the
   throughput numbers meaningful for read-heavy workloads.
5. **Make `lcheck` scale.** Decompose the history by key (operations on
   disjoint keys commute), check the components in parallel, and add an
   incremental mode that consumes operations as they complete. That would let
   the checker cover the throughput workload instead of a 21-operation slice of
   it.
6. **Correlated fault injection**: rack-shaped partitions, correlated
   slowdowns, and a distinction between "slow" and "down" that a real deployment
   has to cope with.
7. **`go test -fuzz` on both decoders**, and a `Fuzz` target that feeds
   arbitrary bytes through raft message handling and asserts the invariants hold.
8. **Entry checksums** in the log so corruption is detected rather than
   silently applied, with a test that a corrupted entry is refused.
9. **A `-race`-clean concurrent driver**: run the node on its own goroutine with
   a lock-free `Ready` handoff, so the single-loop contract becomes a supported
   usage rather than a documented restriction.

---

## 7. Running it

Go 1.22 or later, no third-party modules. In this workspace the toolchain lives
outside the module, so the `Makefile` exports it explicitly; override `GOROOT`,
`GOCACHE`, `GOMODCACHE` and `GOPATH` to use another.

```sh
make test          # go test ./...
make test-race     # go test -race -count=1 ./...      <- the required check
make vet           # go vet ./...
make model-check   # the deterministic simulator sweeps (cmd/graftbench)
make bench         # go test -run XXX -bench=. -benchtime=1s ./...
make results       # regenerate everything under results/ (incl. the bisection)
make prove-regressions   # show each regression test failing against the old code
make verify        # vet + fmt-check + test-race + results + prove-regressions
```

Without `make`:

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...
go test -run XXX -bench=. -benchtime=1s ./...
go run ./cmd/graftbench -seeds 200 -linseeds 60 -elections 200
```

`cmd/graftbench` accepts `-seeds`, `-linseeds`, `-elections` and `-out`; it
writes `results/graftbench.txt` and `results/summary.json`, and exits non-zero
if any measurement was not a pass.

### A note on what the tests are for

The tests are written to be able to fail. `raft/commitrule_test.go` builds the
exact log configuration of Figure 8 in the Raft paper and asserts that an entry
from an earlier term which sits on a majority is *not* committed, then that a
later leader *does* overwrite it. `raft/regression_test.go` holds the two
deterministic reproductions of the bugs the sweep found, each verified to fail
against the code it was written for. `lcheck`'s tests lead with histories that
are unambiguously illegal, because a checker that cannot reject is worse than no
checker. And the sweeps assert liveness alongside safety, because a cluster that
never commits anything is trivially safe.
