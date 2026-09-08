// Package sim is a deterministic, discrete-event simulator for the graft Raft
// implementation.
//
// The whole package exists because raft.Node has no clock, no sockets and no
// disk. Everything it would have reached for is supplied here instead: a
// virtual clock that advances one tick at a time, a message router that can
// delay, drop, duplicate and reorder, a partition primitive, and storage that
// outlives a simulated crash while the node itself does not.
//
// Determinism is the point. Every random decision comes from a single
// rand.Rand seeded from Config.Seed, every collection is iterated in a fixed
// order, and no goroutine is ever started. Two runs with the same seed produce
// byte-identical event logs, so a failure found by a model check over 500 seeds
// can be investigated by replaying one integer.
package sim

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/eigendash/graft/kv"
	"github.com/eigendash/graft/raft"
)

// Latency is the one-way delay distribution for a message, in ticks, drawn
// uniformly from [Min, Max]. Values below 1 are clamped, because a message can
// never be delivered in the tick it was sent: the delivery phase runs before
// the tick that would produce it.
type Latency struct {
	Min int
	Max int
}

func (l Latency) draw(rng *rand.Rand) int {
	if l.Max <= l.Min {
		if l.Min < 1 {
			return 1
		}
		return l.Min
	}
	return l.Min + rng.Intn(l.Max-l.Min+1)
}

// Config describes a simulated cluster and its fault schedule.
type Config struct {
	// Seed drives every random decision. Two Configs that differ only in
	// Nodes are not comparable, but two runs with identical Configs are.
	Seed  int64
	Nodes int

	ElectionTick     int
	ElectionJitter   int
	HeartbeatTick    int
	MaxEntriesPerMsg int
	MaxInflight      int
	CheckQuorum      bool

	Latency Latency

	// DropRate, DuplicateRate and ReorderRate are per-message probabilities.
	// Reorder is implemented as an extra delay rather than as a queue
	// permutation, because that is what a real network does, and because it
	// makes reordering interact correctly with the drop model.
	DropRate      float64
	DuplicateRate float64
	ReorderRate   float64
	ReorderWindow int

	// SnapshotThreshold is the number of applied entries past the last
	// snapshot that triggers compaction. Zero disables snapshots.
	SnapshotThreshold uint64

	// CrashRate is the per-node, per-tick probability of a crash, and
	// RestartDelay is how many ticks a crashed node stays down.
	CrashRate    float64
	RestartDelay int

	// PartitionRate is the per-tick probability of installing a fresh random
	// partition, HealRate the probability of removing it. Both can be zero.
	PartitionRate   float64
	PartitionGroups int
	HealRate        float64

	// Trace records a human-readable event log. It costs an allocation per
	// message, so it is off by default and on in tests that assert on it.
	Trace bool
	// MaxEvents caps the trace. Zero means unlimited.
	MaxEvents int

	// Invariants enables the safety checks run after every tick. Turning them
	// off is only reasonable for benchmarks, where the log is long enough that
	// scanning it would dominate.
	Invariants bool

	// Workload attaches client traffic. A zero value means the run has no
	// clients, which is what a pure consensus sweep wants.
	Workload Workload
}

func (c *Config) withDefaults() error {
	if c.Nodes < 1 {
		return fmt.Errorf("sim: Nodes must be at least 1")
	}
	if c.ElectionTick <= 0 {
		c.ElectionTick = 10
	}
	if c.ElectionJitter <= 0 {
		c.ElectionJitter = 10
	}
	if c.HeartbeatTick <= 0 {
		c.HeartbeatTick = 1
	}
	if c.MaxEntriesPerMsg <= 0 {
		c.MaxEntriesPerMsg = 64
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 64
	}
	if c.Latency.Min < 1 && c.Latency.Max < 1 {
		c.Latency = Latency{Min: 1, Max: 3}
	}
	if c.RestartDelay <= 0 {
		c.RestartDelay = 20
	}
	if c.PartitionGroups <= 0 {
		c.PartitionGroups = 2
	}
	if c.ReorderWindow <= 0 {
		c.ReorderWindow = 4
	}
	if c.DropRate < 0 || c.DropRate > 1 || c.DuplicateRate < 0 || c.DuplicateRate > 1 ||
		c.ReorderRate < 0 || c.ReorderRate > 1 {
		return fmt.Errorf("sim: rates must be in [0,1]")
	}
	return nil
}

// WorstCaseRoundTrip is the longest a message exchange can take under this
// configuration: the base one-way delay plus the largest reorder penalty, out
// and back.
func (c Config) WorstCaseRoundTrip() int {
	oneWay := c.Latency.Max
	if oneWay < c.Latency.Min {
		oneWay = c.Latency.Min
	}
	if c.ReorderRate > 0 {
		window := c.ReorderWindow
		if window < 1 {
			window = 1
		}
		oneWay += 1 + (window - 1)
	}
	return 2 * oneWay
}

// TimeoutToRoundTripRatio reports the election timeout floor divided by the
// worst-case round trip.
//
// It is a measurement, not a verdict. Raft §5.6 requires broadcastTime <<
// electionTimeout, and a ratio well below one means a follower can time out
// while a heartbeat is genuinely in flight. That is not automatically fatal:
// a leader sends several heartbeats per timeout, so a follower only needs one
// of them to survive the network. It does mean the configuration is spending
// its margin, and an empirical measurement of progress is the only trustworthy
// way to judge it. See sim.MeasureThroughput and the sweeps.
func (c Config) TimeoutToRoundTripRatio() float64 {
	rtt := c.WorstCaseRoundTrip()
	if rtt == 0 {
		return 0
	}
	return float64(c.ElectionTick) / float64(rtt)
}

// Stats counts what the run did. Every field is a measurement, not an estimate.
type Stats struct {
	Ticks              int64
	MessagesSent       int64
	MessagesDropped    int64
	MessagesDuplicated int64
	MessagesReordered  int64
	MessagesDelivered  int64
	MessagesToDead     int64
	Elections          int64
	Crashes            int64
	Restarts           int64
	SnapshotsCreated   int64
	SnapshotsInstalled int64
	EntriesCommitted   int64
	StorageWrites      int64
	Partitions         int64
	Heals              int64
	// MaxCommit is the highest commit index any node reached. It is the
	// honest measure of progress: a sum of per-node apply counts is inflated
	// by the replication factor and by catch-up, whereas the committed log has
	// a length.
	MaxCommit raft.Index
}

// envelope is one in-flight message.
type envelope struct {
	deliver int64
	seq     uint64
	from    raft.NodeID
	to      raft.NodeID
	msg     raft.Message
}

// wire is a binary min-heap ordered by (deliver, seq). The sequence number is
// unique, so the ordering is total and the simulation has no ties to break.
//
// It is hand-written rather than built on the standard library's container/heap
// because that interface boxes every element into an any, which costs one
// allocation per message on the hottest path in the simulator.
type wire []envelope

// before reports whether e must be delivered earlier than o.
func (e envelope) before(o envelope) bool {
	if e.deliver != o.deliver {
		return e.deliver < o.deliver
	}
	return e.seq < o.seq
}

func (w *wire) push(e envelope) {
	*w = append(*w, e)
	h := *w
	i := len(h) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if !h[i].before(h[parent]) {
			return
		}
		h[i], h[parent] = h[parent], h[i]
		i = parent
	}
}

func (w *wire) pop() envelope {
	h := *w
	top := h[0]
	n := len(h) - 1
	h[0] = h[n]
	h = h[:n]
	*w = h
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		small := i
		if l < n && h[l].before(h[small]) {
			small = l
		}
		if r < n && h[r].before(h[small]) {
			small = r
		}
		if small == i {
			return top
		}
		h[i], h[small] = h[small], h[i]
		i = small
	}
}

// init restores the heap property after the backing slice has been rebuilt by
// hand, which is what dropping queued messages does.
func (w *wire) init() {
	h := *w
	for i := len(h)/2 - 1; i >= 0; i-- {
		for j := i; ; {
			l, r := 2*j+1, 2*j+2
			small := j
			if l < len(h) && h[l].before(h[small]) {
				small = l
			}
			if r < len(h) && h[r].before(h[small]) {
				small = r
			}
			if small == j {
				break
			}
			h[j], h[small] = h[small], h[j]
			j = small
		}
	}
}

// nodeState is one simulated process: durable storage that survives a crash,
// and the volatile parts that do not.
type nodeState struct {
	id      raft.NodeID
	storage *raft.MemoryStorage
	rng     *rand.Rand

	server *kv.Server
	sm     *kv.StateMachine

	down      bool
	restartAt int64

	// Log Matching is checked incrementally: fpTo is the highest index already
	// fingerprinted and fpTerm its term, which is enough to notice that the log
	// underneath the mark was rewritten.
	fpTo   raft.Index
	fpTerm raft.Term

	// appliedTo is the highest index this process has been observed to apply,
	// reset to the snapshot index on restart. Entries must arrive strictly
	// in sequence after it.
	appliedTo raft.Index

	// lastState and lastSaves let the simulator count leadership transitions
	// and durable writes without the consensus core having to report them.
	lastState raft.StateType
	lastSaves int
}

// Sim is the simulator.
type Sim struct {
	cfg  Config
	rng  *rand.Rand
	tick int64
	seq  uint64

	nodes []*nodeState
	byID  map[raft.NodeID]*nodeState
	wire  wire

	groups [][]raft.NodeID

	events     []string
	digest     uint64
	violations []string
	stopped    bool

	stats Stats

	// Safety observations.
	committedTerm map[raft.Index]raft.Term
	leaderOfTerm  map[raft.Term]raft.NodeID
	leaderTerm    map[raft.NodeID]raft.Term
	leaderCommit  map[raft.NodeID]raft.Index
	logEntries    map[logKey]uint64

	wl *workloadState
}

type logKey struct {
	index raft.Index
	term  raft.Term
}

// New builds a cluster of Config.Nodes processes, all up and fully connected.
func New(cfg Config) (*Sim, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}
	s := &Sim{
		cfg:           cfg,
		rng:           rand.New(rand.NewSource(cfg.Seed)),
		byID:          map[raft.NodeID]*nodeState{},
		committedTerm: map[raft.Index]raft.Term{},
		leaderOfTerm:  map[raft.Term]raft.NodeID{},
		leaderTerm:    map[raft.NodeID]raft.Term{},
		leaderCommit:  map[raft.NodeID]raft.Index{},
		logEntries:    map[logKey]uint64{},
	}
	// Every process is registered before any of them starts, because a node's
	// initial membership is the whole cluster. Starting them one at a time
	// would hand the first node a quorum of one and elect it before the rest
	// existed.
	for i := 1; i <= cfg.Nodes; i++ {
		id := raft.NodeID(i)
		n := &nodeState{
			id:      id,
			storage: raft.NewMemoryStorage(),
			// One generator per node, derived from the cluster seed, so that
			// the election timeouts of one node do not depend on how many
			// messages another node happened to drop.
			rng: rand.New(rand.NewSource(cfg.Seed*1_000_003 + int64(id))),
		}
		s.nodes = append(s.nodes, n)
		s.byID[id] = n
	}
	for _, n := range s.nodes {
		s.start(n)
	}
	if cfg.Workload.Clients > 0 {
		s.Workload(cfg.Workload)
	}
	return s, nil
}

func (s *Sim) voters() []raft.NodeID {
	out := make([]raft.NodeID, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n.id)
	}
	return out
}

func (s *Sim) start(n *nodeState) {
	node, err := raft.NewNode(raft.Config{
		ID:               n.id,
		Voters:           s.voters(),
		ElectionTick:     s.cfg.ElectionTick,
		ElectionJitter:   s.cfg.ElectionJitter,
		HeartbeatTick:    s.cfg.HeartbeatTick,
		MaxEntriesPerMsg: s.cfg.MaxEntriesPerMsg,
		MaxInflight:      s.cfg.MaxInflight,
		CheckQuorum:      s.cfg.CheckQuorum,
		Rand:             n.rng,
		Storage:          n.storage,
		Bug: func(msg string) {
			s.violate("node %d reported an internal invariant failure: %s", n.id, msg)
		},
	})
	if err != nil {
		s.violate("node %d failed to start: %v", n.id, err)
		return
	}
	sm := kv.NewStateMachine()
	if ps, err := n.storage.Load(); err == nil && ps.Snapshot != nil {
		if err := sm.Restore(ps.Snapshot.Data); err != nil {
			s.violate("node %d could not restore its snapshot: %v", n.id, err)
		}
		n.appliedTo = ps.Snapshot.Index
	} else {
		n.appliedTo = 0
	}
	n.server = kv.NewServer(node, sm, s.cfg.SnapshotThreshold)
	n.sm = sm
	n.down = false
	n.restartAt = 0
}

// --- inspection --------------------------------------------------------------

func (s *Sim) Now() int64           { return s.tick }
func (s *Sim) Stats() Stats         { return s.stats }
func (s *Sim) Config() Config       { return s.cfg }
func (s *Sim) Violations() []string { return append([]string(nil), s.violations...) }
func (s *Sim) Events() []string     { return append([]string(nil), s.events...) }
func (s *Sim) EventDigest() uint64  { return s.digest }

func (s *Sim) node(id raft.NodeID) *nodeState { return s.byID[id] }

// Node returns the consensus node, or nil if the process is down.
func (s *Sim) Node(id raft.NodeID) *raft.Node {
	n := s.byID[id]
	if n == nil || n.down {
		return nil
	}
	return n.server.Node()
}

// Server returns the server, or nil if the process is down.
func (s *Sim) Server(id raft.NodeID) *kv.Server {
	n := s.byID[id]
	if n == nil || n.down {
		return nil
	}
	return n.server
}

// StateMachine returns the live state machine, or nil if the process is down.
func (s *Sim) StateMachine(id raft.NodeID) *kv.StateMachine {
	n := s.byID[id]
	if n == nil || n.down {
		return nil
	}
	return n.sm
}

// Storage returns the durable storage, which outlives a crash.
func (s *Sim) Storage(id raft.NodeID) *raft.MemoryStorage {
	n := s.byID[id]
	if n == nil {
		return nil
	}
	return n.storage
}

func (s *Sim) IsDown(id raft.NodeID) bool {
	n := s.byID[id]
	return n == nil || n.down
}

// IDs returns the node identifiers in ascending order.
func (s *Sim) IDs() []raft.NodeID {
	out := make([]raft.NodeID, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n.id)
	}
	return out
}

// Leaders returns the live nodes that currently believe they are leader.
func (s *Sim) Leaders() []raft.NodeID {
	var out []raft.NodeID
	for _, n := range s.nodes {
		if !n.down && n.server.Node().State() == raft.StateLeader {
			out = append(out, n.id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// --- fault injection ---------------------------------------------------------

// Crash stops a process. Its MemoryStorage survives, which is precisely the
// crash model the real system has: the state machine and the raft node are
// volatile and must be rebuilt from what was persisted.
func (s *Sim) Crash(id raft.NodeID) {
	n := s.byID[id]
	if n == nil || n.down {
		return
	}
	n.down = true
	n.server = nil
	n.sm = nil
	n.restartAt = s.tick + int64(s.cfg.RestartDelay)
	n.fpTo, n.fpTerm = 0, 0
	s.stats.Crashes++
	s.event("t=%d crash node=%d", s.tick, id)
	s.dropQueuedTo(id)
}

// Restart brings a crashed process back from its persisted state alone.
func (s *Sim) Restart(id raft.NodeID) {
	n := s.byID[id]
	if n == nil || !n.down {
		return
	}
	s.stats.Restarts++
	s.event("t=%d restart node=%d", s.tick, id)
	s.start(n)
}

func (s *Sim) dropQueuedTo(id raft.NodeID) {
	kept := s.wire[:0]
	for _, e := range s.wire {
		if e.to == id {
			s.stats.MessagesToDead++
			continue
		}
		kept = append(kept, e)
	}
	s.wire = kept
	s.wire.init()
}

// Partition installs a network split. Nodes named in a group can talk only to
// other members of the same group; a node named in no group can talk to
// everyone, and exists so that a test can isolate a subset without having to
// enumerate the rest.
func (s *Sim) Partition(groups ...[]raft.NodeID) {
	s.groups = nil
	for _, g := range groups {
		cp := append([]raft.NodeID(nil), g...)
		sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
		s.groups = append(s.groups, cp)
	}
	s.stats.Partitions++
	s.event("t=%d partition %v", s.tick, s.groups)
}

// Heal removes every partition.
func (s *Sim) Heal() {
	if len(s.groups) == 0 {
		return
	}
	s.groups = nil
	s.stats.Heals++
	s.event("t=%d heal", s.tick)
}

// Isolate cuts one node off from every other node.
func (s *Sim) Isolate(id raft.NodeID) {
	var rest []raft.NodeID
	for _, n := range s.nodes {
		if n.id != id {
			rest = append(rest, n.id)
		}
	}
	s.Partition([]raft.NodeID{id}, rest)
}

func (s *Sim) groupOf(id raft.NodeID) int {
	for i, g := range s.groups {
		for _, x := range g {
			if x == id {
				return i
			}
		}
	}
	return -1
}

// reachable reports whether a message can leave from and arrive at the pair.
// A node that appears in no group is considered connected to everyone, so
// partitioning a subset of the cluster does not accidentally isolate the rest.
func (s *Sim) reachable(from, to raft.NodeID) bool {
	if len(s.groups) == 0 {
		return true
	}
	gf, gt := s.groupOf(from), s.groupOf(to)
	if gf < 0 || gt < 0 {
		return true
	}
	return gf == gt
}

// RandomPartition installs a random split, drawn from the simulator's own
// generator so that the whole fault schedule is reproducible from the seed.
//
// Most splits cut off a minority and leave the rest able to make progress,
// because that is the fault that tests recovery. One split in five is
// unconstrained and can leave no side with a quorum, because that is the fault
// that tests that nothing is committed when nothing can be.
func (s *Sim) RandomPartition() {
	live := make([]raft.NodeID, 0, len(s.nodes))
	for _, n := range s.nodes {
		if !n.down {
			live = append(live, n.id)
		}
	}
	if len(live) < 2 {
		return
	}
	perm := s.rng.Perm(len(live))
	pick := func(idx []int) []raft.NodeID {
		out := make([]raft.NodeID, 0, len(idx))
		for _, i := range idx {
			out = append(out, live[i])
		}
		return out
	}
	if s.rng.Intn(10) < 8 {
		maxMinority := (len(live) - 1) / 2
		if maxMinority < 1 {
			maxMinority = 1
		}
		k := 1 + s.rng.Intn(maxMinority)
		s.Partition(pick(perm[:k]), pick(perm[k:]))
		return
	}
	k := s.cfg.PartitionGroups
	if k > len(live) {
		k = len(live)
	}
	groups := make([][]raft.NodeID, k)
	for i, p := range perm {
		groups[i%k] = append(groups[i%k], live[p])
	}
	s.Partition(groups...)
}

// --- the event loop ----------------------------------------------------------

// Step advances the simulation by exactly one tick.
func (s *Sim) Step() {
	if s.stopped {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.violate("panic during tick %d: %v", s.tick, r)
			s.stopped = true
		}
	}()
	s.tick++
	s.stats.Ticks = s.tick
	s.event("t=%d begin", s.tick)

	s.deliverDue()
	s.tickNodes()
	s.drain()
	s.runWorkload()
	s.drain()
	s.injectFaults()
	if s.cfg.Invariants {
		s.checkInvariants()
	}
}

// Run advances the simulation by n ticks.
func (s *Sim) Run(ticks int) {
	for i := 0; i < ticks && !s.stopped; i++ {
		s.Step()
	}
}

func (s *Sim) tickNodes() {
	for _, n := range s.nodes {
		if n.down {
			continue
		}
		n.server.Node().Tick()
	}
}

func (s *Sim) deliverDue() {
	for len(s.wire) > 0 {
		e := s.wire[0]
		if e.deliver > s.tick {
			return
		}
		e = s.wire.pop()
		n := s.byID[e.to]
		if n == nil || n.down {
			s.stats.MessagesToDead++
			continue
		}
		s.stats.MessagesDelivered++
		s.event("t=%d %s %d->%d", s.tick, e.msg.Type, e.from, e.to)
		n.server.Node().Step(e.msg)
	}
}

// drain collects everything each node wants to do, checks it, and puts it on
// the wire. Messages produced during a Step are not routed until the next
// drain, which is what gives the simulator a well-defined per-tick ordering.
func (s *Sim) drain() {
	for _, n := range s.nodes {
		if n.down {
			continue
		}
		res, err := n.server.PollAll()
		if err != nil {
			s.violate("node %d: %v", n.id, err)
			continue
		}
		s.observe(n, res)
		if st := n.server.Node().State(); st == raft.StateLeader && n.lastState != raft.StateLeader {
			s.stats.Elections++
		}
		n.lastState = n.server.Node().State()
		if saves := n.storage.Saves; saves > n.lastSaves {
			s.stats.StorageWrites += int64(saves - n.lastSaves)
			n.lastSaves = saves
		}
		for _, m := range res.Messages {
			s.route(m)
		}
	}
}

func (s *Sim) route(m raft.Message) {
	if !s.reachable(m.From, m.To) {
		s.stats.MessagesDropped++
		s.event("t=%d partitioned %s %d->%d", s.tick, m.Type, m.From, m.To)
		return
	}
	if s.cfg.DropRate > 0 && s.rng.Float64() < s.cfg.DropRate {
		s.stats.MessagesDropped++
		s.event("t=%d dropped %s %d->%d", s.tick, m.Type, m.From, m.To)
		return
	}
	copies := 1
	if s.cfg.DuplicateRate > 0 && s.rng.Float64() < s.cfg.DuplicateRate {
		copies = 2
		s.stats.MessagesDuplicated++
	}
	for i := 0; i < copies; i++ {
		delay := s.cfg.Latency.draw(s.rng)
		if s.cfg.ReorderRate > 0 && s.rng.Float64() < s.cfg.ReorderRate {
			delay += 1 + s.rng.Intn(s.cfg.ReorderWindow)
			s.stats.MessagesReordered++
		}
		s.seq++
		s.stats.MessagesSent++
		s.wire.push(envelope{
			deliver: s.tick + int64(delay),
			seq:     s.seq,
			from:    m.From,
			to:      m.To,
			msg:     m,
		})
	}
}

func (s *Sim) injectFaults() {
	if s.cfg.CrashRate == 0 && s.cfg.PartitionRate == 0 && s.cfg.HealRate == 0 {
		return
	}
	if s.cfg.CrashRate > 0 {
		for _, n := range s.nodes {
			if n.down {
				continue
			}
			if s.rng.Float64() < s.cfg.CrashRate {
				s.Crash(n.id)
			}
		}
	}
	for _, n := range s.nodes {
		if n.down && n.restartAt > 0 && s.tick >= n.restartAt {
			s.Restart(n.id)
		}
	}
	if s.cfg.PartitionRate > 0 && s.rng.Float64() < s.cfg.PartitionRate {
		s.RandomPartition()
	}
	if s.cfg.HealRate > 0 && s.rng.Float64() < s.cfg.HealRate {
		s.Heal()
	}
}

// --- tracing -----------------------------------------------------------------

func (s *Sim) event(format string, args ...any) {
	if !s.cfg.Trace {
		return
	}
	if s.cfg.MaxEvents > 0 && len(s.events) >= s.cfg.MaxEvents {
		return
	}
	line := fmt.Sprintf(format, args...)
	s.events = append(s.events, line)
	for i := 0; i < len(line); i++ {
		s.digest = (s.digest ^ uint64(line[i])) * 1099511628211
	}
	s.digest = (s.digest ^ '\n') * 1099511628211
}

func (s *Sim) violate(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	s.violations = append(s.violations, fmt.Sprintf("tick %d: %s", s.tick, msg))
	s.event("t=%d VIOLATION %s", s.tick, msg)
}
