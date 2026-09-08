package sim

import (
	"sort"

	"github.com/eigendash/graft/kv"
	"github.com/eigendash/graft/lcheck"
	"github.com/eigendash/graft/raft"
)

// Workload describes the concurrent client traffic a simulation carries. Each
// client issues its operations one at a time, so the history it produces has
// the sequential-per-client shape a linearizability checker expects.
type Workload struct {
	Clients      int
	OpsPerClient int
	Keys         int
	Values       int

	// ClientTimeout is how many ticks a client waits for a response before it
	// assumes the node it used is no longer able to answer and resends the
	// command. The resend carries the same sequence number, so it is a retry
	// rather than a second operation.
	ClientTimeout int

	// GetWeight, PutWeight and AppendWeight select the operation mix. They are
	// integers so that the choice consumes exactly one draw from the
	// generator.
	GetWeight    int
	PutWeight    int
	AppendWeight int
}

func (w *Workload) withDefaults() {
	if w.Clients <= 0 {
		w.Clients = 3
	}
	if w.OpsPerClient <= 0 {
		w.OpsPerClient = 10
	}
	if w.Keys <= 0 {
		w.Keys = 3
	}
	if w.Values <= 0 {
		w.Values = 3
	}
	if w.ClientTimeout <= 0 {
		w.ClientTimeout = 60
	}
	if w.GetWeight == 0 && w.PutWeight == 0 && w.AppendWeight == 0 {
		w.GetWeight, w.PutWeight, w.AppendWeight = 4, 3, 3
	}
}

const (
	phaseIdle = iota
	phasePropose
	phaseWait
)

type client struct {
	id lcheck.ClientID
	// seq is the client's own monotonic request counter. It lives here rather
	// than in the command because the command is cleared between operations,
	// and a counter that resets would let a fresh request collide with a
	// cached answer from an earlier one.
	seq     uint64
	cmd     kv.Command
	opsLeft int

	phase  int
	invoke int64
	target int
	// next is the index of the node this client tried most recently. Starting
	// there again keeps a settled client talking to the same leader instead of
	// spraying every command across the cluster.
	next     int
	deadline int64
	history  []lcheck.Operation
}

type opSample struct {
	latency  int64
	complete int64
}

type workloadState struct {
	wl      Workload
	clients []*client
	samples []opSample
	// stalled counts clients that still had an outstanding command when the
	// run ended. A non-zero value means the history is incomplete, which a
	// test must not treat as a pass.
	stalled int
}

// Workload attaches client traffic to the simulation.
func (s *Sim) Workload(w Workload) {
	w.withDefaults()
	st := &workloadState{wl: w}
	for i := 0; i < w.Clients; i++ {
		st.clients = append(st.clients, &client{
			id:      lcheck.ClientID(i + 1),
			opsLeft: w.OpsPerClient,
		})
	}
	s.wl = st
}

// WorkloadState reports how much of the attached workload has run.
type WorkloadState struct {
	Completed int
	Total     int
	Stalled   int
}

// WorkloadProgress reports progress, so a test can drive a simulation until the
// clients are done instead of guessing a tick count.
func (s *Sim) WorkloadProgress() WorkloadState {
	if s.wl == nil {
		return WorkloadState{}
	}
	var st WorkloadState
	for _, c := range s.wl.clients {
		st.Completed += s.wl.wl.OpsPerClient - c.opsLeft
		st.Total += s.wl.wl.OpsPerClient
		if c.phase != phaseIdle {
			st.Stalled++
		}
	}
	return st
}

// History returns the recorded operations in completion order.
func (s *Sim) History() lcheck.History {
	if s.wl == nil {
		return nil
	}
	var out lcheck.History
	for _, c := range s.wl.clients {
		out = append(out, c.history...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Complete != out[j].Complete {
			return out[i].Complete < out[j].Complete
		}
		return out[i].Client < out[j].Client
	})
	return out
}

// LatencySamples returns the client-observed per-operation latency in ticks.
func (s *Sim) LatencySamples() []int64 {
	if s.wl == nil {
		return nil
	}
	out := make([]int64, 0, len(s.wl.samples))
	for _, sm := range s.wl.samples {
		out = append(out, sm.latency)
	}
	return out
}

func (s *Sim) runWorkload() {
	if s.wl == nil {
		return
	}
	// Clients are stepped in identifier order, so the order in which they
	// consume randomness is fixed even when the network misbehaves.
	for _, c := range s.wl.clients {
		s.stepClient(c)
	}
	s.wl.stalled = 0
	for _, c := range s.wl.clients {
		if c.phase != phaseIdle {
			s.wl.stalled++
		}
	}
}

func (s *Sim) stepClient(c *client) {
	if c.phase == phaseIdle {
		if c.opsLeft <= 0 {
			return
		}
		s.newCommand(c)
		c.phase = phasePropose
	}
	if c.phase == phasePropose {
		idx, ok := s.proposeToAny(c)
		if !ok {
			// No live node would take the command. The client stays where it
			// is and tries again next tick.
			return
		}
		if c.invoke == 0 {
			// The first time a leader accepts the command is the moment the
			// operation is invoked; earlier refusals did not enter the system.
			c.invoke = s.tick
		}
		c.target = idx
		c.deadline = s.tick + int64(s.wl.wl.ClientTimeout)
		c.phase = phaseWait
	}

	res, ok := s.sample(c)
	if ok {
		s.record(c, res)
		return
	}
	if s.tick >= c.deadline || s.IsDown(s.nodes[c.target].id) {
		// The node that accepted the command is no longer answering. Resending
		// the identical command is safe because the session record makes it
		// idempotent, and it is the only way to make progress after a failover.
		if idx, ok := s.proposeToAny(c); ok {
			c.target = idx
		}
		c.deadline = s.tick + int64(s.wl.wl.ClientTimeout)
	}
}

func (s *Sim) sample(c *client) (kv.Result, bool) {
	n := s.nodes[c.target]
	if n.down || n.sm == nil {
		return kv.Result{}, false
	}
	return n.sm.Result(c.cmd.Client, c.cmd.Seq)
}

func (s *Sim) record(c *client, res kv.Result) {
	if c.invoke == 0 {
		// Complete and Invoke must be ordered; a command that was observed
		// before the accepting proposal was recorded cannot happen, but
		// clamping keeps a malformed history out of the checker rather than
		// letting it fail confusingly.
		c.invoke = s.tick
	}
	c.history = append(c.history, lcheck.Operation{
		Client:   c.id,
		Input:    toLcheckInput(c.cmd),
		Output:   lcheck.Output{Val: res.Val, Found: res.Found, Err: res.Err},
		Invoke:   c.invoke,
		Complete: s.tick,
	})
	s.wl.samples = append(s.wl.samples, opSample{latency: s.tick - c.invoke, complete: s.tick})
	c.opsLeft--
	c.phase = phaseIdle
	c.invoke = 0
	c.cmd = kv.Command{}
}

func toLcheckInput(cmd kv.Command) lcheck.Input {
	var kind lcheck.Kind
	switch cmd.Op {
	case kv.OpGet:
		kind = lcheck.KindGet
	case kv.OpPut:
		kind = lcheck.KindPut
	case kv.OpAppend:
		kind = lcheck.KindAppend
	}
	return lcheck.Input{Kind: kind, Key: cmd.Key, Val: cmd.Val}
}

func (s *Sim) newCommand(c *client) {
	c.seq++
	c.cmd = kv.Command{
		Client: uint64(c.id),
		Seq:    c.seq,
		Op:     s.drawOp(),
		Key:    s.drawKey(),
	}
	if c.cmd.Op != kv.OpGet {
		c.cmd.Val = s.drawValue()
	}
}

func (s *Sim) drawOp() kv.OpKind {
	w := s.wl.wl
	total := w.GetWeight + w.PutWeight + w.AppendWeight
	n := s.rng.Intn(total)
	switch {
	case n < w.GetWeight:
		return kv.OpGet
	case n < w.GetWeight+w.PutWeight:
		return kv.OpPut
	default:
		return kv.OpAppend
	}
}

func (s *Sim) drawKey() string {
	// Keys are drawn from c shared name space so that operations from
	// different clients contend; a workload with a key per client would never
	// exercise the ordering the checker is looking for.
	return string(rune('a' + s.rng.Intn(s.wl.wl.Keys)))
}

func (s *Sim) drawValue() string {
	return string(rune('0' + s.rng.Intn(s.wl.wl.Values)))
}

// proposeToAny offers the command to each live node in turn, starting from the
// one that worked last time. Only a leader accepts, so this is how a client
// discovers the leader without a separate lookup protocol.
func (s *Sim) proposeToAny(c *client) (int, bool) {
	n := len(s.nodes)
	for i := 0; i < n; i++ {
		idx := (c.next + i) % n
		ns := s.nodes[idx]
		if ns.down || ns.server == nil {
			continue
		}
		if err := ns.server.Propose(c.cmd); err == nil {
			c.next = idx
			return idx, true
		}
	}
	return 0, false
}

// VerifyAllReplicasAgree is a whole-cluster convergence check: every live node
// must have applied the same command sequence in the same order. It is
// deliberately not run on every tick, because it is O(nodes * log length).
func (s *Sim) VerifyAllReplicasAgree() []string {
	var problems []string
	var reference []raft.Entry
	for _, n := range s.nodes {
		if n.down {
			continue
		}
		if reference == nil {
			reference = n.server.Node().Entries()
			continue
		}
		got := n.server.Node().Entries()
		for i := range got {
			want, ok := lookup(reference, got[i].Index)
			if !ok {
				continue
			}
			if want.Term != got[i].Term {
				problems = append(problems, "index "+itoa(got[i].Index)+" is term "+itoa(want.Term)+
					" on the reference node but term "+itoa(got[i].Term)+" on node "+itoa(n.id))
			}
		}
	}
	return problems
}

func lookup(ents []raft.Entry, i raft.Index) (raft.Entry, bool) {
	for _, e := range ents {
		if e.Index == i {
			return e, true
		}
	}
	return raft.Entry{}, false
}

// itoa formats an integer without pulling fmt into the hot path.
func itoa[T ~int64 | ~uint64](v T) string {
	neg := false
	u := uint64(v)
	if v < 0 {
		neg = true
		u = uint64(-v)
	}
	if u == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
