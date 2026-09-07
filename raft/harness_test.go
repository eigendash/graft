package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// cluster is a synchronous, zero-latency network used by the unit tests.
//
// It exists in addition to the sim package on purpose: sim is built for scale
// and for long randomised runs, whereas these tests need to stop the world
// after every single message and look at intermediate states. The §5.4.2 test
// is only meaningful if it can observe the exact instant at which an
// old-term entry is on a majority but must not be committed.
type cluster struct {
	t      *testing.T
	ids    []NodeID
	nodes  map[NodeID]*Node
	stores map[NodeID]*MemoryStorage
	links  map[NodeID]map[NodeID]bool
	down   map[NodeID]bool

	seedBase   int64
	baseVoters []NodeID
	mutate     func(*Config)
	bugs       []string

	// snapInstalls counts how many InstallSnapshot images were actually
	// installed by a state machine, so a test can prove the snapshot path ran
	// rather than assuming it.
	snapInstalls int

	// commitRuleViolations counts leaders that advanced their commit index
	// onto an entry from an earlier term, which §5.4.2 forbids. It is a
	// counter rather than only a t.Errorf so tests can assert on it directly.
	commitRuleViolations int

	// applied records the entries each node handed to its state machine, and
	// commitTerms records the term of every index that was ever committed by
	// anyone. A committed index changing term would mean a committed entry was
	// overwritten, which is the single worst thing Raft can do.
	applied     map[NodeID][]Entry
	commitTerms map[Index]Term

	// lastCommit/lastTerm track, per node, the commit index and term at the
	// last observation, so checkLeaderCommitRule can tell an advance from a
	// restatement.
	lastCommit map[NodeID]Index
	lastTerm   map[NodeID]Term
}

func newCluster(t *testing.T, n int, mutate func(*Config)) *cluster {
	t.Helper()
	ids := make([]NodeID, n)
	for i := range ids {
		ids[i] = NodeID(i + 1)
	}
	c := &cluster{
		t:           t,
		ids:         ids,
		nodes:       map[NodeID]*Node{},
		stores:      map[NodeID]*MemoryStorage{},
		links:       map[NodeID]map[NodeID]bool{},
		down:        map[NodeID]bool{},
		seedBase:    1,
		mutate:      mutate,
		applied:     map[NodeID][]Entry{},
		commitTerms: map[Index]Term{},
		lastCommit:  map[NodeID]Index{},
		lastTerm:    map[NodeID]Term{},
	}
	for _, a := range ids {
		c.stores[a] = NewMemoryStorage()
		c.links[a] = map[NodeID]bool{}
		for _, b := range ids {
			c.links[a][b] = true
		}
	}
	c.baseVoters = append([]NodeID(nil), ids...)
	for _, id := range ids {
		c.newNode(id)
	}
	return c
}

// addProcess registers an additional process whose durable state is visible to
// the cluster but which is not yet a voter. It is created with the initial
// voter set, not one containing itself, so it cannot campaign its way into the
// configuration.
func (c *cluster) addProcess(id NodeID) {
	c.t.Helper()
	c.ids = append(c.ids, id)
	c.stores[id] = NewMemoryStorage()
	c.links[id] = map[NodeID]bool{}
	c.applied[id] = nil
	for _, o := range c.ids {
		if c.links[o] == nil {
			c.links[o] = map[NodeID]bool{}
		}
		c.links[id][o] = true
		c.links[o][id] = true
	}
	c.newNode(id)
}

func (c *cluster) newNode(id NodeID) {
	c.t.Helper()
	cfg := Config{
		ID:             id,
		Voters:         c.baseVoters,
		ElectionTick:   10,
		ElectionJitter: 6,
		HeartbeatTick:  2,
		Rand:           rand.New(rand.NewSource(c.seedBase*1000 + int64(id))),
		Storage:        c.stores[id],
		Bug: func(msg string) {
			c.bugs = append(c.bugs, fmt.Sprintf("node %d: %s", id, msg))
		},
	}
	if c.mutate != nil {
		c.mutate(&cfg)
	}
	n, err := NewNode(cfg)
	if err != nil {
		c.t.Fatalf("NewNode(%d): %v", id, err)
	}
	c.nodes[id] = n
	c.down[id] = false
}

// crash discards the node. The MemoryStorage survives, which is exactly the
// crash model the simulator implements.
func (c *cluster) crash(id NodeID) {
	c.down[id] = true
	delete(c.nodes, id)
}

func (c *cluster) restart(id NodeID) { c.newNode(id) }

// seed replaces a node's durable state, so a test can start from a hand-written
// log rather than having to drive the cluster into that state.
func (c *cluster) seed(id NodeID, hs HardState, entries []Entry, snap *Snapshot) {
	c.t.Helper()
	st := NewMemoryStorage()
	if snap != nil {
		if err := st.SaveSnapshot(*snap); err != nil {
			c.t.Fatalf("seed %d snapshot: %v", id, err)
		}
	}
	if err := st.SaveHardState(hs); err != nil {
		c.t.Fatalf("seed %d hard state: %v", id, err)
	}
	if len(entries) > 0 {
		if err := st.SaveLog(LogMutation{Entries: entries}); err != nil {
			c.t.Fatalf("seed %d log: %v", id, err)
		}
	}
	c.stores[id] = st
	c.crash(id)
}

func (c *cluster) setAllLinks(up bool) {
	for _, a := range c.ids {
		for _, b := range c.ids {
			c.links[a][b] = up
		}
	}
}

func (c *cluster) link(a, b NodeID, up bool) {
	c.links[a][b] = up
	c.links[b][a] = up
}

func (c *cluster) isolate(id NodeID) {
	for _, o := range c.ids {
		if o != id {
			c.link(id, o, false)
		}
	}
}

func (c *cluster) heal() { c.setAllLinks(true) }

// absorb records what a node hands to its state machine and checks the
// no-committed-entry-ever-changes rule on the way past.
func (c *cluster) absorb(id NodeID, r Ready) {
	if r.Snapshot != nil {
		c.applied[id] = nil
		c.snapInstalls++
	}
	for _, e := range r.CommittedEntries {
		if prev, ok := c.commitTerms[e.Index]; ok && prev != e.Term {
			c.t.Errorf("node %d committed index %d with term %d, previously committed with term %d",
				id, e.Index, e.Term, prev)
		}
		c.commitTerms[e.Index] = e.Term
		c.applied[id] = append(c.applied[id], e)
	}
}

// checkLeaderCommitRule enforces §5.4.2 where it can actually be broken: a
// leader may only advance its commit index onto an entry of its own term.
//
// A leader that counted replicas for an older-term entry would show up here as
// soon as it advanced, which is what makes this a genuine falsification test
// rather than a restatement of the implementation.
func (c *cluster) checkLeaderCommitRule() {
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			continue
		}
		if c.lastTerm[id] != n.Term() {
			c.lastTerm[id] = n.Term()
			c.lastCommit[id] = n.Committed()
			continue
		}
		cur := n.Committed()
		if cur > c.lastCommit[id] && n.State() == StateLeader {
			if term, err := n.TermAt(cur); err == nil && term != n.Term() {
				c.commitRuleViolations++
				c.t.Errorf("node %d committed index %d (term %d) while leading term %d; §5.4.2 forbids this",
					id, cur, term, n.Term())
			}
		}
		c.lastCommit[id] = cur
	}
}

// round performs exactly one Ready/Step exchange. It returns the number of
// messages that reached the network.
func (c *cluster) round() (delivered int) {
	c.t.Helper()
	defer func() {
		if r := recover(); r != nil {
			c.t.Fatalf("raft invariant violated: %v (bug callbacks: %v)", r, c.bugs)
		}
	}()
	var msgs []Message
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			continue
		}
		r := n.Ready()
		c.absorb(id, r)
		msgs = append(msgs, r.Messages...)
		n.Advance()
	}
	for _, m := range msgs {
		if c.down[m.From] || c.down[m.To] || c.nodes[m.To] == nil {
			continue
		}
		if !c.links[m.From][m.To] {
			continue
		}
		c.nodes[m.To].Step(m)
		delivered++
	}
	c.checkLeaderCommitRule()
	return delivered
}

type linkMap map[NodeID]map[NodeID]bool

func (c *cluster) saveLinks() linkMap {
	out := linkMap{}
	for a, row := range c.links {
		out[a] = map[NodeID]bool{}
		for b, up := range row {
			out[a][b] = up
		}
	}
	return out
}

func (c *cluster) restoreLinks(m linkMap) {
	for a, row := range m {
		if c.links[a] == nil {
			c.links[a] = map[NodeID]bool{}
		}
		for b, up := range row {
			c.links[a][b] = up
		}
	}
}

// tickExcept advances every live node except the named ones. It lets a test
// freeze one node's log without also freezing its term, which isolates the
// backtracking path from the more disruptive no-PreVote behaviour.
func (c *cluster) tickExcept(skip ...NodeID) {
	c.t.Helper()
	frozen := map[NodeID]bool{}
	for _, id := range skip {
		frozen[id] = true
	}
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil && !frozen[id] {
			n.Tick()
		}
	}
	c.flush()
}

// propose sends data through whoever currently claims leadership, retrying
// across elections so that tests written against a stable cluster do not have
// to care which node happens to be leading.
func (c *cluster) propose(data string) Index {
	c.t.Helper()
	for i := 0; i < 50; i++ {
		if id, ok := c.leader(); ok {
			if idx, _, err := c.nodes[id].Propose([]byte(data)); err == nil {
				c.flush()
				return idx
			}
		}
		c.tick()
	}
	c.t.Fatalf("no leader accepted %q; terms=%v bugs=%v", data, c.terms(), c.bugs)
	return 0
}

// majorityHasLog reports whether at least a quorum of the named nodes holds an
// entry at index i.
func (c *cluster) majorityHasLog(nodes []NodeID, i Index) bool {
	count := 0
	for _, id := range nodes {
		if n := c.nodes[id]; n != nil && n.LastIndex() >= i {
			count++
		}
	}
	return count >= len(nodes)/2+1
}

// flush runs rounds until the network goes quiet.
func (c *cluster) flush() {
	c.t.Helper()
	for i := 0; i < 500; i++ {
		if c.round() == 0 {
			return
		}
	}
	c.t.Fatalf("cluster: message storm did not settle; bugs=%v", c.bugs)
}

func (c *cluster) tick() {
	c.t.Helper()
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			n.Tick()
		}
	}
	c.flush()
}

func (c *cluster) ticks(n int) {
	c.t.Helper()
	for i := 0; i < n; i++ {
		c.tick()
	}
}

// waitLeader ticks until some node reports itself leader and returns it.
func (c *cluster) waitLeader(maxTicks int) NodeID {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		c.tick()
		if id, ok := c.leader(); ok {
			return id
		}
	}
	c.t.Fatalf("no leader after %d ticks; terms=%v", maxTicks, c.terms())
	return 0
}

// forceElect drives id to candidacy in isolation, restores the links and runs
// the election to completion.
func (c *cluster) forceElect(id NodeID) {
	c.t.Helper()
	c.forceCandidate(id)
	c.flush()
	if c.nodes[id].State() != StateLeader {
		c.t.Fatalf("node %d did not win the election (state=%v)", id, c.nodes[id].State())
	}
}

// forceCandidate makes id a candidate in isolation and then restores the
// links *without* delivering anything, leaving the vote requests in its
// outbox. A test can drive the election one message at a time from there,
// which is what the Figure 8 scenarios need in order to stop at an exact
// instant.
func (c *cluster) forceCandidate(id NodeID) {
	c.t.Helper()
	saved := c.saveLinks()
	c.setAllLinks(false)
	c.flush()
	for i := 0; i < 500 && c.nodes[id].State() != StateCandidate; i++ {
		// Drain without delivering, then tick. The tick that makes id a
		// candidate leaves its vote requests in the outbox and the loop exits
		// before draining them, so they are still there when the links return.
		c.round()
		c.nodes[id].Tick()
	}
	if c.nodes[id].State() != StateCandidate {
		c.t.Fatalf("node %d never became a candidate", id)
	}
	c.restoreLinks(saved)
}

func (c *cluster) leader() (NodeID, bool) {
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil && n.State() == StateLeader {
			return id, true
		}
	}
	return 0, false
}

func (c *cluster) terms() map[NodeID]Term {
	out := map[NodeID]Term{}
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			out[id] = n.Term()
		}
	}
	return out
}

func (c *cluster) commits() map[NodeID]Index {
	out := map[NodeID]Index{}
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			out[id] = n.Committed()
		}
	}
	return out
}

// quorum returns the smallest set of node IDs that forms a majority.
func (c *cluster) quorum() int { return len(c.ids)/2 + 1 }

// snapshotAll compacts every live node at its own applied index, which is what
// a real server does when its log grows past a threshold.
func (c *cluster) snapshotAll() {
	c.t.Helper()
	for _, id := range c.ids {
		n := c.nodes[id]
		if n == nil {
			continue
		}
		if _, err := n.CreateSnapshot([]byte(fmt.Sprintf("state@%d", n.Applied()))); err != nil {
			c.t.Fatalf("node %d snapshot: %v", id, err)
		}
	}
}

func ent(term Term, index Index, data string) Entry {
	return Entry{Term: term, Index: index, Type: EntryNormal, Data: []byte(data)}
}

func ccEntry(term Term, index Index, cc ConfChange) Entry {
	return Entry{Term: term, Index: index, Type: EntryConfChange, Data: cc.Encode()}
}

// waitApplied blocks until every live node has applied through index i, so
// tests assert convergence rather than a fixed number of ticks.
func (c *cluster) waitApplied(i Index, maxTicks int) {
	c.t.Helper()
	for t := 0; t < maxTicks; t++ {
		ok := true
		for _, id := range c.ids {
			n := c.nodes[id]
			if n == nil {
				continue
			}
			if n.Applied() < i {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		c.tick()
	}
	c.t.Fatalf("nodes did not converge to applied >= %d; applied=%v", i, c.appliedIndexes())
}

func (c *cluster) appliedIndexes() map[NodeID]Index {
	out := map[NodeID]Index{}
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			out[id] = n.Applied()
		}
	}
	return out
}

// appliedData returns the payload sequence a node applied, which tests compare
// for Log Matching.
func (c *cluster) appliedData(id NodeID) []string {
	var out []string
	for _, e := range c.applied[id] {
		out = append(out, string(e.Data))
	}
	return out
}
