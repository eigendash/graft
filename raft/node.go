package raft

import (
	"fmt"
	"sort"
)

// Source is the only source of randomness the consensus core may consume.
//
// It is a one-method interface rather than *math/rand.Rand so that this package
// does not import an entropy package at all: a reviewer can see that the entire
// nondeterminism surface of Raft is "how many ticks until the next election".
// math/rand.Rand satisfies it, and a caller that seeds one generator for the
// whole cluster gets a reproducible run.
type Source interface {
	Intn(n int) int
}

// Config configures a Node. Everything the node needs from the outside world
// appears here.
type Config struct {
	// ID must be non-zero; zero is the "nobody" sentinel.
	ID NodeID
	// Voters is the initial membership. It is only a seed: NewNode overlays
	// the snapshot's membership and then replays any configuration entries in
	// the persisted log, so a restarted node recovers the live configuration
	// without the caller having to remember it.
	Voters []NodeID

	// ElectionTick is the base election timeout in ticks and ElectionJitter
	// the width of the uniform random spread added to it. The spread is what
	// breaks symmetry between simultaneous candidates; without it, split votes
	// repeat forever.
	ElectionTick   int
	ElectionJitter int
	// HeartbeatTick is how often a leader re-sends AppendEntries so followers
	// do not start an election.
	HeartbeatTick int

	// Rand is required.
	Rand Source
	// Storage is required.
	Storage Storage

	// MaxEntriesPerMsg bounds one AppendEntries. It is the knob that makes
	// pipelining and backtracking observable: at 1 the protocol degenerates to
	// one entry per round trip.
	MaxEntriesPerMsg int
	// MaxInflight bounds outstanding AppendEntries per follower.
	MaxInflight int

	// CheckQuorum makes a leader step down when it has not heard from a
	// quorum for a couple of election timeouts. Without it an isolated leader
	// keeps accepting proposals that can never commit; with it, clients get a
	// prompt ErrNotLeader and can retry elsewhere. It is the cheap stand-in for
	// the lease mechanism a real system would use to serve reads.
	CheckQuorum bool

	// Bug is called immediately before an internal invariant failure panics.
	// The simulator uses it to record the failure as a violation rather than
	// losing it in a goroutine dump.
	Bug func(msg string)
}

func (c *Config) withDefaults() error {
	if c.ID == 0 {
		return fmt.Errorf("raft: Config.ID must be non-zero (zero means no node)")
	}
	if c.Storage == nil {
		return fmt.Errorf("raft: Config.Storage is required")
	}
	if c.Rand == nil {
		return fmt.Errorf("raft: Config.Rand is required; the core has no entropy of its own")
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
	if c.HeartbeatTick >= c.ElectionTick {
		// A heartbeat interval at or above the election floor makes a follower
		// time out between heartbeats and the cluster never stabilises.
		c.HeartbeatTick = c.ElectionTick / 2
		if c.HeartbeatTick < 1 {
			c.HeartbeatTick = 1
		}
	}
	if c.MaxEntriesPerMsg <= 0 {
		c.MaxEntriesPerMsg = 64
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 64
	}
	return nil
}

// SoftState is the part of a node's state that is not durable and is reported
// to the caller on every Ready so it can notice leadership changes.
type SoftState struct {
	Lead  NodeID
	State StateType
	Term  Term
}

// Ready is the hand-off between the consensus core and its caller. A caller
// must, in order: install Snapshot if present, apply CommittedEntries, deliver
// Messages, then call Advance.
type Ready struct {
	Messages []Message
	// Snapshot, when non-nil, supersedes whatever the state machine held: it
	// must be installed before CommittedEntries are applied.
	Snapshot *Snapshot
	// CommittedEntries are newly committed and not yet applied, in log order.
	CommittedEntries []Entry
	SoftState        SoftState
}

// Node is a single Raft participant. It is not safe for concurrent use; the
// caller is expected to drive it from one loop, which is also what makes the
// tick count a meaningful global ordering.
type Node struct {
	cfg Config
	id  NodeID
	rnd Source
	// now is a logical tick counter. It is the only notion of time in this
	// package, and the only reason CheckQuorum is expressible without a clock.
	now int64

	term  Term
	vote  NodeID
	state StateType
	lead  NodeID

	log      *raftLog
	snapshot *Snapshot

	// voters and voterList are kept in lockstep. voterList is sorted so that
	// message emission order is a pure function of the configuration, which is
	// what makes an entire simulated run reproducible.
	voters    map[NodeID]bool
	voterList []NodeID

	prs map[NodeID]*Progress

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	votesGranted map[NodeID]bool

	outbox []Message

	pendingSnapshot *Snapshot
	readyAppliedTo  Index
}

// NewNode builds a node and recovers its durable state. It performs no I/O of
// its own: the storage handle does.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}
	ps, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading persisted state: %w", err)
	}

	n := &Node{
		cfg:    cfg,
		id:     cfg.ID,
		rnd:    cfg.Rand,
		term:   ps.HardState.Term,
		vote:   ps.HardState.Vote,
		state:  StateFollower,
		voters: map[NodeID]bool{},
	}

	snapIdx, snapTerm := Index(0), Term(0)
	if ps.Snapshot != nil {
		n.snapshot = ps.Snapshot.Clone()
		snapIdx, snapTerm = ps.Snapshot.Index, ps.Snapshot.Term
		n.pendingSnapshot = ps.Snapshot.Clone()
	}

	seed := cfg.Voters
	if ps.Snapshot != nil && len(ps.Snapshot.Voters) > 0 {
		// A snapshot is authoritative: entries describing earlier membership
		// changes have been compacted away, so the image has to carry it.
		seed = ps.Snapshot.Voters
	}
	for _, id := range SortedVoters(seed) {
		if id == 0 {
			continue
		}
		n.voters[id] = true
		n.voterList = append(n.voterList, id)
	}

	n.log = newRaftLog(snapIdx, snapTerm, ps.Entries)
	n.log.committed = ps.HardState.Commit
	if n.log.committed < snapIdx {
		n.log.committed = snapIdx
	}
	// Nothing above the snapshot has been applied yet: the caller restores the
	// state machine from the snapshot and then applies CommittedEntries, which
	// is exactly "drop volatile state and replay only what was persisted".
	n.log.applied = snapIdx
	n.readyAppliedTo = snapIdx

	// Configuration entries take effect when they are appended, so replaying
	// the persisted log rebuilds the membership that was in force.
	for _, e := range ps.Entries {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			return nil, fmt.Errorf("raft: replaying configuration at index %d: %w", e.Index, err)
		}
		n.applyConfChange(cc)
	}

	n.resetElectionTimeout()
	return n, nil
}

// --- accessors ---------------------------------------------------------------

func (n *Node) ID() NodeID          { return n.id }
func (n *Node) Term() Term          { return n.term }
func (n *Node) State() StateType    { return n.state }
func (n *Node) Leader() NodeID      { return n.lead }
func (n *Node) Committed() Index    { return n.log.committed }
func (n *Node) Applied() Index      { return n.log.applied }
func (n *Node) LastIndex() Index    { return n.log.lastIndex() }
func (n *Node) LastTerm() Term      { return n.log.lastTerm() }
func (n *Node) Now() int64          { return n.now }
func (n *Node) Snapshot() *Snapshot { return n.snapshot.Clone() }

// Voters returns the current membership in sorted order.
func (n *Node) Voters() []NodeID { return append([]NodeID(nil), n.voterList...) }

// TermAt returns the term of the entry at index i in this node's log.
func (n *Node) TermAt(i Index) (Term, error) { return n.log.term(i) }

// Entries returns a copy of the entries currently in memory, i.e. everything
// above the snapshot.
func (n *Node) Entries() []Entry { return n.log.entriesFrom() }

// FirstIndex is the lowest index the node can still serve.
func (n *Node) FirstIndex() Index { return n.log.firstIndex() }

func (n *Node) quorum() int {
	q := len(n.voterList)/2 + 1
	if q < 1 {
		q = 1
	}
	return q
}

func (n *Node) bug(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if n.cfg.Bug != nil {
		n.cfg.Bug(msg)
	}
	panic("raft: " + msg)
}

func (n *Node) send(m Message) {
	m.From = n.id
	n.outbox = append(n.outbox, m)
}

// --- driving -----------------------------------------------------------------

// Tick advances logical time by one tick. It is the only way time enters the
// core.
func (n *Node) Tick() {
	n.now++
	if n.state == StateLeader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			for _, id := range n.voterList {
				if id != n.id {
					n.sendAppend(id, true)
				}
			}
		}
		if n.cfg.CheckQuorum && !n.hasQuorumContact() {
			// An isolated leader would otherwise keep accepting proposals that
			// can never commit. Stepping down converts a silent hang into an
			// immediate ErrNotLeader.
			n.becomeFollower(n.term, 0)
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

func (n *Node) hasQuorumContact() bool {
	active := 1 // ourselves
	idle := n.quorumIdleTicks()
	for _, id := range n.voterList {
		if id == n.id {
			continue
		}
		if pr := n.prs[id]; pr != nil && pr.LastActive+idle >= n.now {
			active++
		}
	}
	return active >= n.quorum()
}

func (n *Node) quorumIdleTicks() int64 {
	return 2 * int64(n.cfg.ElectionTick+n.cfg.ElectionJitter)
}

func (n *Node) resetElectionTimeout() {
	n.electionTimeout = n.cfg.ElectionTick + n.rnd.Intn(n.cfg.ElectionJitter)
	if n.electionTimeout < 1 {
		n.electionTimeout = 1
	}
}

// Step feeds one inbound message. Messages may arrive in any order, be
// duplicated, or be arbitrarily delayed; the protocol is responsible for
// tolerating all of that.
func (n *Node) Step(m Message) {
	if m.To != 0 && m.To != n.id {
		n.bug("message addressed to %d delivered to %d", m.To, n.id)
		return
	}
	if m.Term > n.term {
		// Any message from a newer term forces adoption of that term. Raft
		// depends on this for both elections and leader step-down.
		n.becomeFollower(m.Term, 0)
	}
	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		n.handleAppend(m)
	case MsgAppResp, MsgSnapResp:
		n.handleAppendResp(m)
	case MsgSnap:
		n.handleSnapshot(m)
	default:
		n.bug("unknown message type %d", m.Type)
	}
}

// Ready drains the effects the caller must act on.
func (n *Node) Ready() Ready {
	r := Ready{
		Messages:  n.outbox,
		SoftState: SoftState{Lead: n.lead, State: n.state, Term: n.term},
	}
	n.outbox = nil
	n.readyAppliedTo = n.log.applied

	if n.pendingSnapshot != nil {
		r.Snapshot = n.pendingSnapshot
		n.pendingSnapshot = nil
		if r.Snapshot.Index > n.readyAppliedTo {
			n.readyAppliedTo = r.Snapshot.Index
		}
	}
	if n.log.committed > n.readyAppliedTo {
		ents, err := n.log.slice(n.readyAppliedTo+1, n.log.committed+1)
		if err != nil {
			n.bug("committed entries (%d,%d] unavailable: %v", n.readyAppliedTo, n.log.committed, err)
		} else {
			// Copy: the caller may hold these across the next Step/Tick, and
			// the log's backing array is reused by conflict resolution.
			r.CommittedEntries = append([]Entry(nil), ents...)
			n.readyAppliedTo = n.log.committed
		}
	}
	return r
}

// Advance confirms that everything from the last Ready has been applied.
func (n *Node) Advance() {
	if n.readyAppliedTo > n.log.applied {
		n.log.applied = n.readyAppliedTo
	}
}

// --- proposing ---------------------------------------------------------------

// Propose appends data to the log if this node is the leader. The returned
// index is only a promise of position, not of commitment: the entry may still
// be overwritten if this leader loses an election it has not yet committed in.
func (n *Node) Propose(data []byte) (Index, Term, error) {
	if n.state != StateLeader {
		return 0, 0, ErrNotLeader
	}
	e := n.appendEntry(Entry{Type: EntryNormal, Data: data})
	n.maybeCommit()
	n.broadcastAppend()
	return e.Index, e.Term, nil
}

// ProposeConfChange adds or removes a single voter.
func (n *Node) ProposeConfChange(cc ConfChange) (Index, Term, error) {
	if n.state != StateLeader {
		return 0, 0, ErrNotLeader
	}
	if cc.Node == 0 {
		return 0, 0, fmt.Errorf("raft: conf change names node 0")
	}
	e := n.appendEntry(Entry{Type: EntryConfChange, Data: cc.Encode()})
	n.maybeCommit()
	n.broadcastAppend()
	return e.Index, e.Term, nil
}

// CreateSnapshot compacts the log up to the last applied index and stores data
// as the state machine image for that point. It returns nil when there is
// nothing new to compact, which makes it safe to call on a fixed schedule.
func (n *Node) CreateSnapshot(data []byte) (*Snapshot, error) {
	idx := n.log.applied
	if idx <= n.log.snapshotIndex {
		return nil, nil
	}
	t, err := n.log.term(idx)
	if err != nil {
		return nil, fmt.Errorf("raft: snapshotting to %d: %w", idx, err)
	}
	snap := Snapshot{
		Index:  idx,
		Term:   t,
		Voters: append([]NodeID(nil), n.voterList...),
		Data:   append([]byte(nil), data...),
	}
	if err := n.cfg.Storage.SaveSnapshot(snap); err != nil {
		return nil, fmt.Errorf("raft: persisting snapshot: %w", err)
	}
	n.snapshot = &snap
	n.log.compactTo(idx, t)
	return &snap, nil
}

func (n *Node) broadcastAppend() {
	for _, id := range n.voterList {
		if id != n.id {
			n.sendAppend(id, false)
		}
	}
}

// --- elections ---------------------------------------------------------------

func (n *Node) campaign() {
	n.becomeCandidate()
	if n.voters[n.id] && len(n.voterList) == 1 {
		n.becomeLeader()
		return
	}
	lastIndex, lastTerm := n.log.lastIndex(), n.log.lastTerm()
	for _, id := range n.voterList {
		if id != n.id {
			n.send(Message{Type: MsgVote, To: id, Term: n.term, LogTerm: lastTerm, Index: lastIndex})
		}
	}
}

func (n *Node) becomeCandidate() {
	if n.state == StateLeader {
		n.bug("leader %d tried to campaign", n.id)
	}
	n.state = StateCandidate
	n.lead = 0
	n.term++
	// Voting for ourselves must be durable before the request goes out;
	// otherwise a crash could let us vote for somebody else in the same term.
	n.vote = n.id
	n.persistHardState()
	n.votesGranted = map[NodeID]bool{n.id: true}
	n.electionElapsed = 0
	n.resetElectionTimeout()
}

func (n *Node) becomeLeader() {
	if n.state != StateCandidate {
		n.bug("becomeLeader from %v", n.state)
	}
	n.state = StateLeader
	n.lead = n.id
	n.heartbeatElapsed = 0
	n.prs = make(map[NodeID]*Progress, len(n.voterList))
	last := n.log.lastIndex()
	for _, id := range n.voterList {
		if id == n.id {
			pr := newProgress(last+1, n.cfg.MaxInflight)
			pr.Match, pr.State, pr.LastActive = last, ProgressReplicate, n.now
			n.prs[id] = pr
			continue
		}
		n.prs[id] = newProgress(last+1, n.cfg.MaxInflight)
	}
	// The no-op entry is load-bearing. §5.4.2 forbids a leader from committing
	// an entry of an earlier term by counting replicas, so a fresh leader must
	// place an entry of its own term in the log before it can advance the
	// commit index at all. It also gives the cluster a single point at which
	// "this leader is established" becomes observable.
	n.appendEntry(Entry{Type: EntryNormal})
	n.maybeCommit()
	n.broadcastAppend()
}

func (n *Node) becomeFollower(term Term, lead NodeID) {
	if term < n.term {
		n.bug("term regression %d -> %d", n.term, term)
		return
	}
	if term > n.term {
		n.term = term
		n.vote = 0
		n.persistHardState()
	}
	n.state = StateFollower
	n.lead = lead
	n.prs = nil
	n.votesGranted = nil
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.resetElectionTimeout()
}

func (n *Node) handleVote(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	if n.vote != 0 && n.vote != m.From {
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	if !n.log.isUpToDate(m.Index, m.LogTerm) {
		n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term, Reject: true})
		return
	}
	if n.vote != m.From {
		n.vote = m.From
		n.persistHardState()
	}
	// Granting a vote restarts the election timer, so a node that is helping
	// somebody else win does not simultaneously campaign against them.
	n.electionElapsed = 0
	n.resetElectionTimeout()
	n.send(Message{Type: MsgVoteResp, To: m.From, Term: n.term})
}

func (n *Node) handleVoteResp(m Message) {
	if m.Term < n.term || n.state != StateCandidate || m.Term != n.term {
		return
	}
	if m.Reject {
		return
	}
	n.votesGranted[m.From] = true
	if n.countVotes() >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) countVotes() int {
	c := 0
	for _, id := range n.voterList {
		if n.votesGranted[id] {
			c++
		}
	}
	return c
}

// --- AppendEntries -----------------------------------------------------------

func (n *Node) handleAppend(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Index: m.Index})
		return
	}
	if n.state == StateLeader && m.Term == n.term {
		n.bug("two leaders in term %d: %d and %d", n.term, n.id, m.From)
		return
	}
	n.becomeFollower(m.Term, m.From)

	// A delayed or duplicated AppendEntries whose prefix is already committed
	// must not be allowed to truncate committed entries. Reporting our own
	// commit index is safe because the Leader Completeness Property guarantees
	// the leader's committed prefix is identical to ours.
	if m.Index < n.log.committed {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: n.log.committed})
		return
	}

	if !n.log.matchTerm(m.Index, m.LogTerm) {
		hint := n.log.lastIndex() + 1
		if m.Index <= n.log.lastIndex() {
			if t, err := n.log.term(m.Index); err == nil {
				if first := n.log.firstIndexInTerm(t); first != 0 {
					hint = first
				}
			}
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Index: m.Index, RejectHint: hint})
		return
	}

	lastNew, ok := n.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries)
	if !ok {
		n.bug("append accepted at %d/%d then rejected", m.LogTerm, m.Index)
		return
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: lastNew})
}

func (n *Node) maybeAppend(index Index, logTerm Term, committed Index, ents []Entry) (Index, bool) {
	if !n.log.matchTerm(index, logTerm) {
		return 0, false
	}
	lastNew := index + Index(len(ents))
	if ci := n.log.findConflict(ents); ci != 0 {
		if ci <= n.log.committed {
			n.bug("leader asked to overwrite committed entry %d (commit=%d)", ci, n.log.committed)
			return lastNew, false
		}
		// Truncate and append in one durable operation. Only the conflicting
		// suffix is touched: when the entries already match, doing nothing
		// keeps the common path free of writes.
		n.appendEntriesToLog(ci, ents[ci-index-1:])
	}
	if committed > n.log.committed {
		// §5.3: a follower commits to min(leaderCommit, index of last new
		// entry). The leader's own commit index already respected §5.4.2, so
		// the follower can follow it directly.
		if committed > lastNew {
			committed = lastNew
		}
		n.commitTo(committed)
	}
	return lastNew, true
}

func (n *Node) appendEntriesToLog(truncateFrom Index, ents []Entry) {
	if len(ents) == 0 && truncateFrom == 0 {
		return
	}
	if err := n.cfg.Storage.SaveLog(LogMutation{TruncateFrom: truncateFrom, Entries: ents}); err != nil {
		n.bug("persisting log: %v", err)
		return
	}
	n.log.truncateAndAppend(truncateFrom, ents)
	for _, e := range ents {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			n.bug("decoding conf change at index %d: %v", e.Index, err)
			return
		}
		n.applyConfChange(cc)
	}
}

// appendEntry places a new entry of the current term at the end of the log on
// a leader.
func (n *Node) appendEntry(e Entry) Entry {
	e.Term = n.term
	e.Index = n.log.lastIndex() + 1
	// A leader replicates to itself immediately: its own match index is what
	// makes a quorum of one possible and is half of every majority.
	if n.state == StateLeader {
		if pr := n.prs[n.id]; pr != nil {
			pr.Match = e.Index
			pr.Next = e.Index + 1
		}
	}
	n.appendEntriesToLog(0, []Entry{e})
	return e
}

func (n *Node) handleAppendResp(m Message) {
	if m.Term < n.term || n.state != StateLeader || m.Term != n.term {
		return
	}
	pr := n.prs[m.From]
	if pr == nil {
		return
	}
	pr.LastActive = n.now

	if m.Reject {
		pr.LastResponse = n.now
		if pr.State == ProgressSnapshot && pr.PendingSnapshot > m.RejectHint {
			// A rejection that predates the snapshot we shipped says nothing
			// about the follower's current position.
			return
		}
		pr.State = ProgressProbe
		pr.Inflights.reset()
		pr.PendingSnapshot = 0
		// Backtrack to the follower's hint, but never forward and never below
		// index 1, so each rejection is guaranteed to make progress.
		next := m.Index
		if m.RejectHint > 0 && m.RejectHint < next {
			next = m.RejectHint
		}
		if next < 1 {
			next = 1
		}
		pr.Next = next
		pr.PausedUntil = 0
		n.sendAppend(m.From, true)
		return
	}

	if pr.State == ProgressSnapshot {
		if m.Index < pr.PendingSnapshot {
			return
		}
		pr.State = ProgressProbe
		pr.PendingSnapshot = 0
		pr.Inflights.reset()
		pr.Match, pr.Next = m.Index, m.Index+1
		pr.LastResponse = n.now
		n.maybeCommit()
		n.sendAppend(m.From, false)
		return
	}

	if m.Index <= pr.Match {
		return // stale or duplicated reply
	}
	if pr.State == ProgressReplicate {
		before := pr.Inflights.count
		pr.Inflights.freeTo(m.Index)
		if pr.Inflights.count == before {
			pr.Inflights.freeFirstOne()
		}
	} else {
		pr.State = ProgressReplicate
		pr.Inflights.reset()
	}
	pr.PausedUntil = 0
	pr.LastResponse = n.now
	pr.Match, pr.Next = m.Index, m.Index+1
	n.maybeCommit()
	n.sendAppend(m.From, false)
}

func (n *Node) maybeCommit() bool {
	if n.state != StateLeader {
		return false
	}
	matches := make([]Index, 0, len(n.voterList))
	for _, id := range n.voterList {
		if pr := n.prs[id]; pr != nil {
			matches = append(matches, pr.Match)
			continue
		}
		matches = append(matches, 0)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	quorumIndex := matches[n.quorum()-1]
	if quorumIndex <= n.log.committed {
		return false
	}
	t, err := n.log.term(quorumIndex)
	if err != nil {
		return false
	}
	// §5.4.2: "a leader may only count replicas for entries from its own term".
	// Committing an entry from an earlier term by replica count is the exact
	// mistake Figure 8 is designed to catch: that entry can still be
	// overwritten by a leader that never saw it.
	if t != n.term {
		return false
	}
	n.commitTo(quorumIndex)
	return true
}

func (n *Node) commitTo(i Index) {
	if i <= n.log.committed {
		return
	}
	if i > n.log.lastIndex() {
		n.bug("commit index %d beyond last index %d", i, n.log.lastIndex())
		return
	}
	n.log.committed = i
	n.persistHardState()
}

func (n *Node) persistHardState() {
	hs := HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}
	if err := n.cfg.Storage.SaveHardState(hs); err != nil {
		n.bug("persisting hard state: %v", err)
	}
}

// sendAppend implements the leader half of AppendEntries, including
// backtracking, pipelining and snapshot fallback.
func (n *Node) sendAppend(to NodeID, force bool) {
	pr := n.prs[to]
	if pr == nil {
		return
	}
	switch pr.State {
	case ProgressProbe, ProgressSnapshot:
		// A probe or a snapshot that is lost in the network would otherwise
		// pause this follower forever, because the thing that would clear the
		// pause is the reply. Expiring the pause on an election-timeout
		// cadence turns the heartbeat into a retransmission timer.
		if pr.PausedUntil > n.now {
			return
		}
	case ProgressReplicate:
		if pr.Inflights.full() {
			if n.now-pr.LastResponse < int64(n.cfg.ElectionTick) {
				return
			}
			// The window is full and nothing has come back for a whole
			// election timeout. Treating the pipeline as lost is the
			// retransmission timer for the pipelined path; without it a burst
			// of drops would wedge the follower permanently.
			pr.Inflights.reset()
			pr.LastResponse = n.now
		}
	}

	last := n.log.lastIndex()
	if pr.Next > last {
		if !force {
			return
		}
		// An empty AppendEntries still carries the commit index, so it serves
		// as both heartbeat and probe.
		pt, err := n.log.term(last)
		if err != nil {
			n.bug("term(%d): %v", last, err)
			return
		}
		if pr.State == ProgressProbe {
			pr.PausedUntil = n.now + int64(n.cfg.ElectionTick)
		}
		n.send(Message{Type: MsgApp, To: to, Term: n.term, Index: last, LogTerm: pt, Commit: n.log.committed})
		return
	}

	if pr.Next <= n.log.snapshotIndex {
		if n.snapshot == nil {
			n.bug("node %d is behind the compacted log but no snapshot is held", to)
			return
		}
		pr.State = ProgressSnapshot
		pr.PendingSnapshot = n.snapshot.Index
		pr.PausedUntil = n.now + int64(n.cfg.ElectionTick)
		pr.Inflights.reset()
		n.send(Message{Type: MsgSnap, To: to, Term: n.term, Snapshot: n.snapshot})
		return
	}

	prev := pr.Next - 1
	pt, err := n.log.term(prev)
	if err != nil {
		n.bug("term(%d): %v", prev, err)
		return
	}
	hi := last + 1
	if lim := pr.Next + Index(n.cfg.MaxEntriesPerMsg); lim < hi {
		hi = lim
	}
	ents, err := n.log.slice(pr.Next, hi)
	if err != nil {
		n.bug("slice(%d,%d): %v", pr.Next, hi, err)
		return
	}
	if pr.State == ProgressProbe {
		pr.PausedUntil = n.now + int64(n.cfg.ElectionTick)
	} else {
		pr.Inflights.add(hi - 1)
	}
	n.send(Message{Type: MsgApp, To: to, Term: n.term, Index: prev, LogTerm: pt, Entries: ents, Commit: n.log.committed})
}

// --- InstallSnapshot ---------------------------------------------------------

func (n *Node) handleSnapshot(m Message) {
	if m.Term < n.term || m.Snapshot == nil {
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Reject: true, Index: m.Index})
		return
	}
	if n.state == StateLeader && m.Term == n.term {
		n.bug("leader %d received InstallSnapshot in its own term", n.id)
		return
	}
	n.becomeFollower(m.Term, m.From)
	snap := m.Snapshot

	if n.log.matchTerm(snap.Index, snap.Term) {
		// Already have that point; just move our commit index, which the
		// leader will not learn about from an AppendEntries it never sent.
		n.commitTo(snap.Index)
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: n.log.lastIndex()})
		return
	}
	if snap.Index <= n.log.committed {
		// Stale snapshot for a prefix we already committed; acknowledging the
		// real position is more useful than restarting from the image.
		n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: n.log.lastIndex()})
		return
	}
	if err := n.cfg.Storage.SaveSnapshot(*snap); err != nil {
		n.bug("persisting installed snapshot: %v", err)
		return
	}
	n.snapshot = snap.Clone()
	n.pendingSnapshot = snap.Clone()
	n.log.restore(*snap)
	n.readyAppliedTo = snap.Index
	if len(snap.Voters) > 0 {
		n.replaceVoters(snap.Voters)
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Term: n.term, Index: snap.Index})
}

// --- membership --------------------------------------------------------------

func (n *Node) replaceVoters(ids []NodeID) {
	n.voters = make(map[NodeID]bool, len(ids))
	n.voterList = nil
	for _, id := range SortedVoters(ids) {
		if id == 0 {
			continue
		}
		n.voters[id] = true
		n.voterList = append(n.voterList, id)
	}
}

// applyConfChange installs a membership change.
//
// Configuration entries take effect as soon as they are appended, not when they
// commit. That is the rule the Raft dissertation prescribes for single-server
// changes (§4.1): a node that waits for commitment can be removed from the
// configuration that would have committed the removal, and it would sit there
// campaigning forever.
func (n *Node) applyConfChange(cc ConfChange) {
	switch cc.Type {
	case ConfAddNode:
		if n.voters[cc.Node] {
			return
		}
		n.voters[cc.Node] = true
		n.voterList = sortedInsert(n.voterList, cc.Node)
		if n.state == StateLeader {
			n.prs[cc.Node] = newProgress(n.log.lastIndex()+1, n.cfg.MaxInflight)
			// A brand new voter has nothing, so push immediately instead of
			// waiting for the next heartbeat.
			n.sendAppend(cc.Node, true)
		}
	case ConfRemoveNode:
		if !n.voters[cc.Node] {
			return
		}
		delete(n.voters, cc.Node)
		n.voterList = sortedRemove(n.voterList, cc.Node)
		delete(n.prs, cc.Node)
		if cc.Node == n.id {
			n.becomeFollower(n.term, 0)
		}
	}
}

func sortedInsert(list []NodeID, id NodeID) []NodeID {
	i := sort.Search(len(list), func(i int) bool { return list[i] >= id })
	if i < len(list) && list[i] == id {
		return list
	}
	list = append(list, 0)
	copy(list[i+1:], list[i:])
	list[i] = id
	return list
}

func sortedRemove(list []NodeID, id NodeID) []NodeID {
	i := sort.Search(len(list), func(i int) bool { return list[i] >= id })
	if i < len(list) && list[i] == id {
		return append(list[:i], list[i+1:]...)
	}
	return list
}
