package raft

// ProgressState is the replication mode a leader uses for one follower.
type ProgressState uint8

const (
	// ProgressProbe sends one AppendEntries at a time and waits for the reply.
	// It is used whenever the leader does not know where the follower's log
	// ends, because a pipelined stream of entries that all miss wastes a lot
	// of bandwidth before the leader finds the match point.
	ProgressProbe ProgressState = iota
	// ProgressReplicate streams a window of entries without waiting.
	ProgressReplicate
	// ProgressSnapshot means an InstallSnapshot is outstanding; nothing else
	// may be sent to that follower until it replies.
	ProgressSnapshot
)

func (p ProgressState) String() string {
	switch p {
	case ProgressProbe:
		return "probe"
	case ProgressReplicate:
		return "replicate"
	case ProgressSnapshot:
		return "snapshot"
	default:
		return "unknown"
	}
}

// Progress is the leader's view of one follower (Figure 2's nextIndex and
// matchIndex, plus the state needed to drive them).
type Progress struct {
	// Match is the highest index known to be replicated on the follower.
	Match Index
	// Next is the next index to send. It is Match+1 once the follower is
	// caught up, and is walked backwards during backtracking.
	Next  Index
	State ProgressState

	// Inflights is the sliding window of pipelined AppendEntries. Bounding it
	// is what keeps a leader from buffering the whole log per follower.
	Inflights *inflights

	// PausedUntil is the tick before which a probe must not be re-sent. A
	// sticky pause (the etcd approach) relies on a heartbeat reply to clear it,
	// which never arrives if the probe itself is dropped; expiring the pause on
	// an election-timeout cadence makes a lost probe self-healing.
	PausedUntil int64
	// LastResponse is the tick of the last accepted or rejected reply, used as
	// the retransmission timer for a full window.
	LastResponse int64

	// PendingSnapshot is the snapshot index in flight, and used to ignore
	// stale replies that predate it.
	PendingSnapshot Index

	// LastActive is the tick this follower was last heard from, which is what
	// CheckQuorum examines.
	LastActive int64
}

func newProgress(next Index, window int) *Progress {
	return &Progress{Next: next, State: ProgressProbe, Inflights: newInflights(window)}
}

// inflights is a fixed-size ring of the last index of each in-flight
// AppendEntries. It is a ring rather than a slice so that appending and
// releasing are both O(1) and allocate nothing in steady state.
type inflights struct {
	buf   []Index
	start int
	count int
}

func newInflights(size int) *inflights {
	if size < 1 {
		size = 1
	}
	return &inflights{buf: make([]Index, size)}
}

func (in *inflights) full() bool { return in.count == len(in.buf) }

func (in *inflights) add(last Index) {
	if in.full() {
		panic("raft: inflight window overflow")
	}
	in.buf[(in.start+in.count)%len(in.buf)] = last
	in.count++
}

// freeTo releases every in-flight message whose last index is <= last.
func (in *inflights) freeTo(last Index) {
	for in.count > 0 {
		f := in.buf[in.start]
		if f > last {
			return
		}
		in.start = (in.start + 1) % len(in.buf)
		in.count--
	}
}

// freeFirstOne releases the oldest in-flight message regardless of index. It is
// the escape hatch when a reply proves the follower is alive but does not
// identify which message it answers.
func (in *inflights) freeFirstOne() {
	if in.count == 0 {
		return
	}
	in.start = (in.start + 1) % len(in.buf)
	in.count--
}

func (in *inflights) reset() {
	in.start, in.count = 0, 0
}
