package sim

import (
	"hash/fnv"

	"github.com/eigendash/graft/kv"
	"github.com/eigendash/graft/raft"
)

// observe folds one node's hand-off into the cluster-wide safety record.
//
// Everything here is a check against what is already known, not a restatement
// of what the node just did. A node that applies an entry out of sequence, or
// applies a different entry at an index somebody else already committed, is
// reported even though it believes it is behaving correctly.
func (s *Sim) observe(n *nodeState, res kv.PollResult) {
	if res.Snapshot != nil {
		s.stats.SnapshotsInstalled++
		idx, term := res.Snapshot.Index, res.Snapshot.Term
		if prev, ok := s.committedTerm[idx]; ok && prev != term {
			s.violate("snapshot from node %d covers index %d with term %d, but index %d was already committed with term %d",
				n.id, idx, term, idx, prev)
		}
		s.committedTerm[idx] = term
		if idx < n.appliedTo {
			s.violate("node %d installed a snapshot at %d behind its applied index %d", n.id, idx, n.appliedTo)
		}
		n.appliedTo = idx
	}
	for _, e := range res.Committed {
		if want := n.appliedTo + 1; e.Index != want && e.Index != 0 {
			s.violate("node %d applied entry %d after %d, which is not the next index (entries arrived out of order or a committed entry was skipped)",
				n.id, e.Index, n.appliedTo)
			n.appliedTo = e.Index
		} else {
			n.appliedTo = e.Index
		}
		if prev, ok := s.committedTerm[e.Index]; ok {
			if prev != e.Term {
				s.violate("committed entry at index %d changed term from %d to %d when applied by node %d",
					e.Index, prev, e.Term, n.id)
			}
		} else {
			s.committedTerm[e.Index] = e.Term
		}
		s.stats.EntriesCommitted++
	}
	if res.Compacted {
		s.stats.SnapshotsCreated++
	}
}

// checkInvariants runs the safety properties the project claims.
func (s *Sim) checkInvariants() {
	for _, n := range s.nodes {
		if n.down {
			continue
		}
		nd := n.server.Node()

		if nd.Committed() > s.stats.MaxCommit {
			s.stats.MaxCommit = nd.Committed()
		}

		// Every applied entry was committed, and nothing is committed beyond
		// the end of the log.
		if nd.Applied() > nd.Committed() {
			s.violate("node %d applied to %d but has only committed to %d", n.id, nd.Applied(), nd.Committed())
		}
		if nd.Committed() > nd.LastIndex() {
			s.violate("node %d committed to %d beyond its last index %d", n.id, nd.Committed(), nd.LastIndex())
		}

		if nd.State() == raft.StateLeader {
			if prev, ok := s.leaderOfTerm[nd.Term()]; ok && prev != n.id {
				s.violate("term %d elected both node %d and node %d", nd.Term(), prev, n.id)
			}
			s.leaderOfTerm[nd.Term()] = n.id

			// §5.4.2 again, this time from the outside: a leader may only
			// advance its commit index onto an entry of its own term. Only the
			// first observation of a term counts, because a new leader
			// inherits the commit index its predecessor established.
			cur := nd.Committed()
			if s.leaderTerm[n.id] != nd.Term() {
				s.leaderTerm[n.id] = nd.Term()
			} else if cur > s.leaderCommit[n.id] {
				if term, err := nd.TermAt(cur); err == nil && term != nd.Term() {
					s.violate("node %d advanced its commit index to %d (term %d) while leading term %d",
						n.id, cur, term, nd.Term())
				}
			}
			s.leaderCommit[n.id] = cur
		}

		s.checkLogMatching(n, nd)
	}
}

// checkLogMatching enforces the Log Matching Property: if two logs contain an
// entry with the same index and term, then the entries are identical and so are
// all preceding entries.
//
// It is checked incrementally. For each entry it records a fingerprint of the
// entry together with the term of its predecessor; by induction, agreement on
// every (index, term) pair implies agreement on every prefix. The scan resumes
// from the highest index already verified, and restarts from scratch whenever
// the term at that index changes, which is the signature of a log being
// rewritten underneath the mark.
func (s *Sim) checkLogMatching(n *nodeState, nd *raft.Node) {
	if n.fpTo > 0 {
		if t, err := nd.TermAt(n.fpTo); err != nil || t != n.fpTerm {
			n.fpTo = nd.FirstIndex() - 1
			if n.fpTo > 0 {
				n.fpTo--
			}
		}
	}
	from := n.fpTo + 1
	if first := nd.FirstIndex(); from < first {
		from = first
	}
	var prevTerm raft.Term
	if from > 1 {
		t, err := nd.TermAt(from - 1)
		if err != nil {
			// The predecessor has been compacted, so the snapshot term is the
			// only available answer; that is exactly what a peer with the same
			// prefix would compute.
			if snap := nd.Snapshot(); snap != nil {
				t = snap.Term
			}
		}
		prevTerm = t
	}
	for _, e := range nd.EntriesFrom(from) {
		if e.Term < prevTerm {
			s.violate("node %d holds entry %d with term %d after term %d at index %d; a log's terms never decrease",
				n.id, e.Index, e.Term, prevTerm, e.Index-1)
		}
		key := logKey{index: e.Index, term: e.Term}
		fp := fingerprint(e, prevTerm)
		if old, ok := s.logEntries[key]; ok && old != fp {
			s.violate("Log Matching: index %d term %d is held with different content by different nodes", e.Index, e.Term)
		}
		s.logEntries[key] = fp
		n.fpTo, n.fpTerm = e.Index, e.Term
		prevTerm = e.Term
	}
}

func fingerprint(e raft.Entry, prevTerm raft.Term) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v uint64) {
		for i := 0; i < 8; i++ {
			buf[i] = byte(v >> (8 * i))
		}
		h.Write(buf[:])
	}
	put(uint64(e.Index))
	put(uint64(e.Term))
	put(uint64(e.Type))
	put(uint64(len(e.Data)))
	h.Write(e.Data)
	// The predecessor's term is what extends agreement on one entry into
	// agreement on the whole prefix.
	put(uint64(prevTerm))
	return h.Sum64()
}
