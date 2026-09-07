// Package raft implements the Raft consensus algorithm with one unusual
// design constraint: the consensus core performs no I/O and reads no clock.
//
// Everything that would ordinarily touch the outside world arrives through an
// injected interface:
//
//   - Time enters as Tick calls. The core counts ticks; it has no idea how
//     long one lasts. Election timeouts are counts of ticks.
//   - Randomness enters as a Source. That is the only nondeterminism in the
//     package, and a caller that supplies a seeded generator makes the whole
//     node reproducible.
//   - Durability enters as a Storage. The core calls it synchronously and in
//     the order Raft requires (term and vote before a vote reply, entries
//     before an acknowledgement) but never learns what is behind it.
//   - Networking enters through Ready, which hands outbound messages to the
//     caller, and Step, which accepts inbound ones in whatever order the
//     caller chooses. Reordering, duplication and loss can therefore be
//     modelled exactly rather than approximated.
//
// The payoff is that the same binary that runs against a real disk and a real
// socket can also be driven by a deterministic simulator that replays a fault
// schedule bit for bit. See the sim package.
//
// The core implements §5.4.2's commit restriction, log compaction via
// snapshots plus InstallSnapshot, and single-server membership changes.
package raft
