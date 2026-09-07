package lcheck

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrBudgetExhausted is returned when the search exceeds Options.MaxSteps. It
// is deliberately distinguishable from "not linearizable": an inconclusive
// answer must never be reported as a refutation.
var ErrBudgetExhausted = errors.New("lcheck: search step budget exhausted")

// DefaultMaxSteps is the candidate-operation budget a zero Options gets.
const DefaultMaxSteps = 5_000_000

// Options tunes the search.
type Options struct {
	// MaxSteps bounds the number of candidate operations the search may try
	// across the whole run. Zero means DefaultMaxSteps.
	MaxSteps int
	// Memoize prunes search states that have already been refuted.
	Memoize bool
}

// DefaultOptions returns the options used when a caller has no opinion.
func DefaultOptions() Options { return Options{MaxSteps: DefaultMaxSteps, Memoize: true} }

// Result reports the outcome of a check.
type Result struct {
	Linearizable bool
	// Steps is the number of candidate operations that were tried.
	Steps int
	// Witness is a sequential order that explains the history, expressed as
	// indices into the input history.
	Witness []int
	// Explanation describes how far the search got, for a history that is not
	// linearizable.
	Explanation string
}

// Check searches for a sequential ordering of the operations that is
// consistent with both the real-time order and the sequential specification.
//
// The algorithm is Wing & Gong's: an operation may be linearized next only if
// no remaining operation must precede it in real time, that is, if its
// invocation is no later than the earliest completion among the remaining
// operations. The search backtracks over those minimal candidates. Within one
// client the operations are additionally forced into issue order, which the
// real-time rule alone does not guarantee when a client's operation intervals
// merely touch.
//
// It returns ErrBudgetExhausted, with Result.Linearizable false, when the
// budget runs out; callers must treat that as "unknown", never as a
// refutation.
func Check(h History, m Model, opts Options) (Result, error) {
	if m == nil {
		return Result{}, fmt.Errorf("lcheck: nil model")
	}
	if opts.MaxSteps <= 0 {
		opts.MaxSteps = DefaultMaxSteps
	}
	if err := h.Validate(); err != nil {
		return Result{}, err
	}
	if len(h) == 0 {
		return Result{Linearizable: true}, nil
	}

	s := &searcher{
		hist: h,
		opts: opts,
		memo: map[string]bool{},
		prev: prevByClient(h),
		done: make([]bool, len(h)),
	}
	remaining := make([]int, len(h))
	for i := range remaining {
		remaining[i] = i
	}
	ok, err := s.search(remaining, m)
	res := Result{Linearizable: ok, Steps: s.steps}
	if ok {
		res.Witness = append([]int(nil), s.order...)
	} else if err == nil {
		res.Explanation = s.explain()
	}
	return res, err
}

type searcher struct {
	hist History
	opts Options
	memo map[string]bool
	prev []int
	done []bool

	steps   int
	order   []int
	deepest []int
}

// prevByClient returns, for each operation, the index of the previous
// operation issued by the same client, or -1. It is precomputed so the
// per-candidate ordering test is O(1) rather than a scan.
func prevByClient(h History) []int {
	prev := make([]int, len(h))
	last := map[ClientID]int{}
	for i, op := range h {
		if p, ok := last[op.Client]; ok {
			prev[i] = p
		} else {
			prev[i] = -1
		}
		last[op.Client] = i
	}
	return prev
}

func (s *searcher) search(remaining []int, m Model) (bool, error) {
	if len(remaining) == 0 {
		return true, nil
	}

	var key string
	if s.opts.Memoize {
		key = memoKey(remaining, m.String())
		if s.memo[key] {
			return false, nil
		}
	}

	minComplete := s.hist[remaining[0]].Complete
	for _, i := range remaining[1:] {
		if c := s.hist[i].Complete; c < minComplete {
			minComplete = c
		}
	}

	for pos, i := range remaining {
		op := s.hist[i]
		if op.Invoke > minComplete {
			continue
		}
		if p := s.prev[i]; p >= 0 && !s.done[p] {
			// An earlier operation from the same client has not been
			// linearized yet.
			continue
		}
		next, ok := m.Apply(op)
		if !ok {
			continue
		}
		s.steps++
		if s.steps > s.opts.MaxSteps {
			return false, ErrBudgetExhausted
		}
		s.order = append(s.order, i)
		if len(s.order) > len(s.deepest) {
			s.deepest = append(s.deepest[:0], s.order...)
		}
		s.done[i] = true
		solved, err := s.search(removeAt(remaining, pos), next)
		s.done[i] = false
		if err != nil {
			return false, err
		}
		if solved {
			return true, nil
		}
		s.order = s.order[:len(s.order)-1]
	}

	if s.opts.Memoize {
		s.memo[key] = true
	}
	return false, nil
}

func (s *searcher) explain() string {
	if len(s.deepest) == 0 {
		return "no operation is legal as the first step, so the history has no linearization"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "the longest prefix the search could linearize was %d of %d operations:\n",
		len(s.deepest), len(s.hist))
	for _, i := range s.deepest {
		fmt.Fprintf(&b, "  %s\n", s.hist[i])
	}
	done := map[int]bool{}
	for _, i := range s.deepest {
		done[i] = true
	}
	for i, op := range s.hist {
		if !done[i] {
			fmt.Fprintf(&b, "  (unplaced) %s\n", op)
			break
		}
	}
	return b.String()
}

func memoKey(remaining []int, modelState string) string {
	var b strings.Builder
	b.Grow(len(remaining)*4 + len(modelState) + 1)
	for _, i := range remaining {
		b.WriteString(strconv.Itoa(i))
		b.WriteByte(',')
	}
	b.WriteByte('|')
	b.WriteString(modelState)
	return b.String()
}

func removeAt(s []int, pos int) []int {
	out := make([]int, 0, len(s)-1)
	out = append(out, s[:pos]...)
	out = append(out, s[pos+1:]...)
	return out
}
