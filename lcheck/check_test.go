package lcheck

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func regWrite(client ClientID, val string, invoke, complete int64) Operation {
	return Operation{Client: client, Input: RegisterInput{Write: true, Val: val}, Output: RegisterOutput{}, Invoke: invoke, Complete: complete}
}

func regRead(client ClientID, val string, invoke, complete int64) Operation {
	return Operation{Client: client, Input: RegisterInput{Write: false}, Output: RegisterOutput{Val: val}, Invoke: invoke, Complete: complete}
}

func kvGet(client ClientID, key, val string, found bool, invoke, complete int64) Operation {
	return Operation{Client: client, Input: Input{Kind: KindGet, Key: key}, Output: Output{Val: val, Found: found}, Invoke: invoke, Complete: complete}
}

func kvPut(client ClientID, key, val string, invoke, complete int64) Operation {
	return Operation{Client: client, Input: Input{Kind: KindPut, Key: key, Val: val}, Output: Output{}, Invoke: invoke, Complete: complete}
}

func kvAppend(client ClientID, key, val string, invoke, complete int64) Operation {
	return Operation{Client: client, Input: Input{Kind: KindAppend, Key: key, Val: val}, Output: Output{}, Invoke: invoke, Complete: complete}
}

// TestCheckRejectsNonLinearizableRegister is the test that gives the checker
// its value. A checker that cannot fail is worse than no checker at all, so
// these histories are hand-written to be unambiguously illegal.
func TestCheckRejectsNonLinearizableRegister(t *testing.T) {
	cases := []struct {
		name string
		h    History
	}{
		{
			// The write completed before the read began, so the read must see
			// it. It does not.
			name: "read misses a completed write",
			h: History{
				regWrite(1, "1", 0, 10),
				regRead(2, "0", 11, 20),
			},
		},
		{
			// Both writes complete before the final read starts, so the read
			// must see the later one. It sees the initial value instead.
			name: "final read ignores two completed writes",
			h: History{
				regRead(3, "0", 0, 5),
				regWrite(1, "1", 1, 10),
				regRead(2, "1", 6, 12),
				regWrite(5, "2", 8, 15),
				regRead(4, "0", 16, 20),
			},
		},
		{
			// A client must observe its own completed write: no sequential
			// register can explain reading the old value straight afterwards.
			name: "client cannot read its own stale write",
			h: History{
				regWrite(1, "1", 0, 5),
				regRead(1, "0", 6, 10),
			},
		},
		{
			// The two writes are concurrent, but the read is not: it starts
			// after both completed, so it must see one of them and not 0.
			name: "concurrent writes then a sequential read of the initial value",
			h: History{
				regWrite(1, "1", 0, 10),
				regWrite(2, "2", 0, 10),
				regRead(3, "0", 11, 20),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Check(tc.h, NewRegisterModel("0"), DefaultOptions())
			if err != nil {
				t.Fatalf("Check returned an error for a small history: %v", err)
			}
			if res.Linearizable {
				t.Fatalf("checker accepted a non-linearizable history (witness %v, steps %d)", res.Witness, res.Steps)
			}
			if res.Explanation == "" {
				t.Error("rejection came with no explanation")
			}
		})
	}
}

func TestCheckAcceptsLinearizableRegister(t *testing.T) {
	cases := []struct {
		name string
		h    History
	}{
		{
			name: "sequential write then read",
			h: History{
				regWrite(1, "1", 0, 10),
				regRead(2, "1", 11, 20),
			},
		},
		{
			name: "read overlapping the write may see the old value",
			h: History{
				regWrite(1, "1", 0, 10),
				regRead(2, "0", 5, 15),
			},
		},
		{
			name: "read overlapping the write may see the new value",
			h: History{
				regWrite(1, "1", 0, 10),
				regRead(2, "1", 5, 15),
			},
		},
		{
			name: "two concurrent writes with a later read of the second",
			h: History{
				regWrite(1, "1", 0, 10),
				regWrite(2, "2", 1, 11),
				regRead(3, "2", 12, 20),
			},
		},
		{
			name: "two concurrent writes with a later read of the first",
			h: History{
				regWrite(1, "1", 0, 10),
				regWrite(2, "2", 1, 11),
				regRead(3, "1", 12, 20),
			},
		},
		{
			name: "a chain of clients each observing the previous write",
			h: History{
				regWrite(1, "1", 0, 5),
				regRead(2, "1", 6, 10),
				regWrite(2, "2", 11, 15),
				regRead(3, "2", 16, 20),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Check(tc.h, NewRegisterModel("0"), DefaultOptions())
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if !res.Linearizable {
				t.Fatalf("checker rejected a linearizable history: %s", res.Explanation)
			}
		})
	}
}

func TestCheckRejectsNonLinearizableKV(t *testing.T) {
	cases := []struct {
		name string
		h    History
	}{
		{
			// Both reads start after the Put completed, so both must see the
			// value the Put installed. One of them does not.
			name: "read after a completed put reports the key as absent",
			h: History{
				kvPut(1, "x", "a", 0, 10),
				kvGet(2, "x", "a", true, 11, 20),
				kvGet(3, "x", "", false, 12, 21),
			},
		},
		{
			// The two appends are ordered by real time, so the concatenation
			// order is fixed. The read reports the reverse.
			name: "append order contradicted by a later read",
			h: History{
				kvAppend(1, "x", "a", 0, 5),
				kvAppend(2, "x", "b", 6, 11),
				kvGet(3, "x", "ba", true, 12, 20),
			},
		},
		{
			// A client sees its own Put overwritten by nothing, yet reads the
			// old value after the Put completed.
			name: "put then a later read of the previous value",
			h: History{
				kvPut(1, "x", "a", 0, 5),
				kvGet(1, "x", "", false, 6, 10),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Check(tc.h, NewKVModel(), DefaultOptions())
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if res.Linearizable {
				t.Fatalf("checker accepted a non-linearizable key-value history (witness %v)", res.Witness)
			}
		})
	}
}

func TestCheckAcceptsLinearizableKV(t *testing.T) {
	h := History{
		kvAppend(1, "x", "a", 0, 4),
		kvGet(2, "x", "a", true, 5, 9),
		kvAppend(3, "x", "b", 10, 14),
		kvGet(4, "x", "ab", true, 15, 19),
		kvPut(5, "x", "z", 20, 24),
		kvGet(6, "x", "z", true, 25, 29),
		kvGet(7, "y", "", false, 30, 34),
	}
	res, err := Check(h, NewKVModel(), DefaultOptions())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("checker rejected a linearizable key-value history: %s", res.Explanation)
	}
}

// TestCheckWitnessIsAValidLinearization replays the returned order through the
// model. A witness that does not replay is worse than no witness, because it
// would be used to explain a run that the checker got wrong.
func TestCheckWitnessIsAValidLinearization(t *testing.T) {
	h := History{
		kvPut(1, "x", "a", 0, 20),
		kvGet(3, "x", "a", true, 1, 21),
		kvAppend(5, "y", "q", 0, 5),
		kvGet(6, "y", "q", true, 6, 9),
		kvPut(2, "x", "b", 22, 40),
		kvGet(4, "x", "b", true, 23, 41),
	}
	res, err := Check(h, NewKVModel(), DefaultOptions())
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !res.Linearizable {
		t.Fatalf("checker rejected a linearizable history: %s", res.Explanation)
	}
	if len(res.Witness) != len(h) {
		t.Fatalf("witness has %d entries, history has %d", len(res.Witness), len(h))
	}
	seen := map[int]bool{}
	m := NewKVModel()
	for _, idx := range res.Witness {
		if idx < 0 || idx >= len(h) {
			t.Fatalf("witness names operation %d, which does not exist", idx)
		}
		if seen[idx] {
			t.Fatalf("witness names operation %d twice", idx)
		}
		seen[idx] = true
		next, ok := m.Apply(h[idx])
		if !ok {
			t.Fatalf("witness step %v does not apply in sequence", h[idx])
		}
		m = next
	}
	// The witness must also respect the real-time order: if the operation the
	// witness placed second had already completed before the one it placed
	// first was invoked, the two are in the wrong order.
	for a := range res.Witness {
		for b := a + 1; b < len(res.Witness); b++ {
			x, y := h[res.Witness[a]], h[res.Witness[b]]
			if y.Complete < x.Invoke {
				t.Fatalf("witness places %v before %v even though the second completed at %d and the first was invoked at %d",
					x, y, y.Complete, x.Invoke)
			}
		}
	}
}

func TestCheckRespectsBudget(t *testing.T) {
	// Forty mutually concurrent writes: every prefix of every permutation is a
	// legal linearization, so the search has to try at least one candidate per
	// level and a small budget runs out before it finishes.
	var h History
	for i := 0; i < 40; i++ {
		h = append(h, regWrite(ClientID(i), fmt.Sprintf("v%d", i), 0, 1000))
	}
	res, err := Check(h, NewRegisterModel("0"), Options{MaxSteps: 20})
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Check returned (%v, %v), want ErrBudgetExhausted", res.Linearizable, err)
	}
	if res.Linearizable {
		t.Error("an inconclusive result was reported as linearizable")
	}
	if res.Steps <= 20 {
		t.Errorf("steps = %d, want more than the budget of 20", res.Steps)
	}
}

func TestCheckEmptyHistory(t *testing.T) {
	res, err := Check(nil, NewKVModel(), DefaultOptions())
	if err != nil {
		t.Fatalf("Check(nil): %v", err)
	}
	if !res.Linearizable || len(res.Witness) != 0 || res.Steps != 0 {
		t.Fatalf("empty history produced %+v", res)
	}
}

func TestCheckRejectsMalformedHistory(t *testing.T) {
	overlapping := History{
		regWrite(1, "1", 0, 10),
		regRead(1, "1", 5, 20),
	}
	if _, err := Check(overlapping, NewRegisterModel("0"), DefaultOptions()); err == nil {
		t.Error("Check accepted two overlapping operations from one client")
	}
	backwards := History{regWrite(1, "1", 10, 0)}
	if _, err := Check(backwards, NewRegisterModel("0"), DefaultOptions()); err == nil {
		t.Error("Check accepted an operation that completes before it is invoked")
	}
	outOfOrder := History{
		regWrite(1, "1", 20, 30),
		regWrite(1, "2", 0, 10),
	}
	if _, err := Check(outOfOrder, NewRegisterModel("0"), DefaultOptions()); err == nil {
		t.Error("Check accepted a client's operations in the wrong issue order")
	}
}

func TestHistoryValidate(t *testing.T) {
	if err := (History{}).Validate(); err != nil {
		t.Fatalf("empty history: %v", err)
	}
	ok := History{regWrite(1, "a", 0, 1), regWrite(1, "b", 1, 2), regRead(2, "b", 0, 5)}
	if err := ok.Validate(); err != nil {
		t.Fatalf("well-formed history rejected: %v", err)
	}
}

// TestCheckRandomizedSequentialNeverRejected builds histories by executing
// operations sequentially and then assigning itervals that keep that order
// real-time consistent but allow overlap between clients. The execution order
// is a linearization by construction, so a rejection is always a false
// negative.
func TestCheckRandomizedSequentialNeverRejected(t *testing.T) {
	const clients = 4
	const keys = 3
	for seed := int64(0); seed < 200; seed++ {
		h := generateHistory(t, seed, clients, keys, 12)
		res, err := Check(h, NewKVModel(), Options{MaxSteps: 400000, Memoize: true})
		if err != nil {
			t.Fatalf("seed %d: %v (history %v)", seed, err, h)
		}
		if !res.Linearizable {
			t.Fatalf("seed %d: a history built from a real sequential execution was rejected:\n%s\n%v",
				seed, res.Explanation, h)
		}
	}
}

// generateHistory executes n operations against a concrete map in a chosen
// order and then assigns invocation and completion times that (a) keep each
// client's operations sequential, (b) make invocation times monotonically
// non-decreasing along the execution order, which is exactly the condition for
// the execution order to be real-time consistent, and (c) let neighbouring
// operations from different clients overlap.
func generateHistory(t *testing.T, seed int64, clients, keys, n int) History {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	// A deliberate mix: enough reads that the checker has to reason about the
	// order of writes, enough writes that many orders are plausible.
	kind := func() Kind {
		switch rng.Intn(10) {
		case 0, 1, 2, 3:
			return KindPut
		case 4, 5, 6:
			return KindAppend
		default:
			return KindGet
		}
	}
	key := func() string { return fmt.Sprintf("k%d", rng.Intn(keys)) }

	data := map[string]string{}
	var h History
	lastComplete := map[ClientID]int64{}
	prevInvoke := int64(0)
	for i := 0; i < n; i++ {
		c := ClientID(rng.Intn(clients))
		k := key()
		op := Operation{Client: c}
		switch kind() {
		case KindGet:
			v, found := data[k]
			op.Input = Input{Kind: KindGet, Key: k}
			op.Output = Output{Val: v, Found: found}
		case KindPut:
			v := fmt.Sprintf("v%d", rng.Intn(6))
			data[k] = v
			op.Input = Input{Kind: KindPut, Key: k, Val: v}
			op.Output = Output{}
		case KindAppend:
			v := fmt.Sprintf("+%d", rng.Intn(6))
			data[k] += v
			op.Input = Input{Kind: KindAppend, Key: k, Val: v}
			op.Output = Output{}
		}
		start := prevInvoke
		if last := lastComplete[c]; last > start {
			start = last
		}
		if i > 0 && rng.Intn(2) == 0 {
			prev := h[i-1]
			if prev.Client != c && prev.Complete > prev.Invoke {
				overlap := prev.Invoke + rng.Int63n(prev.Complete-prev.Invoke)
				if overlap > start {
					start = overlap
				}
			}
		}
		dur := 1 + rng.Int63n(8)
		op.Invoke, op.Complete = start, start+dur
		lastComplete[c] = op.Complete
		prevInvoke = start
		h = append(h, op)
	}
	if err := h.Validate(); err != nil {
		t.Fatalf("seed %d: generated history is malformed: %v", seed, err)
	}
	return h
}
