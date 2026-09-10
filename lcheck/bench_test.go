package lcheck

import "testing"

// BenchmarkCheck measures the Wing & Gong search on histories generated the way
// the simulator generates them: a real sequential execution with random
// overlapping intervals, so a linearization provably exists and a rejection
// would be a false negative.
//
// History length is the parameter that matters. The search is exponential in the
// worst case, which is why the simulator's histories are bounded.
func BenchmarkCheck(b *testing.B) {
	for _, n := range []int{8, 16, 24, 32} {
		b.Run("kv/ops="+itoa(n), func(b *testing.B) {
			h := benchHistory(n, 4, 3)
			opts := Options{MaxSteps: 5_000_000, Memoize: true}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := Check(h, NewKVModel(), opts)
				if err != nil {
					b.Fatal(err)
				}
				if !res.Linearizable {
					b.Fatalf("generated history rejected: %s", res.Explanation)
				}
				b.ReportMetric(float64(res.Steps), "search-steps")
			}
		})
	}
}

// BenchmarkCheckRegister measures the smallest useful specification, which is
// the fastest way to see how the search scales with concurrency alone.
func BenchmarkCheckRegister(b *testing.B) {
	for _, n := range []int{8, 16, 24} {
		b.Run("register/ops="+itoa(n), func(b *testing.B) {
			h := benchRegisterHistory(n, 4)
			opts := Options{MaxSteps: 5_000_000, Memoize: true}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := Check(h, NewRegisterModel("0"), opts)
				if err != nil {
					b.Fatal(err)
				}
				if !res.Linearizable {
					b.Fatalf("generated history rejected: %s", res.Explanation)
				}
			}
		})
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

type benchRand struct{ state uint64 }

func newBenchRand(seed uint64) *benchRand { return &benchRand{state: seed*6364136223846793005 + 1} }

func (r *benchRand) next() uint64 {
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	return r.state
}
func (r *benchRand) intn(n int) int       { return int(r.next() % uint64(n)) }
func (r *benchRand) int63n(n int64) int64 { return int64(r.next() % uint64(n)) }

func benchHistory(n, clients, keys int) History {
	r := newBenchRand(20241007)
	data := map[string]string{}
	var h History
	last := map[ClientID]int64{}
	prevInvoke := int64(0)
	for i := 0; i < n; i++ {
		c := ClientID(r.intn(clients))
		k := string(rune('a' + r.intn(keys)))
		op := Operation{Client: c}
		switch r.intn(10) {
		case 0, 1, 2, 3:
			v := string(rune('0' + r.intn(6)))
			data[k] = v
			op.Input = Input{Kind: KindPut, Key: k, Val: v}
			op.Output = Output{}
		case 4, 5, 6:
			v := string(rune('0' + r.intn(6)))
			data[k] += v
			op.Input = Input{Kind: KindAppend, Key: k, Val: v}
			op.Output = Output{}
		default:
			v, ok := data[k]
			op.Input = Input{Kind: KindGet, Key: k}
			op.Output = Output{Val: v, Found: ok}
		}
		start := prevInvoke
		if l := last[c]; l > start {
			start = l
		}
		if i > 0 && r.intn(2) == 0 {
			prev := h[i-1]
			if prev.Client != c && prev.Complete > prev.Invoke {
				if o := prev.Invoke + r.int63n(prev.Complete-prev.Invoke); o > start {
					start = o
				}
			}
		}
		op.Invoke, op.Complete = start, start+1+r.int63n(8)
		last[c] = op.Complete
		prevInvoke = start
		h = append(h, op)
	}
	return h
}

func benchRegisterHistory(n, clients int) History {
	r := newBenchRand(99)
	val := "0"
	var h History
	last := map[ClientID]int64{}
	prevInvoke := int64(0)
	for i := 0; i < n; i++ {
		c := ClientID(r.intn(clients))
		op := Operation{Client: c}
		if r.intn(2) == 0 {
			val = string(rune('0' + r.intn(6)))
			op.Input = RegisterInput{Write: true, Val: val}
			op.Output = RegisterOutput{}
		} else {
			op.Input = RegisterInput{Write: false}
			op.Output = RegisterOutput{Val: val}
		}
		start := prevInvoke
		if l := last[c]; l > start {
			start = l
		}
		op.Invoke, op.Complete = start, start+1+r.int63n(8)
		last[c] = op.Complete
		prevInvoke = start
		h = append(h, op)
	}
	return h
}
