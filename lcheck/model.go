package lcheck

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// Model is a sequential specification.
//
// Apply must not mutate the receiver: it returns a successor state, or
// ok == false when the observed output is impossible in the current state. The
// checker relies on this to explore alternatives without copying the whole
// search state, and on Hash to recognise states it has already refuted.
type Model interface {
	Apply(op Operation) (Model, bool)
	// Hash returns a digest of the state. It is only used to key the memo
	// table, which is always corroborated by String, so an occasional
	// collision costs time rather than correctness.
	Hash() uint64
	// String renders the state, deterministically, for failure messages and
	// for the memo key.
	String() string
}

// --- key-value model ---------------------------------------------------------

// kvModel is a map from string to string with Get, Put and Append.
//
// Reads are modelled as operations that change nothing but are still checked
// against the observed value: a linearizable read must return what the
// sequential specification would return at the point it is linearized.
type kvModel struct {
	data map[string]string
}

// NewKVModel returns an empty key-value model.
func NewKVModel() Model { return &kvModel{data: map[string]string{}} }

func (m *kvModel) clone() *kvModel {
	n := &kvModel{data: make(map[string]string, len(m.data))}
	for k, v := range m.data {
		n.data[k] = v
	}
	return n
}

func (m *kvModel) Apply(op Operation) (Model, bool) {
	in, ok := op.Input.(Input)
	if !ok {
		return nil, false
	}
	out, ok := op.Output.(Output)
	if !ok {
		return nil, false
	}
	if out.Err != "" {
		// A recorded operation always completed; if it carries an error the
		// vocabulary is wrong for this model.
		return nil, false
	}
	switch in.Kind {
	case KindGet:
		v, found := m.data[in.Key]
		if out.Val != v || out.Found != found {
			return nil, false
		}
		// A read has no successor state; sharing the receiver is safe because
		// Apply never mutates it.
		return m, true
	case KindPut:
		next := m.clone()
		next.data[in.Key] = in.Val
		return next, true
	case KindAppend:
		next := m.clone()
		next.data[in.Key] = next.data[in.Key] + in.Val
		return next, true
	default:
		return nil, false
	}
}

func (m *kvModel) Hash() uint64 {
	var keys []string
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(m.data[k]))
		h.Write([]byte{1})
	}
	return h.Sum64()
}

func (m *kvModel) String() string {
	var keys []string
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%q", k, m.data[k])
	}
	b.WriteByte('}')
	return b.String()
}

// --- register model ----------------------------------------------------------

// RegisterInput is the vocabulary for NewRegisterModel.
type RegisterInput struct {
	Write bool
	Val   string
}

// RegisterOutput is the vocabulary for NewRegisterModel.
type RegisterOutput struct {
	Val string
}

// registerModel is a single read/write register. It exists alongside the
// key-value model because it is the smallest specification for which a
// linearizability checker can be hand-checked, which makes it the right place
// to prove the checker can actually reject things.
type registerModel struct {
	val string
}

// NewRegisterModel returns a register initialised to initial.
func NewRegisterModel(initial string) Model { return &registerModel{val: initial} }

func (m *registerModel) Apply(op Operation) (Model, bool) {
	in, ok := op.Input.(RegisterInput)
	if !ok {
		return nil, false
	}
	out, ok := op.Output.(RegisterOutput)
	if !ok {
		return nil, false
	}
	if in.Write {
		return &registerModel{val: in.Val}, true
	}
	if out.Val != m.val {
		return nil, false
	}
	return m, true
}

func (m *registerModel) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(m.val))
	return h.Sum64()
}

func (m *registerModel) String() string { return fmt.Sprintf("%q", m.val) }
