package lcheck

import "testing"

func TestKVModelHashIsStable(t *testing.T) {
	a := &kvModel{data: map[string]string{}}
	a.data["x"] = "1"
	a.data["y"] = "2"
	a.data["z"] = "3"

	b := &kvModel{data: map[string]string{}}
	b.data["z"] = "3"
	b.data["x"] = "1"
	b.data["y"] = "2"

	if a.Hash() != b.Hash() {
		t.Fatalf("hash depends on insertion order: %d vs %d", a.Hash(), b.Hash())
	}
	if a.String() != b.String() {
		t.Fatalf("String depends on insertion order: %q vs %q", a.String(), b.String())
	}

	c := &kvModel{data: map[string]string{}}
	c.data["x"] = "1"
	c.data["y"] = "2"
	c.data["z"] = "4"
	if a.Hash() == c.Hash() {
		t.Error("distinct states share a hash, which would let the memo table prune a live branch")
	}
}

func TestKVModelApplyIsPure(t *testing.T) {
	base := &kvModel{data: map[string]string{"x": "1"}}
	before := base.String()
	next, ok := base.Apply(kvPut(1, "x", "2", 0, 1))
	if !ok {
		t.Fatal("Put was rejected on an empty model")
	}
	if base.String() != before {
		t.Fatalf("Apply mutated the receiver: %q became %q", before, base.String())
	}
	if next.String() == before {
		t.Fatal("Apply returned a successor identical to the receiver")
	}
	// A read shares the receiver; it must still not be mutable through Apply.
	if _, ok := base.Apply(kvGet(1, "x", "1", true, 0, 1)); !ok {
		t.Fatal("Get with the correct output was rejected")
	}
	if base.String() != before {
		t.Fatal("a read mutated the model")
	}
}

func TestKVModelSemantics(t *testing.T) {
	m := NewKVModel()

	// Appending to a missing key starts from the empty string.
	next, ok := m.Apply(kvAppend(1, "k", "a", 0, 1))
	if !ok {
		t.Fatal("Append to a missing key was rejected")
	}
	next2, ok := next.Apply(kvAppend(1, "k", "b", 2, 3))
	if !ok {
		t.Fatal("second Append was rejected")
	}
	if got := next2.String(); got != `{k="ab"}` {
		t.Fatalf("state = %s, want {k=\"ab\"}", got)
	}

	if _, ok := next2.Apply(kvGet(2, "k", "ab", true, 4, 5)); !ok {
		t.Error("Get of the correct value was rejected")
	}
	if _, ok := next2.Apply(kvGet(2, "k", "a", true, 4, 5)); ok {
		t.Error("Get with the wrong value was accepted")
	}
	if _, ok := next2.Apply(kvGet(2, "k", "", false, 4, 5)); ok {
		t.Error("Get reporting a present key as absent was accepted")
	}
	if _, ok := next2.Apply(kvGet(2, "missing", "", true, 4, 5)); ok {
		t.Error("Get reporting an absent key as present was accepted")
	}

	// Overwriting a key through Put must be visible to a later Get.
	next3, ok := next2.Apply(kvPut(3, "k", "z", 6, 7))
	if !ok {
		t.Fatal("Put was rejected")
	}
	if _, ok := next3.Apply(kvGet(4, "k", "z", true, 8, 9)); !ok {
		t.Error("Get did not observe the Put")
	}
}

func TestModelsRejectUnknownVocabulary(t *testing.T) {
	kv := NewKVModel()
	if _, ok := kv.Apply(Operation{Client: 1, Input: RegisterInput{Write: true}, Output: RegisterOutput{}}); ok {
		t.Error("the key-value model accepted a register input")
	}
	if _, ok := kv.Apply(Operation{Client: 1, Input: Input{Kind: KindPut}, Output: "not an Output"}); ok {
		t.Error("the key-value model accepted a foreign output type")
	}
	if _, ok := kv.Apply(Operation{Client: 1, Input: Input{Kind: Kind(99)}, Output: Output{}}); ok {
		t.Error("the key-value model accepted an unknown operation kind")
	}
	if _, ok := kv.Apply(Operation{Client: 1, Input: Input{Kind: KindGet, Key: "x"}, Output: Output{Err: "boom"}}); ok {
		t.Error("the key-value model accepted an operation carrying an error")
	}

	reg := NewRegisterModel("0")
	if _, ok := reg.Apply(Operation{Client: 1, Input: Input{Kind: KindGet}, Output: Output{}}); ok {
		t.Error("the register model accepted a key-value input")
	}
	if _, ok := reg.Apply(Operation{Client: 1, Input: RegisterInput{}, Output: "not a RegisterOutput"}); ok {
		t.Error("the register model accepted a foreign output type")
	}
}

func TestRegisterModelSemantics(t *testing.T) {
	m := NewRegisterModel("0")
	if _, ok := m.Apply(regRead(1, "0", 0, 1)); !ok {
		t.Error("reading the initial value was rejected")
	}
	if _, ok := m.Apply(regRead(1, "1", 0, 1)); ok {
		t.Error("reading a value that was never written was accepted")
	}
	next, ok := m.Apply(regWrite(1, "1", 2, 3))
	if !ok {
		t.Fatal("write was rejected")
	}
	if _, ok := next.Apply(regRead(2, "1", 4, 5)); !ok {
		t.Error("read did not observe the write")
	}
	if _, ok := next.Apply(regRead(2, "0", 4, 5)); ok {
		t.Error("read observed the pre-write value after the write completed")
	}
}
