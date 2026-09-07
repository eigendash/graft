package lcheck

import "fmt"

// ClientID identifies the process that issued an operation. Operations issued
// by one client are sequential: a client that is waiting for a response does
// not start another operation.
type ClientID int

// Kind enumerates the operations the key-value model understands.
type Kind uint8

const (
	KindGet Kind = iota
	KindPut
	KindAppend
)

func (k Kind) String() string {
	switch k {
	case KindGet:
		return "get"
	case KindPut:
		return "put"
	case KindAppend:
		return "append"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// Input is the argument vocabulary for the key-value model. It also serves as
// the generic input slot for models with a different vocabulary, which is why
// Operation.Input is an interface.
type Input struct {
	Kind Kind
	Key  string
	Val  string
}

func (in Input) String() string {
	if in.Kind == KindGet {
		return fmt.Sprintf("%s(%s)", in.Kind, in.Key)
	}
	return fmt.Sprintf("%s(%s,%q)", in.Kind, in.Key, in.Val)
}

// Output is the result vocabulary for the key-value model. Err is empty for an
// operation that completed; the recorder never records operations that failed,
// because an operation whose outcome is unknown carries no constraint.
type Output struct {
	Val   string
	Found bool
	Err   string
}

func (out Output) String() string {
	if !out.Found {
		return "<absent>"
	}
	return fmt.Sprintf("%q", out.Val)
}

// Operation is one invocation/response pair. Invoke and Complete are logical
// timestamps, and the operation is considered pending over the half-open
// interval [Invoke, Complete).
type Operation struct {
	Client   ClientID
	Input    any
	Output   any
	Invoke   int64
	Complete int64
}

func (op Operation) String() string {
	return fmt.Sprintf("c%d %v -> %v [%d,%d]", op.Client, op.Input, op.Output, op.Invoke, op.Complete)
}

// History is the sequence of operations observed by the recorder.
type History []Operation

// Len returns the number of operations.
func (h History) Len() int { return len(h) }

// Validate reports whether the history is well formed. A malformed history
// would make the checker's answer meaningless, so Check refuses to guess.
func (h History) Validate() error {
	lastEnd := map[ClientID]int64{}
	lastSeen := map[ClientID]int{}
	for i, op := range h {
		if op.Invoke > op.Complete {
			return fmt.Errorf("lcheck: operation %d (client %d) is invoked at %d and completes at %d",
				i, op.Client, op.Invoke, op.Complete)
		}
		if end, ok := lastEnd[op.Client]; ok {
			if op.Invoke < end {
				return fmt.Errorf("lcheck: client %d overlaps: operation %d starts at %d but operation %d runs until %d",
					op.Client, i, op.Invoke, lastSeen[op.Client], end)
			}
			if op.Invoke < h[lastSeen[op.Client]].Invoke {
				return fmt.Errorf("lcheck: client %d issued operation %d before operation %d",
					op.Client, i, lastSeen[op.Client])
			}
		}
		lastEnd[op.Client] = op.Complete
		lastSeen[op.Client] = i
	}
	return nil
}

// SortByInvoke returns a copy ordered by invocation time. The checker does not
// require it, but failure messages are far easier to read in real-time order.
func (h History) SortByInvoke() History {
	out := append(History(nil), h...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Invoke < out[j-1].Invoke; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
