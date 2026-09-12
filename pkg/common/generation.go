package common

import (
	"fmt"
	"time"
)

// Incarnation represents the upper 64 bits of a 128-bit Generation.
// It tracks cluster/store incarnation lifecycles to guarantee strictly increasing generations
// across cluster restarts or storage layer replacements.
// Ported directly from com.databricks.dicer.common.Incarnation in Scala.
type Incarnation struct {
	value uint64
}

// UpperBoundFatFingerGuardrail prevents accidental overflow. (5L << 48 in Scala).
const UpperBoundFatFingerGuardrail uint64 = 5 << 48

// MinIncarnation is the lowest legal Incarnation.
var MinIncarnation = Incarnation{value: 0}

// NewIncarnation constructs and validates an Incarnation.
func NewIncarnation(value uint64) (Incarnation, error) {
	if value >= UpperBoundFatFingerGuardrail {
		return Incarnation{}, fmt.Errorf("incarnation value is too large (%d >= %d)", value, UpperBoundFatFingerGuardrail)
	}
	return Incarnation{value: value}, nil
}

// MustNewIncarnation constructs an Incarnation or panics.
func MustNewIncarnation(value uint64) Incarnation {
	inc, err := NewIncarnation(value)
	if err != nil {
		panic(err)
	}
	return inc
}

// Value returns the raw uint64 value.
func (inc Incarnation) Value() uint64 {
	return inc.value
}

// Compare compares two Incarnations.
func (inc Incarnation) Compare(other Incarnation) int {
	if inc.value < other.value {
		return -1
	} else if inc.value > other.value {
		return 1
	}
	return 0
}

func (inc Incarnation) String() string {
	return fmt.Sprintf("%d", inc.value)
}

// Generation represents a 128-bit monotonically increasing version number comprising
// an Incarnation (most significant bits) and a UnixTimeVersion number (least significant bits).
// Ported directly from com.databricks.dicer.common.Generation in Scala.
type Generation struct {
	Incarnation Incarnation
	Number      uint64 // Unix time in milliseconds or monotonic sequence number
}

// EmptyGeneration represents an uninitialized or empty generation sentinel.
var EmptyGeneration = Generation{Incarnation: MinIncarnation, Number: 0}

// NewGeneration constructs a Generation.
func NewGeneration(inc Incarnation, number uint64) Generation {
	return Generation{
		Incarnation: inc,
		Number:      number,
	}
}

// NewGenerationFromTime constructs a Generation using the current UTC millisecond timestamp.
func NewGenerationFromTime(inc Incarnation, t time.Time) Generation {
	millis := uint64(t.UnixMilli())
	return Generation{
		Incarnation: inc,
		Number:      millis,
	}
}

// Compare compares two Generations by Incarnation first, then Number.
func (g Generation) Compare(other Generation) int {
	incCmp := g.Incarnation.Compare(other.Incarnation)
	if incCmp != 0 {
		return incCmp
	}
	if g.Number < other.Number {
		return -1
	} else if g.Number > other.Number {
		return 1
	}
	return 0
}

// Less returns true if g < other.
func (g Generation) Less(other Generation) bool {
	return g.Compare(other) < 0
}

// Equal returns true if g == other.
func (g Generation) Equal(other Generation) bool {
	return g.Compare(other) == 0
}

// IsEmpty checks if generation is empty.
func (g Generation) IsEmpty() bool {
	return g.Equal(EmptyGeneration)
}

func (g Generation) String() string {
	return fmt.Sprintf("%s#%d", g.Incarnation, g.Number)
}
