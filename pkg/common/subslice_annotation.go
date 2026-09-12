package common

import (
	"fmt"

	"github.com/quangh33/godicer/pkg/friend"
)

// Transfer represents state transfer metadata for a migrating slice.
// Ported from com.databricks.api.proto.dicer.common.DiffAssignmentP.TransferP in Scala.
type Transfer struct {
	FromResource friend.Squid // The source server pod incarnation (Squid) transferring state
}

// SubsliceAnnotation represents fine-grained metadata for an ordered, disjoint subslice
// that has remained assigned to a specific resource without interruption.
// Ported directly from com.databricks.dicer.common.SubsliceAnnotation in Scala.
type SubsliceAnnotation struct {
	Subslice                   friend.Slice
	ContinuousGenerationNumber uint64    // Generation number since continuous assignment began
	StateTransfer              *Transfer // Optional state transfer provider
}

// NewSubsliceAnnotation constructs a SubsliceAnnotation.
func NewSubsliceAnnotation(subslice friend.Slice, genNumber uint64, transfer *Transfer) SubsliceAnnotation {
	return SubsliceAnnotation{
		Subslice:                   subslice,
		ContinuousGenerationNumber: genNumber,
		StateTransfer:              transfer,
	}
}

func (sa SubsliceAnnotation) String() string {
	res := fmt.Sprintf("%s:%d", sa.Subslice, sa.ContinuousGenerationNumber)
	if sa.StateTransfer != nil {
		res += fmt.Sprintf(", state provider: %s", sa.StateTransfer.FromResource)
	}
	return res
}
