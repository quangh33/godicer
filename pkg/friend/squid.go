package friend

import (
	"fmt"
	"strings"
	"time"
)

// Squid represents a Slicelet Incarnation UniQUe ID (SQUID).
// It uniquely identifies a specific running instance (incarnation) of a server pod,
// ensuring that a newly restarted pod reusing the same IP address is not confused
// with its predecessor.
// Ported directly from com.databricks.dicer.friend.Squid in Scala.
type Squid struct {
	ResourceAddress    string // Address used to route requests (e.g., "10.0.0.1:50051")
	CreationTimeMillis int64  // Creation time in milliseconds since Unix epoch
	ResourceUUID       string // Globally unique ID (e.g. K8s Pod UID)
}

// NewSquid constructs a Squid.
func NewSquid(address string, creationTime time.Time, uuid string) Squid {
	return Squid{
		ResourceAddress:    address,
		CreationTimeMillis: creationTime.UnixMilli(),
		ResourceUUID:       uuid,
	}
}

// CreationTime returns the creation time as a time.Time in UTC.
func (sq Squid) CreationTime() time.Time {
	return time.UnixMilli(sq.CreationTimeMillis).UTC()
}

// Compare compares two Squids by ResourceAddress, CreationTimeMillis, then ResourceUUID.
// Equivalent to Squid.ORDERING in Scala.
func (sq Squid) Compare(other Squid) int {
	addrCmp := strings.Compare(sq.ResourceAddress, other.ResourceAddress)
	if addrCmp != 0 {
		return addrCmp
	}
	if sq.CreationTimeMillis < other.CreationTimeMillis {
		return -1
	} else if sq.CreationTimeMillis > other.CreationTimeMillis {
		return 1
	}
	return strings.Compare(sq.ResourceUUID, other.ResourceUUID)
}

// Equal checks for exact equality between two Squids.
func (sq Squid) Equal(other Squid) bool {
	return sq.ResourceAddress == other.ResourceAddress &&
		sq.CreationTimeMillis == other.CreationTimeMillis &&
		sq.ResourceUUID == other.ResourceUUID
}

func (sq Squid) String() string {
	return fmt.Sprintf("[%s %s %s]", sq.ResourceAddress, sq.CreationTime().Format(time.RFC3339Nano), sq.ResourceUUID)
}
