package bson

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// ObjectID is a 12-byte unique identifier.
// Layout: 4-byte timestamp + 5-byte random + 3-byte counter.
type ObjectID [12]byte

var (
	objectidCounter uint32
	objectidRandom  [5]byte
)

func init() {
	// Initialize random bytes
	_, _ = rand.Read(objectidRandom[:])
	// Initialize counter with random value
	var buf [4]byte
	_, _ = rand.Read(buf[:])
	objectidCounter = binary.BigEndian.Uint32(buf[:])
}

// NewObjectID generates a new ObjectID.
func NewObjectID() ObjectID {
	var id ObjectID

	// 4-byte timestamp (seconds since epoch)
	ts := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(id[0:4], ts)

	// 5-byte random
	copy(id[4:9], objectidRandom[:])

	// 3-byte counter
	c := atomic.AddUint32(&objectidCounter, 1)
	id[9] = byte(c >> 16)
	id[10] = byte(c >> 8)
	id[11] = byte(c)

	return id
}

// ObjectIDFromTimestamp creates an ObjectID with the given timestamp.
func ObjectIDFromTimestamp(t time.Time) ObjectID {
	var id ObjectID
	binary.BigEndian.PutUint32(id[0:4], uint32(t.Unix()))
	return id
}

// Timestamp returns the timestamp portion of the ObjectID.
func (id ObjectID) Timestamp() time.Time {
	ts := binary.BigEndian.Uint32(id[0:4])
	return time.Unix(int64(ts), 0)
}

// String returns the hex encoding of the ObjectID.
func (id ObjectID) String() string {
	return hex.EncodeToString(id[:])
}

// IsZero returns true if the ObjectID is all zeros.
func (id ObjectID) IsZero() bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}

// MarshalHex returns the hex encoding.
func (id ObjectID) MarshalHex() string {
	return hex.EncodeToString(id[:])
}

// ParseObjectID parses a 24-character hex string into an ObjectID.
func ParseObjectID(s string) (ObjectID, error) {
	var id ObjectID
	if len(s) != 24 {
		return id, fmt.Errorf("invalid ObjectID length: %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("invalid ObjectID hex: %w", err)
	}
	copy(id[:], b)
	return id, nil
}

// MustParseObjectID parses or panics.
func MustParseObjectID(s string) ObjectID {
	id, err := ParseObjectID(s)
	if err != nil {
		panic(err)
	}
	return id
}

// Equal returns true if two ObjectIDs are equal.
func (id ObjectID) Equal(other ObjectID) bool {
	return id == other
}

// Bytes returns the raw bytes.
func (id ObjectID) Bytes() []byte {
	return id[:]
}

var errInvalidObjectID = errors.New("invalid ObjectID")

// MarshalBinary implements encoding.BinaryMarshaler.
func (id ObjectID) MarshalBinary() ([]byte, error) {
	b := make([]byte, 12)
	copy(b, id[:])
	return b, nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (id *ObjectID) UnmarshalBinary(data []byte) error {
	if len(data) != 12 {
		return errInvalidObjectID
	}
	copy(id[:], data)
	return nil
}
