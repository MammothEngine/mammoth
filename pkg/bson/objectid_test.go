package bson

import (
	"testing"
	"time"
)

func TestObjectIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 10000; i++ {
		id := NewObjectID()
		s := id.String()
		if ids[s] {
			t.Fatalf("duplicate ObjectID: %s", s)
		}
		ids[s] = true
	}
}

func TestObjectIDTimestamp(t *testing.T) {
	before := time.Now().Truncate(time.Second)
	id := NewObjectID()
	after := time.Now().Truncate(time.Second).Add(time.Second)

	ts := id.Timestamp()
	if ts.Before(before) || ts.After(after) {
		t.Fatalf("timestamp %v not between %v and %v", ts, before, after)
	}
}

func TestObjectIDFromTimestamp(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	id := ObjectIDFromTimestamp(now)
	if !id.Timestamp().Equal(now) {
		t.Fatalf("expected %v, got %v", now, id.Timestamp())
	}
}

func TestObjectIDStringRoundTrip(t *testing.T) {
	id := NewObjectID()
	s := id.String()
	parsed, err := ParseObjectID(s)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if !id.Equal(parsed) {
		t.Fatalf("expected %v, got %v", id, parsed)
	}
}

func TestObjectIDIsZero(t *testing.T) {
	var zero ObjectID
	if !zero.IsZero() {
		t.Fatal("expected zero ObjectID to be zero")
	}
	id := NewObjectID()
	if id.IsZero() {
		t.Fatal("expected new ObjectID to be non-zero")
	}
}

func TestParseObjectIDInvalid(t *testing.T) {
	_, err := ParseObjectID("abc")
	if err == nil {
		t.Fatal("expected error for short string")
	}
	_, err = ParseObjectID("zzzzzzzzzzzzzzzzzzzzzzzz")
	if err == nil {
		t.Fatal("expected error for invalid hex")
	}
}

func TestObjectIDBytes(t *testing.T) {
	id := NewObjectID()
	b := id.Bytes()
	if len(b) != 12 {
		t.Fatalf("expected 12 bytes, got %d", len(b))
	}
}

func TestObjectIDMarshalHex(t *testing.T) {
	id := NewObjectID()
	hex := id.MarshalHex()
	if len(hex) != 24 {
		t.Fatalf("expected 24 character hex string, got %d", len(hex))
	}
	// Verify it's valid hex
	parsed, err := ParseObjectID(hex)
	if err != nil {
		t.Fatalf("MarshalHex produced invalid hex: %v", err)
	}
	if !id.Equal(parsed) {
		t.Fatal("MarshalHex round-trip failed")
	}
}

func TestMustParseObjectID(t *testing.T) {
	id := NewObjectID()
	s := id.String()

	parsed := MustParseObjectID(s)
	if !id.Equal(parsed) {
		t.Fatal("MustParseObjectID failed for valid ID")
	}

	// Test panic on invalid
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("MustParseObjectID should panic on invalid input")
		}
	}()
	MustParseObjectID("invalid")
}

func TestObjectIDMarshalBinary(t *testing.T) {
	id := NewObjectID()
	data, err := id.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}
	if len(data) != 12 {
		t.Fatalf("expected 12 bytes, got %d", len(data))
	}

	// Verify round-trip
	var parsed ObjectID
	if err := parsed.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}
	if !id.Equal(parsed) {
		t.Fatal("Binary round-trip failed")
	}
}

func TestObjectIDUnmarshalBinaryInvalid(t *testing.T) {
	var id ObjectID
	// Wrong length
	if err := id.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error for wrong length")
	}
	// Correct length
	data := make([]byte, 12)
	copy(data, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	if err := id.UnmarshalBinary(data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
