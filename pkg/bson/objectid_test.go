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
