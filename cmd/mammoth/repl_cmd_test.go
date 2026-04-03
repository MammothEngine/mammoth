package main

import (
	"strings"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestValueToString(t *testing.T) {
	tests := []struct {
		name     string
		value    bson.Value
		expected string
	}{
		{"string", bson.VString("hello"), `"hello"`},
		{"int32", bson.VInt32(42), `42`},
		{"int64", bson.VInt64(123456789), `123456789`},
		{"double", bson.VDouble(3.14), `3.140000`},
		{"true", bson.VBool(true), `true`},
		{"false", bson.VBool(false), `false`},
		{"null", bson.VNull(), `null`},
		{"objectid", bson.VObjectID(bson.NewObjectID()), `ObjectId(`},
		{"datetime", bson.VDateTime(1234567890), `Date(1234567890)`},
		{"document", bson.VDoc(bson.NewDocument()), `{...}`},
		{"array", bson.VArray(bson.Array{bson.VInt32(1), bson.VInt32(2)}), `[1, 2]`},
		{"default", bson.VBinary(0, []byte{1, 2, 3}), `<5>`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := valueToString(tc.value)
			// For ObjectId, just check it contains the prefix
			if tc.name == "objectid" {
				if len(got) < 10 || !strings.Contains(got, "ObjectId") {
					t.Errorf("valueToString() = %q, want prefix %s", got, tc.expected)
				}
				return
			}
			if got != tc.expected {
				t.Errorf("valueToString() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestTokenizeREPL(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{}},
		{"   ", []string{}},
		{"use test", []string{"use", "test"}},
		{"show collections", []string{"show", "collections"}},
		{"db.users.find()", []string{"db.users.find()"}},
		{"  db.users.find(  )  ", []string{"db.users.find(", ")"}},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := tokenizeREPL(tc.input)
			if len(got) != len(tc.expected) {
				t.Errorf("tokenizeREPL(%q) = %v, want %v", tc.input, got, tc.expected)
				return
			}
			for i := range got {
				if got[i] != tc.expected[i] {
					t.Errorf("tokenizeREPL(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.expected[i])
				}
			}
		})
	}
}
