package main

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestParseDocument(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		check   func(*bson.Document) bool
	}{
		{
			name:    "empty document",
			input:   "{}",
			wantErr: false,
			check:   func(d *bson.Document) bool { return d.Len() == 0 },
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: false,
			check:   func(d *bson.Document) bool { return d.Len() == 0 },
		},
		{
			name:    "simple string field",
			input:   `{name: "test"}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("name")
				return ok && v.Type == bson.TypeString && v.String() == "test"
			},
		},
		{
			name:    "integer field",
			input:   `{age: 42}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("age")
				return ok && v.Type == bson.TypeInt32 && v.Int32() == 42
			},
		},
		{
			name:    "negative integer",
			input:   `{temp: -10}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("temp")
				return ok && v.Type == bson.TypeInt32 && v.Int32() == -10
			},
		},
		{
			name:    "float field",
			input:   `{pi: 3.14}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("pi")
				return ok && v.Type == bson.TypeDouble && v.Double() == 3.14
			},
		},
		{
			name:    "boolean true",
			input:   `{active: true}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("active")
				return ok && v.Type == bson.TypeBoolean && v.Boolean() == true
			},
		},
		{
			name:    "boolean false",
			input:   `{active: false}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("active")
				return ok && v.Type == bson.TypeBoolean && v.Boolean() == false
			},
		},
		{
			name:    "null value",
			input:   `{data: null}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("data")
				return ok && v.Type == bson.TypeNull
			},
		},
		{
			name:    "multiple fields",
			input:   `{name: "test", age: 30}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				name, ok1 := d.Get("name")
				age, ok2 := d.Get("age")
				return ok1 && ok2 && name.String() == "test" && age.Int32() == 30
			},
		},
		{
			name:    "nested document",
			input:   `{user: {name: "test"}}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("user")
				if !ok || v.Type != bson.TypeDocument {
					return false
				}
				inner, _ := v.DocumentValue().Get("name")
				return inner.String() == "test"
			},
		},
		{
			name:    "array field",
			input:   `{tags: [1, 2, 3]}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("tags")
				return ok && v.Type == bson.TypeArray && len(v.ArrayValue()) == 3
			},
		},
		{
			name:    "missing braces",
			input:   `name: "test"`,
			wantErr: true,
		},
		{
			name:    "unterminated string",
			input:   `{name: "test}`,
			wantErr: true,
		},
		{
			name:    "dollar operator",
			input:   `{$gt: 5}`,
			wantErr: false,
			check: func(d *bson.Document) bool {
				v, ok := d.Get("$gt")
				return ok && v.Type == bson.TypeInt32 && v.Int32() == 5
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := parseDocument(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseDocument(%q) expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseDocument(%q) unexpected error: %v", tc.input, err)
				return
			}
			if tc.check != nil && !tc.check(doc) {
				t.Errorf("parseDocument(%q) check failed", tc.input)
			}
		})
	}
}

func TestParseNumber(t *testing.T) {
	tests := []struct {
		input    string
		wantType bson.BSONType
		wantVal  interface{}
	}{
		{"42", bson.TypeInt32, int32(42)},
		{"0", bson.TypeInt32, int32(0)},
		{"-1", bson.TypeInt32, int32(-1)},
		{"2147483647", bson.TypeInt32, int32(2147483647)},
		{"2147483648", bson.TypeInt64, int64(2147483648)},
		{"-2147483649", bson.TypeInt64, int64(-2147483649)},
		{"3.14", bson.TypeDouble, float64(3.14)},
		{"-0.5", bson.TypeDouble, float64(-0.5)},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			val, _, err := parseNumber(tc.input, 0)
			if err != nil {
				t.Fatalf("parseNumber(%q) error: %v", tc.input, err)
			}
			if val.Type != tc.wantType {
				t.Errorf("parseNumber(%q) type = %d, want %d", tc.input, val.Type, tc.wantType)
			}
		})
	}
}

func TestSkipWS(t *testing.T) {
	tests := []struct {
		input string
		pos   int
		want  int
	}{
		{"hello", 0, 0},
		{"  hello", 0, 2},
		{"  hello", 2, 2},
		{"hello  ", 5, 7},
		{"", 0, 0},
		{"   ", 0, 3},
	}

	for _, tc := range tests {
		got := skipWS(tc.input, tc.pos)
		if got != tc.want {
			t.Errorf("skipWS(%q, %d) = %d, want %d", tc.input, tc.pos, got, tc.want)
		}
	}
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		input   string
		pos     int
		wantKey string
		wantErr bool
	}{
		{`name:`, 0, "name", false},
		{`"quoted key":`, 0, "quoted key", false},
		{`  name:`, 0, "name", false},
		{``, 0, "", true},
	}

	for _, tc := range tests {
		key, _, err := parseKey(tc.input, tc.pos)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseKey(%q) expected error", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseKey(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if key != tc.wantKey {
			t.Errorf("parseKey(%q) = %q, want %q", tc.input, key, tc.wantKey)
		}
	}
}

func TestFindMatchingBrace(t *testing.T) {
	tests := []struct {
		input   string
		start   int
		want    int
		wantErr bool
	}{
		{"{}", 0, 1, false},
		{"{{}}", 0, 3, false},
		{"{abc}", 0, 4, false},
		{`{"a":"b"}`, 0, 8, false},
		{"{", 0, 0, true},
		{"{\"test}", 0, 0, true},
	}

	for _, tc := range tests {
		got, err := findMatchingBrace(tc.input, tc.start)
		if tc.wantErr {
			if err == nil {
				t.Errorf("findMatchingBrace(%q) expected error", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("findMatchingBrace(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.want {
			t.Errorf("findMatchingBrace(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestFindMatchingBracket(t *testing.T) {
	tests := []struct {
		input   string
		start   int
		want    int
		wantErr bool
	}{
		{"[]", 0, 1, false},
		{"[[]]", 0, 3, false},
		{"[1,2,3]", 0, 6, false},
		{`["a","b"]`, 0, 8, false},
		{"[", 0, 0, true},
		{"[\"test]", 0, 0, true},
	}

	for _, tc := range tests {
		got, err := findMatchingBracket(tc.input, tc.start)
		if tc.wantErr {
			if err == nil {
				t.Errorf("findMatchingBracket(%q) expected error", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("findMatchingBracket(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.want {
			t.Errorf("findMatchingBracket(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestParseArray(t *testing.T) {
	tests := []struct {
		input    string
		wantLen  int
		wantErr  bool
	}{
		{"", 0, false},
		{"1, 2, 3", 3, false},
		{`"a", "b"`, 2, false},
		{"1,", 1, false}, // trailing comma is ok
	}

	for _, tc := range tests {
		arr, err := parseArray(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseArray(%q) expected error", tc.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseArray(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if len(arr) != tc.wantLen {
			t.Errorf("parseArray(%q) len = %d, want %d", tc.input, len(arr), tc.wantLen)
		}
	}
}
