package auth

import "testing"

func TestResourceMatch(t *testing.T) {
	tests := []struct {
		r1, r2 Resource
		want   bool
	}{
		{Resource{"test", "users"}, Resource{"test", "users"}, true},
		{Resource{"*", "*"}, Resource{"test", "users"}, true},
		{Resource{"test", "*"}, Resource{"test", "users"}, true},
		{Resource{"*", "users"}, Resource{"test", "users"}, true},
		{Resource{"other", "*"}, Resource{"test", "users"}, false},
		{Resource{"test", "other"}, Resource{"test", "users"}, false},
	}
	for _, tt := range tests {
		if got := tt.r1.Match(tt.r2); got != tt.want {
			t.Errorf("Resource{%s,%s}.Match({%s,%s}) = %v, want %v",
				tt.r1.DB, tt.r1.Collection, tt.r2.DB, tt.r2.Collection, got, tt.want)
		}
	}
}

func TestBuiltinRoles(t *testing.T) {
	readRole := LookupRole("", "read")
	if readRole == nil {
		t.Fatal("read role not found")
	}
	if !readRole.HasPermission(ActionFind, Resource{"test", "users"}) {
		t.Error("read should allow find")
	}
	if readRole.HasPermission(ActionInsert, Resource{"test", "users"}) {
		t.Error("read should not allow insert")
	}

	rwRole := LookupRole("", "readWrite")
	if !rwRole.HasPermission(ActionInsert, Resource{"test", "users"}) {
		t.Error("readWrite should allow insert")
	}

	root := LookupRole("", "root")
	if !root.HasPermission(ActionAny, Resource{"any", "any"}) {
		t.Error("root should allow everything")
	}
}

func TestCommandToAction(t *testing.T) {
	tests := []struct {
		cmd      string
		expected ActionType
	}{
		// Read operations
		{"find", ActionFind},
		{"count", ActionFind},
		{"aggregate", ActionFind},
		{"getMore", ActionFind},

		// Write operations
		{"insert", ActionInsert},
		{"update", ActionUpdate},
		{"delete", ActionDelete},

		// Collection operations
		{"create", ActionCreateCollection},
		{"drop", ActionDropCollection},
		{"dropDatabase", ActionDropCollection},

		// Index operations
		{"createIndexes", ActionCreateIndex},
		{"dropIndexes", ActionDropIndex},

		// User operations
		{"createUser", ActionCreateUser},
		{"dropUser", ActionDropUser},

		// Role operations
		{"createRole", ActionCreateRole},
		{"dropRole", ActionDropRole},

		// Unknown command
		{"unknown", ActionAny},
		{"", ActionAny},
		{"randomCmd", ActionAny},
	}

	for _, tt := range tests {
		result := CommandToAction(tt.cmd)
		if result != tt.expected {
			t.Errorf("CommandToAction(%q) = %v, want %v", tt.cmd, result, tt.expected)
		}
	}
}
