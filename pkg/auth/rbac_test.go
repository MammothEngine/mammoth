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
	if CommandToAction("find") != ActionFind {
		t.Error("find")
	}
	if CommandToAction("insert") != ActionInsert {
		t.Error("insert")
	}
	if CommandToAction("create") != ActionCreateCollection {
		t.Error("create")
	}
	if CommandToAction("unknown") != ActionAny {
		t.Error("unknown should map to Any")
	}
}
