package auth

import "sync"

// ActionType represents a database operation.
type ActionType int

const (
	ActionFind ActionType = iota
	ActionInsert
	ActionUpdate
	ActionDelete
	ActionCreateCollection
	ActionDropCollection
	ActionCreateIndex
	ActionDropIndex
	ActionCreateUser
	ActionDropUser
	ActionCreateRole
	ActionDropRole
	ActionAny
)

// Resource identifies a database resource.
type Resource struct {
	DB         string
	Collection string
}

// Match checks if this resource matches the given resource (wildcard support).
func (r Resource) Match(other Resource) bool {
	if r.DB != "*" && r.DB != other.DB {
		return false
	}
	if r.Collection != "*" && r.Collection != other.Collection {
		return false
	}
	return true
}

// Privilege grants an action on a resource.
type Privilege struct {
	Action   ActionType
	Resource Resource
}

// RoleRef is a reference to a role.
type RoleRef struct {
	DB   string
	Name string
}

// Role defines a set of privileges and inherited roles.
type Role struct {
	Name       string
	DB         string
	Privileges []Privilege
	Inherited  []RoleRef
}

// HasPermission checks if the role grants the specified action on the resource.
func (r *Role) HasPermission(action ActionType, resource Resource) bool {
	for _, p := range r.Privileges {
		if (p.Action == action || p.Action == ActionAny) && p.Resource.Match(resource) {
			return true
		}
	}
	return false
}

var (
	builtinMu    sync.RWMutex
	builtinRoles = make(map[string]*Role)
)

func init() {
	// read: find on all collections in db
	registerBuiltin(&Role{
		Name: "read", DB: "",
		Privileges: []Privilege{
			{ActionFind, Resource{"*", "*"}},
		},
	})
	// readWrite: all CRUD
	registerBuiltin(&Role{
		Name: "readWrite", DB: "",
		Privileges: []Privilege{
			{ActionFind, Resource{"*", "*"}},
			{ActionInsert, Resource{"*", "*"}},
			{ActionUpdate, Resource{"*", "*"}},
			{ActionDelete, Resource{"*", "*"}},
		},
	})
	// dbAdmin: collection + index management
	registerBuiltin(&Role{
		Name: "dbAdmin", DB: "",
		Privileges: []Privilege{
			{ActionFind, Resource{"*", "*"}},
			{ActionCreateCollection, Resource{"*", "*"}},
			{ActionDropCollection, Resource{"*", "*"}},
			{ActionCreateIndex, Resource{"*", "*"}},
			{ActionDropIndex, Resource{"*", "*"}},
		},
	})
	// userAdmin: user management
	registerBuiltin(&Role{
		Name: "userAdmin", DB: "",
		Privileges: []Privilege{
			{ActionCreateUser, Resource{"*", "*"}},
			{ActionDropUser, Resource{"*", "*"}},
		},
	})
	// root: everything
	registerBuiltin(&Role{
		Name: "root", DB: "",
		Privileges: []Privilege{
			{ActionAny, Resource{"*", "*"}},
		},
	})
}

func registerBuiltin(r *Role) {
	builtinRoles[r.Name] = r
}

// LookupRole finds a role by db and name (checks built-ins first).
func LookupRole(db, name string) *Role {
	builtinMu.RLock()
	defer builtinMu.RUnlock()
	if r, ok := builtinRoles[name]; ok {
		return r
	}
	return nil
}

// CommandToAction maps a wire command name to an ActionType.
func CommandToAction(cmd string) ActionType {
	switch cmd {
	case "find", "count", "aggregate", "getMore":
		return ActionFind
	case "insert":
		return ActionInsert
	case "update":
		return ActionUpdate
	case "delete":
		return ActionDelete
	case "create":
		return ActionCreateCollection
	case "drop", "dropDatabase":
		return ActionDropCollection
	case "createIndexes":
		return ActionCreateIndex
	case "dropIndexes":
		return ActionDropIndex
	case "createUser":
		return ActionCreateUser
	case "dropUser":
		return ActionDropUser
	case "createRole":
		return ActionCreateRole
	case "dropRole":
		return ActionDropRole
	default:
		return ActionAny
	}
}
