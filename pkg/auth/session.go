package auth

import "sync"

// AuthSession tracks authentication state for a connection.
type AuthSession struct {
	ConnID        uint64
	Authenticated bool
	Username      string
	AuthDB        string
	Roles         []RoleRef
	scram         *SCRAMSession
}

// AuthManager manages per-connection authentication state.
type AuthManager struct {
	mu       sync.RWMutex
	sessions map[uint64]*AuthSession
	users    *UserStore
	enabled  bool
}

// NewAuthManager creates a new auth manager.
func NewAuthManager(users *UserStore, enabled bool) *AuthManager {
	return &AuthManager{
		sessions: make(map[uint64]*AuthSession),
		users:    users,
		enabled:  enabled,
	}
}

// Enabled returns whether authentication is required.
func (am *AuthManager) Enabled() bool { return am.enabled }

// UserStore returns the underlying user store.
func (am *AuthManager) UserStore() *UserStore { return am.users }

// GetOrCreateSession returns the auth session for a connection.
func (am *AuthManager) GetOrCreateSession(connID uint64) *AuthSession {
	am.mu.Lock()
	defer am.mu.Unlock()
	if s, ok := am.sessions[connID]; ok {
		return s
	}
	s := &AuthSession{ConnID: connID}
	am.sessions[connID] = s
	return s
}

// GetSession returns the auth session for a connection.
func (am *AuthManager) GetSession(connID uint64) *AuthSession {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.sessions[connID]
}

// RemoveSession cleans up when a connection closes.
func (am *AuthManager) RemoveSession(connID uint64) {
	am.mu.Lock()
	delete(am.sessions, connID)
	am.mu.Unlock()
}

// SetSCRAMSession stores an in-progress SCRAM exchange.
func (am *AuthManager) SetSCRAMSession(connID uint64, scram *SCRAMSession) {
	am.mu.Lock()
	defer am.mu.Unlock()
	s := am.sessions[connID]
	if s == nil {
		s = &AuthSession{ConnID: connID}
		am.sessions[connID] = s
	}
	s.scram = scram
}

// GetSCRAMSession retrieves the in-progress SCRAM exchange.
func (am *AuthManager) GetSCRAMSession(connID uint64) *SCRAMSession {
	am.mu.RLock()
	defer am.mu.RUnlock()
	s := am.sessions[connID]
	if s == nil {
		return nil
	}
	return s.scram
}

// MarkAuthenticated marks a connection as authenticated.
func (am *AuthManager) MarkAuthenticated(connID uint64, username, authDB string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	s := am.sessions[connID]
	if s == nil {
		s = &AuthSession{ConnID: connID}
		am.sessions[connID] = s
	}
	s.Authenticated = true
	s.Username = username
	s.AuthDB = authDB
	s.scram = nil
}

// IsAuthenticated checks if a connection is authenticated.
func (am *AuthManager) IsAuthenticated(connID uint64) bool {
	if !am.enabled {
		return true
	}
	am.mu.RLock()
	defer am.mu.RUnlock()
	s := am.sessions[connID]
	return s != nil && s.Authenticated
}

// GetUser returns the authenticated user for a connection.
func (am *AuthManager) GetUser(connID uint64) (string, string, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	s := am.sessions[connID]
	if s == nil || !s.Authenticated {
		return "", "", false
	}
	return s.Username, s.AuthDB, true
}

// SetRoles sets the roles for an authenticated session.
func (am *AuthManager) SetRoles(connID uint64, roles []RoleRef) {
	am.mu.Lock()
	defer am.mu.Unlock()
	s := am.sessions[connID]
	if s != nil {
		s.Roles = roles
	}
}

// CheckPermission checks if a connection has the specified permission.
func (am *AuthManager) CheckPermission(connID uint64, action ActionType, resource Resource) bool {
	if !am.enabled {
		return true
	}
	am.mu.RLock()
	s := am.sessions[connID]
	am.mu.RUnlock()

	if s == nil || !s.Authenticated {
		return false
	}
	for _, ref := range s.Roles {
		role := LookupRole(ref.DB, ref.Name)
		if role != nil && role.HasPermission(action, resource) {
			return true
		}
	}
	return false
}
