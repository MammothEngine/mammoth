package auth

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// UserRecord stores SCRAM credentials for a user.
type UserRecord struct {
	Username   string `json:"username"`
	AuthDB     string `json:"authDB"`
	Salt       []byte `json:"salt"`
	Iterations int    `json:"iterations"`
	StoredKey  []byte `json:"storedKey"`
	ServerKey  []byte `json:"serverKey"`
	CreatedAt  int64  `json:"createdAt"`
}

// UserStore manages user credentials in the engine KV store.
type UserStore struct {
	eng        *engine.Engine
	getUserFn  func(string) (*UserRecord, error) // for testing
}

// NewUserStore creates a new user store backed by the engine.
func NewUserStore(eng *engine.Engine) *UserStore {
	return &UserStore{eng: eng}
}

// CreateUser creates a new user with the given password.
func (us *UserStore) CreateUser(username, authDB, password string) error {
	key := mongo.EncodeCatalogKeyUser(authDB, username)
	if _, err := us.eng.Get(key); err == nil {
		return fmt.Errorf("user already exists: %s", username)
	}
	return us.putUser(username, authDB, password)
}

// UpdatePassword updates a user's password.
func (us *UserStore) UpdatePassword(username, authDB, password string) error {
	key := mongo.EncodeCatalogKeyUser(authDB, username)
	if _, err := us.eng.Get(key); err != nil {
		return fmt.Errorf("user not found: %s", username)
	}
	return us.putUser(username, authDB, password)
}

// DropUser removes a user.
func (us *UserStore) DropUser(username, authDB string) error {
	key := mongo.EncodeCatalogKeyUser(authDB, username)
	if _, err := us.eng.Get(key); err != nil {
		return fmt.Errorf("user not found: %s", username)
	}
	return us.eng.Delete(key)
}

// GetUser retrieves a user by username (searches all databases).
func (us *UserStore) GetUser(username string) (*UserRecord, error) {
	if us.getUserFn != nil {
		return us.getUserFn(username)
	}
	prefix := mongo.EncodeCatalogKeyUserPrefix()
	var user *UserRecord
	us.eng.Scan(prefix, func(key, value []byte) bool {
		rec := &UserRecord{}
		if json.Unmarshal(value, rec) == nil && rec.Username == username {
			user = rec
			return false
		}
		return true
	})
	if user == nil {
		return nil, fmt.Errorf("user not found: %s", username)
	}
	return user, nil
}

// GetUsersInDB returns all users in a specific database.
func (us *UserStore) GetUsersInDB(authDB string) ([]*UserRecord, error) {
	prefix := mongo.EncodeCatalogKeyUserDBPrefix(authDB)
	var users []*UserRecord
	us.eng.Scan(prefix, func(key, value []byte) bool {
		rec := &UserRecord{}
		if json.Unmarshal(value, rec) == nil {
			users = append(users, rec)
		}
		return true
	})
	return users, nil
}

func (us *UserStore) putUser(username, authDB, password string) error {
	salt := GenerateSalt()
	storedKey, serverKey := DeriveKeys(password, salt, defaultIterations)

	rec := UserRecord{
		Username:   username,
		AuthDB:     authDB,
		Salt:       salt,
		Iterations: defaultIterations,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
		CreatedAt:  time.Now().Unix(),
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	key := mongo.EncodeCatalogKeyUser(authDB, username)
	return us.eng.Put(key, data)
}
