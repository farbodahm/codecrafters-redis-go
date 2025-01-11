package main

import (
	"errors"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

var _ Storage = (*InMemoryStorage)(nil)

// Storage is an interface for storing and retrieving key-value pairs for the Redis server.
type Storage interface {
	Get(key string) (string, error)
	Set(key, value string) error
	SetWithTTL(key, value string, ttl int64) error
}

// InMemoryStorage is an in-memory implementation of the Storage interface.
// Currently, it uses Golang's map to store key-value pairs.
type InMemoryStorage struct {
	sync.RWMutex
	data map[string]InMemoryStorageValue
}

// InMemoryStorageValue represents a value stored in the in-memory storage.
type InMemoryStorageValue struct {
	Value    string
	StoredAt time.Time
	TTLMs    int64
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make(map[string]InMemoryStorageValue),
	}
}

// Get retrieves the value of a key from the in-memory storage.
func (s *InMemoryStorage) Get(key string) (string, error) {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	if v.TTLMs > 0 {
		if time.Since(v.StoredAt).Milliseconds() > v.TTLMs {
			// TODO: This is `passive` expiration. Implement `active` expiration also to really remove records efficiently.
			return "", ErrKeyNotFound
		}
	}

	return v.Value, nil
}

// Set sets the value of a key in the in-memory storage.
func (s *InMemoryStorage) Set(key, value string) error {
	s.Lock()
	defer s.Unlock()

	s.data[key] = InMemoryStorageValue{Value: value}
	return nil
}

// SetWithTTL sets the value of a key in the in-memory storage with a time-to-live (TTL) in milliseconds.
func (s *InMemoryStorage) SetWithTTL(key, value string, ttl int64) error {
	s.Lock()
	defer s.Unlock()

	s.data[key] = InMemoryStorageValue{Value: value, TTLMs: ttl, StoredAt: time.Now()}
	return nil
}
