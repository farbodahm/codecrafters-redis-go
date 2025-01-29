package main

import (
	"errors"
	"strconv"
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
	Keys() ([]string, error)
	Increment(key string) (int, error)
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

// Keys returns a list of all keys in the in-memory storage.
func (s *InMemoryStorage) Keys() ([]string, error) {
	s.RLock()
	defer s.RUnlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}

	return keys, nil
}

// Increment increments the value of a key in the in-memory storage.
func (s *InMemoryStorage) Increment(key string) (int, error) {
	s.Lock()
	defer s.Unlock()

	v, ok := s.data[key]
	if !ok {
		s.data[key] = InMemoryStorageValue{Value: "1"}
		return 1, nil
	}

	// TODO: After using the correct data type, remove this conversion.
	i, err := strconv.Atoi(v.Value)
	if err != nil {
		return -1, err
	}

	v.Value = strconv.Itoa(i + 1)
	s.data[key] = v

	return i + 1, nil
}
