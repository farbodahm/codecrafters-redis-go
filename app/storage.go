package main

import (
	"errors"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

// Storage is an interface for storing and retrieving key-value pairs for the Redis server.
type Storage interface {
	Get(key string) (string, error)
	Set(key, value string) error
}

// InMemoryStorage is an in-memory implementation of the Storage interface.
// Currently, it uses Golang's map to store key-value pairs.
type InMemoryStorage struct {
	sync.RWMutex
	data map[string]string
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		data: make(map[string]string),
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

	return v, nil
}

// Set sets the value of a key in the in-memory storage.
func (s *InMemoryStorage) Set(key, value string) error {
	s.Lock()
	defer s.Unlock()

	s.data[key] = value
	return nil
}
