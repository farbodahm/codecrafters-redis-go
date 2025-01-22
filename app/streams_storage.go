package main

var _ StreamsStorage = (*InMemoryOrderedMap)(nil)

// XRecord represents a record in Streams.
type XRecord struct {
	Id   string
	Data map[string]string
}

// StreamsStorage is an interface for storing and retrieving streams of data.
type StreamsStorage interface {
	XAdd(stream, id string, data map[string]string)
	XRange(stream, start_id, end_id string) []XRecord
}

// InMemoryOrderedMap is an in-memory implementation of the OrderedMap data structure.
type InMemoryOrderedMap struct {
	streams map[string]*OrderedMap
}

func NewInMemoryOrderedMap() *InMemoryOrderedMap {
	storage := &InMemoryOrderedMap{
		streams: make(map[string]*OrderedMap),
	}
	return storage
}

func (storage *InMemoryOrderedMap) XAdd(stream, id string, data map[string]string) {
	if _, ok := storage.streams[stream]; !ok {
		storage.streams[stream] = NewOrderedMap()
	}
	storage.streams[stream].XAdd(id, data)
}

func (storage *InMemoryOrderedMap) XRange(stream, start_id, end_id string) []XRecord {
	if _, ok := storage.streams[stream]; !ok {
		return []XRecord{}
	}
	return storage.streams[stream].XRange(start_id, end_id)
}
