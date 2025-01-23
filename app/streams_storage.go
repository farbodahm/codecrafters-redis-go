package main

var _ StreamsStorage = (*InMemoryLinkedOrderedMap)(nil)

// XRecord represents a record in Streams.
type XRecord struct {
	Id   string
	Data map[string]string
}

// StreamsStorage is an interface for storing and retrieving streams of data.
type StreamsStorage interface {
	XAdd(stream, id string, data map[string]string)
	XRange(stream, start_id, end_id string) []XRecord
	XGetStream(stream string) (OrderedMap, bool)
}

// InMemoryLinkedOrderedMap is an in-memory implementation of the LinkedOrderedMap data structure.
type InMemoryLinkedOrderedMap struct {
	streams map[string]*LinkedOrderedMap
}

func NewInMemoryLinkedOrderedMap() *InMemoryLinkedOrderedMap {
	storage := &InMemoryLinkedOrderedMap{
		streams: make(map[string]*LinkedOrderedMap),
	}
	return storage
}

func (storage *InMemoryLinkedOrderedMap) XAdd(stream, id string, data map[string]string) {
	if _, ok := storage.streams[stream]; !ok {
		storage.streams[stream] = NewLinkedOrderedMap()
	}
	storage.streams[stream].Add(id, data)
}

func (storage *InMemoryLinkedOrderedMap) XRange(stream, start_id, end_id string) []XRecord {
	if _, ok := storage.streams[stream]; !ok {
		return []XRecord{}
	}
	return storage.streams[stream].Range(start_id, end_id)
}

func (storage *InMemoryLinkedOrderedMap) XGetStream(stream string) (OrderedMap, bool) {
	if _, ok := storage.streams[stream]; !ok {
		return nil, false
	}
	return storage.streams[stream], true
}
