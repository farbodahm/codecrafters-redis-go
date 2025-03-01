package main

var _ StreamsStorage = (*InMemoryLinkedOrderedMap)(nil)

// XRecord represents a record in Streams.
type XRecord struct {
	Id               string
	MillisecondsTime int64
	SequenceNumber   int
	Data             map[string]string
}

// StreamsStorage is an interface for storing and retrieving streams of data.
type StreamsStorage interface {
	XAdd(stream, id string, data map[string]string) (XRecord, error)
	XRange(stream, start_id, end_id string) []XRecord
	XGetStream(stream string) (OrderedMap, bool)
	XRead(stream string, id string) []XRecord
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

func (storage *InMemoryLinkedOrderedMap) XAdd(stream, id string, data map[string]string) (XRecord, error) {
	if _, ok := storage.streams[stream]; !ok {
		storage.streams[stream] = NewLinkedOrderedMap()
	}
	return storage.streams[stream].Add(id, data)
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

func (storage *InMemoryLinkedOrderedMap) XRead(stream string, id string) []XRecord {
	if _, ok := storage.streams[stream]; !ok {
		return []XRecord{}
	}
	return storage.streams[stream].Read(id)
}
