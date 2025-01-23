package main

var _ OrderedMap = (*LinkedOrderedMap)(nil)

// OrderedMap defines the behavior of a map that preserves insertion order
// and allows range queries.
type OrderedMap interface {
	Add(id string, data map[string]string)
	Range(startID, endID string) []XRecord
}

// LinkedOrderedMap is one specific implementation of the OrderedMap interface,
// using a doubly linked list and a map under the hood.
type LinkedOrderedMap struct {
	Head *OMNode
	Tail *OMNode

	// Map for fast access to the nodes
	Nodes map[string]*OMNode
}

// OMNode represents a node in the LinkedOrderedMap.
type OMNode struct {
	Prev  *OMNode
	Next  *OMNode
	Value XRecord
}

func NewLinkedOrderedMap() *LinkedOrderedMap {
	return &LinkedOrderedMap{
		Nodes: make(map[string]*OMNode),
	}
}

// Add adds a new key-value pair to the LinkedOrderedMap.
func (om *LinkedOrderedMap) Add(id string, data map[string]string) {
	v := XRecord{
		Id:   id,
		Data: data,
	}
	node := &OMNode{
		Value: v,
	}

	if om.Head == nil {
		om.Head = node
		om.Tail = node
	} else {
		om.Tail.Next = node
		node.Prev = om.Tail
		om.Tail = node
	}

	om.Nodes[id] = node
}

// Range returns a range of nodes from the LinkedOrderedMap starting from `start` key to `end` key.
func (om *LinkedOrderedMap) Range(start_id, end_id string) []XRecord {
	var nodes []XRecord

	node := om.Nodes[start_id]
	for node != nil {
		nodes = append(nodes, node.Value)
		node = node.Next
		if node != nil && node.Value.Id == end_id {
			nodes = append(nodes, node.Value)
			break
		}
	}

	return nodes
}
