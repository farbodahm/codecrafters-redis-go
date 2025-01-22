package main

// OrderedMap is a data structure that maintains:
// 1. Order of insertion
// 2. Fast access to the nodes
// 3. Fast range access
// It's implemented using a doubly linked list and a map (LRU Cache).
// Currently, it's used in Streams implementation to provide fast XRANGE and XREAD access.
type OrderedMap struct {
	Head *OMNode
	Tail *OMNode

	// Map for fast access to the nodes
	Nodes map[string]*OMNode
}

// OMNode represents a node in the OrderedMap.
type OMNode struct {
	Prev  *OMNode
	Next  *OMNode
	Value XRecord
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		Nodes: make(map[string]*OMNode),
	}
}

// XAdd adds a new key-value pair to the OrderedMap.
func (om *OrderedMap) XAdd(id string, data map[string]string) {
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

// XRange returns a range of nodes from the OrderedMap starting from `start` key to `end` key.
func (om *OrderedMap) XRange(start_id, end_id string) []XRecord {
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

// XGet returns the node with the given key from the OrderedMap.
func (om *OrderedMap) XGet(key string) (*OMNode, bool) {
	v, exists := om.Nodes[key]
	return v, exists
}
