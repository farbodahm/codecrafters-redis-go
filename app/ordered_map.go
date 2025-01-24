package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

var _ OrderedMap = (*LinkedOrderedMap)(nil)

// OrderedMap defines the behavior of a map that preserves insertion order
// and allows range queries.
type OrderedMap interface {
	Add(id string, data map[string]string) (XRecord, error)
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
func (om *LinkedOrderedMap) Add(id string, data map[string]string) (XRecord, error) {
	milisecondsTime, sequence, err := om.verifyId(id)
	if err != nil {
		return XRecord{}, err
	}

	v := XRecord{
		Id:               fmt.Sprintf("%d-%d", milisecondsTime, sequence),
		MillisecondsTime: milisecondsTime,
		SequenceNumber:   sequence,
		Data:             data,
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
	return v, nil
}

// verifyId verifies if the id is greater than the last id in the LinkedOrderedMap.
// It returns the milisecondsTime and sequence number of the id.
func (om *LinkedOrderedMap) verifyId(id string) (int64, int, error) {

	var milisecondsTime int64
	var sequence int
	var err error
	if id == "*" {
		milisecondsTime = time.Now().UnixMilli()
		sequence = 0
	} else {
		s := strings.Split(id, "-")
		milisecondsTime, err = strconv.ParseInt(s[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("error converting milisecondsTime to int: %w", err)
		}

		if s[1] == "*" {
			sequence = om.generateNextSequenceNumber(milisecondsTime)
		} else {
			sequence, err = strconv.Atoi(s[1])
			if err != nil {
				return 0, 0, fmt.Errorf("error converting sequence to int: %w", err)
			}
		}
	}

	log.Println("milisecondsTime", milisecondsTime, "sequence", sequence)

	if milisecondsTime <= 0 && sequence <= 0 {
		return 0, 0, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	// check if the id is greater than the last id
	if om.Tail != nil {
		lastMilisecondsTime := om.Tail.Value.MillisecondsTime
		lastSequence := om.Tail.Value.SequenceNumber
		if milisecondsTime < lastMilisecondsTime || (milisecondsTime == lastMilisecondsTime && sequence <= lastSequence) {
			return 0, 0, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	return milisecondsTime, sequence, nil
}

// generateNextSequenceNumber generates the next sequence number for the LinkedOrderedMap.
func (om *LinkedOrderedMap) generateNextSequenceNumber(milisecondsTime int64) int {
	if om.Tail == nil || milisecondsTime == 0 {
		return 1
	}

	if om.Tail.Value.MillisecondsTime == milisecondsTime {
		return om.Tail.Value.SequenceNumber + 1
	}

	return 0
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
