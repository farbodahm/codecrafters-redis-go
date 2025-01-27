package main

import (
	"fmt"
	"strconv"
	"strings"
)

// parseXReadArgs extracts the block duration and a map of stream->startID from
// the command arguments. Expected patterns:
//
//	XREAD BLOCK <blockMs> STREAMS <stream1> <stream2> ... <id1> <id2> ...
//	XREAD STREAMS <stream1> <stream2> ... <id1> <id2> ...
func parseXReadArgs(args []string) (int, map[string]string, error) {
	// By default, no blocking
	blockMs := -1
	var streamNames []string

	// Check if second argument is "BLOCK"
	if strings.ToLower(args[1]) == "block" {
		// blockMs should be args[2]
		ms, err := strconv.Atoi(args[2])
		if err != nil {
			return 0, nil, fmt.Errorf("invalid block duration: %v", err)
		}
		blockMs = ms

		// skip "BLOCK <ms> STREAMS"
		// we expect: XREAD BLOCK <ms> STREAMS <stream1> <stream2> ... <id1> <id2> ...
		if len(args) < 5 || strings.ToLower(args[3]) != "streams" {
			return 0, nil, ErrInvalidCommand
		}
		streamNames = args[4:]
	} else {
		// we expect: XREAD STREAMS <stream1> <stream2> ... <id1> <id2> ...
		if strings.ToLower(args[1]) != "streams" {
			return 0, nil, ErrInvalidCommand
		}
		streamNames = args[2:]
	}

	if len(streamNames) == 0 || len(streamNames)%2 != 0 {
		// We expect pairs of (stream, ID)
		return 0, nil, ErrInvalidCommand
	}

	// Build map of stream -> start ID
	numStreams := len(streamNames) / 2
	streamToID := make(map[string]string, numStreams)
	for i := 0; i < numStreams; i++ {
		streamName := streamNames[i]
		startID := streamNames[i+numStreams]
		streamToID[streamName] = startID
	}

	return blockMs, streamToID, nil
}
