package main

import (
	"fmt"
	"io"
	"os"
)

// GenerateMasterReplicationId generates a random 40 char string for replication id
func GenerateMasterReplicationId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

// ReadRDB is used for reading a RDB file as bytes.
// Currently it's used in first handshake of slave with master.
func ReadRDB(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening RDB file: %v", err)
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error reading RDB file: %v", err)
	}

	return bytes, nil
}

// MapToArray converts a map to an array of strings.
func MapToArray(m map[string]string) []string {
	var arr []string
	for k, v := range m {
		arr = append(arr, k, v)
	}
	return arr
}
