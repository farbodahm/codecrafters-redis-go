package main

// ReplicationConfig holds all config related to Redis replication
type ReplicationConfig struct {
	Role string
	// Master Config
	MasterReplicationID     string
	MasterReplicationOffset int

	// Slave Config
	SlaveMasterHost string
	SlaveMasterPort int
}

// GenerateMasterReplicationId generates a random 40 char string for replication id
func GenerateMasterReplicationId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}
