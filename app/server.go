package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidCommand = errors.New("invalid command")
var ErrInvalidConfigParameter = errors.New("invalid config parameter")

// Redis is a simple Redis server implementation.
type Redis struct {
	storage Storage
	ss      StreamsStorage
	config  Config
	rconfig ReplicationConfig

	// master is the connection to master after handshake finished successfully
	master *bufio.ReadWriter
	// slaves is the connections to slaves after handshake finished successfully
	slaves map[string]*Slave
	// ackChan represents id of the slave which has processed all of the propagated messages
	ackChan chan string
}

// Config holds the configuration for the Redis server.
type Config struct {
	Address    string
	Port       int
	Dir        string
	DBFileName string
}

// ReplicationConfig holds all config related to Redis replication
type ReplicationConfig struct {
	Role string
	// Master Config
	MasterReplicationID     string
	MasterReplicationOffset int

	// Slave Config
	SlaveMasterHost string
	SlaveMasterPort int
	bytesReceived   int
	bytesProcessed  int
}

// Slave represents a replica instance used in master process.
type Slave struct {
	id        string
	bytesSend int
	rw        *bufio.ReadWriter
}

func NewRedis(config Config, storage Storage, ss StreamsStorage, replicaOf string) *Redis {
	var rconfig ReplicationConfig
	if replicaOf == "" {
		rconfig = ReplicationConfig{
			Role:                    "master",
			MasterReplicationID:     GenerateMasterReplicationId(),
			MasterReplicationOffset: 0,
		}
	} else {
		s := strings.Split(replicaOf, " ")
		port, err := strconv.Atoi(s[1])
		if err != nil {
			log.Fatal("Invalid port")
		}
		rconfig = ReplicationConfig{
			Role:            "slave",
			SlaveMasterHost: s[0],
			SlaveMasterPort: port,
		}
	}

	return &Redis{
		storage: storage,
		ss:      ss,
		config:  config,
		rconfig: rconfig,
		slaves:  make(map[string]*Slave),
		ackChan: make(chan string, 100),
	}
}

// HandleSetCommand handles the SET command including the optional TTL argument.
func (r *Redis) HandleSetCommand(args []string) error {
	if r.rconfig.Role == "master" {
		if err := r.PropagateToSlaves(EncodeRESPArray(args)); err != nil {
			log.Println("WARN: Failed propagating", args, "to slaves")
		}
	}

	if len(args) == 3 {
		return r.storage.Set(args[1], args[2])
	}

	if len(args) == 5 {
		log.Println("Setting with TTL")
		ttl, err := strconv.Atoi(args[4])
		if err != nil {
			return err
		}

		return r.storage.SetWithTTL(args[1], args[2], int64(ttl))
	}

	return ErrInvalidCommand
}

// HandleConfigCommand handles the CONFIG command.
func (r *Redis) HandleConfigCommand(args []string) ([]byte, error) {
	if len(args) == 3 {
		switch args[1] {
		case "GET":
			switch args[2] {
			case "dir":
				return EncodeRESPArray([]string{"dir", r.config.Dir}), nil
			case "dbfilename":
				return EncodeRESPArray([]string{"dbfilename", r.config.DBFileName}), nil
			default:
				return EncodeRESPBulkString(""), ErrInvalidConfigParameter
			}
		}
	}

	return EncodeRESPBulkString(""), ErrInvalidCommand
}

// HandleInfoCommand handles info command. Currently it only handles replication related info.
func (r *Redis) HandleInfoCommand(args []string) ([]byte, error) {
	if len(args) != 2 || args[1] != "replication" {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s%s", r.rconfig.Role, RESPDelimiter))
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d%s", r.rconfig.MasterReplicationOffset, RESPDelimiter))
	sb.WriteString(fmt.Sprintf("master_replid:%s", r.rconfig.MasterReplicationID))

	return EncodeRESPBulkString(sb.String()), nil
}

// Write writes a buffer to a connection.
func (r *Redis) Write(w *bufio.Writer, buf []byte) error {
	_, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("error sending: %w", err)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("error flushing: %w", err)
	}
	return err
}

// HandleKeysCommand handles the KEYS command.
func (r *Redis) HandleKeysCommand(args []string) ([]byte, error) {
	if len(args) != 2 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}

	keys, err := r.storage.Keys()
	if err != nil {
		return EncodeRESPBulkString(""), err
	}

	return EncodeRESPArray(keys), nil
}

// HandleReplconfCommand handles the REPLCONF command.
// If this function is used by master, the related replica to the connection will also be passed.
func (r *Redis) HandleReplconfCommand(s *Slave, args []string) ([]byte, error) {
	switch strings.ToLower(args[1]) {
	case "listening-port", "capa":
		// TODO: properly handle this case when needed; Currently we are not using this functionality
		return EncodeRESPSimpleString("OK"), nil
	case "getack":
		return EncodeRESPArray([]string{"REPLCONF", "ACK", fmt.Sprintf("%d", r.rconfig.bytesProcessed)}), nil
	case "ack":
		processedBytes, err := strconv.Atoi(args[2])
		if err != nil {
			return EncodeRESPBulkString(""), fmt.Errorf("invalid ack value: %w", err)
		}
		if processedBytes == r.slaves[s.id].bytesSend-37 {
			log.Println("ACK ", s.id, processedBytes)
			r.ackChan <- s.id
		} else {
			log.Println("WARN: ACK ", s.id, " but not all of the data was processed", processedBytes)
			log.Println("Sent bytes:", r.slaves[s.id].bytesSend-37, " Received ack bytes:", processedBytes)
		}

		return []byte{}, nil
	}

	return EncodeRESPBulkString(""), ErrInvalidCommand
}

// HandlePsyncCommand returns replication id, offset and initial RDB for the first handshake.
func (r *Redis) HandlePsyncCommand(args []string) ([]byte, error) {
	if len(args) != 3 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}

	var buffer bytes.Buffer
	buffer.Write(
		EncodeRESPSimpleString(
			fmt.Sprintf("FULLRESYNC %s %d", r.rconfig.MasterReplicationID, r.rconfig.MasterReplicationOffset),
		),
	)

	// For the initial PSYNC:
	// 1) send RDB as well for full resync.
	if args[1] == "?" {
		rdb, err := ReadRDB("./redis-data/empty.rdb")
		if err != nil {
			return EncodeRESPBulkString(""), err
		}
		buffer.WriteString(fmt.Sprintf("$%d%s", len(rdb), RESPDelimiter))
		buffer.Write(rdb)
	}

	return buffer.Bytes(), nil
}

// HandleWaitCommand
func (r *Redis) HandleWaitCommand(args []string) ([]byte, error) {
	if len(args) != 3 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}
	ackCount, err := strconv.Atoi(args[1])
	if err != nil {
		return EncodeRESPBulkString(""), fmt.Errorf("invalid ackCount value: %w", err)
	}
	timeout, err := strconv.Atoi(args[2])
	if err != nil {
		return EncodeRESPBulkString(""), fmt.Errorf("invalid timeout value: %w", err)
	}

	// If nothing is propagated to slaves, return immediately
	if r.slaves == nil || len(r.slaves) == 0 {
		return EncodeRESPInteger(0), nil
	}
	for _, slave := range r.slaves {
		if slave.bytesSend > 0 {
			break
		}
		return EncodeRESPInteger(len(r.slaves)), nil
	}

	// Ask slaves to send their processed bytes
	// TODO: Refactor in a way to ask for processed bytes periodically
	if err := r.PropagateToSlaves(EncodeRESPArray([]string{"REPLCONF", "GETACK", "*"})); err != nil {
		return EncodeRESPBulkString(""), fmt.Errorf("failed to propagate ack requests to slaves: %w", err)
	}

	totalAcks := 0
	for {
		select {
		case <-r.ackChan:
			totalAcks++
			if totalAcks == ackCount {
				return EncodeRESPInteger(totalAcks), nil
			}
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			return EncodeRESPInteger(totalAcks), nil
		}
	}
}

// HandleTypeCommand returns type of the given key.
func (r *Redis) HandleTypeCommand(args []string) ([]byte, error) {
	if len(args) != 2 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}

	_, err := r.storage.Get(args[1])
	if err == nil {
		return EncodeRESPSimpleString("string"), nil
	}

	_, exists := r.ss.XGetStream(args[1])
	if exists {
		return EncodeRESPSimpleString("stream"), nil
	}

	return EncodeRESPSimpleString("none"), nil
}

// HandleXAddCommand adds a new record(s) to the stream.
func (r *Redis) HandleXAddCommand(args []string) ([]byte, error) {
	if len(args) < 4 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}

	streamName := args[1]
	recordId := args[2]
	records := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		records[args[i]] = args[i+1]
	}

	node, err := r.ss.XAdd(streamName, recordId, records)
	if err != nil {
		return EncodeRESPError(err.Error()), nil
	}

	return EncodeRESPBulkString(node.Id), nil
}

// handleConnection handles a new connection to the Redis server.
func (r *Redis) handleConnection(c net.Conn) {
	defer c.Close()

	reader := bufio.NewReader(c)
	writer := bufio.NewWriter(c)
	parser := RESPParser{}

	for {
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Println("Error reading:", err.Error())
			break
		}

		log.Println("Received:", string(buf))

		args, ready, err := parser.ParseToken(buf)
		if err != nil {
			log.Println("Error parsing:", err.Error())
			break
		}
		if ready {
			log.Println("Command ready:", args)

			// TODO: Implement the actual command handling. Probably need another state machine here.
			switch args[0] {
			case "PING":
				log.Println("Responding with PONG")
				if err := r.Write(writer, EncodeRESPSimpleString("PONG")); err != nil {
					break
				}
			case "ECHO":
				log.Println("Responding with ECHO")
				if err := r.Write(writer, EncodeRESPBulkString(args[1])); err != nil {
					break
				}
			case "SET":
				err := r.HandleSetCommand(args)
				if err != nil {
					log.Println("Error setting value:", err.Error())
				}
				if err := r.Write(writer, EncodeRESPSimpleString("OK")); err != nil {
					break
				}
			case "GET":
				resp, err := r.storage.Get(args[1])
				if err != nil && err != ErrKeyNotFound {
					log.Println("Error getting value:", err.Error())
					break
				}
				if err := r.Write(writer, EncodeRESPBulkString(resp)); err != nil {
					break
				}

			case "CONFIG":
				resp, err := r.HandleConfigCommand(args)
				if err != nil {
					log.Println("Error handling CONFIG command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			case "KEYS":
				resp, err := r.HandleKeysCommand(args)
				if err != nil {
					log.Println("Error handling KEYS command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			case "INFO":
				resp, err := r.HandleInfoCommand(args)
				if err != nil {
					log.Println("Error handling INFO command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			case "REPLCONF":
				resp, err := r.HandleReplconfCommand(r.slaves[c.RemoteAddr().String()], args)
				if err != nil {
					log.Println("Error handling REPLCONF command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			case "PSYNC":
				resp, err := r.HandlePsyncCommand(args)
				if err != nil {
					log.Println("Error handling PSYNC command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
				r.slaves[c.RemoteAddr().String()] = &Slave{id: c.RemoteAddr().String(), rw: bufio.NewReadWriter(reader, writer)}
			case "WAIT":
				resp, err := r.HandleWaitCommand(args)
				if err != nil {
					log.Println("Error handling WAIT command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			case "TYPE":
				resp, err := r.HandleTypeCommand(args)
				if err != nil {
					log.Println("Error handling TYPE command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			case "XADD":
				resp, err := r.HandleXAddCommand(args)
				if err != nil {
					log.Println("Error handling XADD command:", err.Error())
				}
				if err := r.Write(writer, resp); err != nil {
					break
				}
			default:
				log.Println("Unknown command:", args[0])
			}
		}
	}
}

// handleReplicationConnection, used by slave, handles a replication connection from master.
func (r *Redis) handleReplicationConnection(rw *bufio.ReadWriter) {
	parser := RESPParser{}

	for {
		buf, err := rw.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Println("Error reading:", err.Error())
			break
		}

		args, ready, err := parser.ParseToken(buf)
		if err != nil {
			log.Println("Error parsing:", err.Error())
			break
		}
		if ready {
			log.Println("Command ready:", args)

			// TODO: Implement the actual command handling. Probably need another state machine here.
			switch args[0] {
			case "SET":
				log.Println("Received SET from master", args)
				err := r.HandleSetCommand(args)
				if err != nil {
					log.Println("Error setting value:", err.Error())
				}
			case "PING":
				log.Println("Received PING from master")
			case "REPLCONF":
				resp, err := r.HandleReplconfCommand(nil, args)
				if err != nil {
					log.Println("Error handling REPLCONF command:", err.Error())
				}
				if err := r.Write(rw.Writer, resp); err != nil {
					break
				}
			default:
				log.Println("Unknown command from master:", args[0])
			}
			r.rconfig.bytesProcessed = r.rconfig.bytesReceived + len(buf)
		}
		r.rconfig.bytesReceived += len(buf)
	}
}

// PropagateToSlaves is a fire-and-forget way for sending the given buffer to slaves of the current master.
// It doesn't wait for receiving an ACK from slave.
func (r *Redis) PropagateToSlaves(buf []byte) error {
	for id, slave := range r.slaves {
		log.Println("Propagating to slave", id)
		if err := r.Write(slave.rw.Writer, buf); err != nil {
			return err
		}
		slave.bytesSend += len(buf)
		log.Println("Propagated to slave", id, "with", slave.bytesSend, "bytes")
	}

	return nil
}

// handshakePing is the first step for handshaking with master.
// It will send a PING to the master and waits for a PONG as response.
func (r *Redis) handshakePing(rw *bufio.ReadWriter, parser RESPParser) error {
	err := r.Write(rw.Writer, (EncodeRESPArray([]string{"PING"})))
	if err != nil {
		return fmt.Errorf("error sending PING: %w", err)

	}

	buf, err := rw.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading PING response: %w", err)

	}

	args, ready, err := parser.ParseToken(buf)
	if err != nil {
		return fmt.Errorf("error parsing PING response: %w", err)
	}
	if !ready || len(args) != 1 {
		return errors.New("invalid response from master")
	}
	if args[0] != "PONG" {
		return errors.New("expected PONG from master")
	}

	return nil
}

// handshakeReplconf is the second step for handshaking with master.
// It sends 2 times of REPLCONF message for communicating slave port and capa.
func (r *Redis) handshakeReplconf(rw *bufio.ReadWriter, parser RESPParser) error {
	err := r.Write(rw.Writer, EncodeRESPArray([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)}))
	if err != nil {
		return fmt.Errorf("error sending REPLCONF: %w", err)
	}

	buf, err := rw.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading REPLCONF response: %w", err)
	}

	args, ready, err := parser.ParseToken(buf)
	if err != nil {
		return fmt.Errorf("error parsing REPLCONF response: %w", err)
	}
	if !ready || len(args) != 1 {
		return errors.New("invalid response from master")
	}
	if args[0] != "OK" {
		return errors.New("expected OK from master")
	}

	err = r.Write(rw.Writer, EncodeRESPArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return fmt.Errorf("error sending REPLCONF: %w", err)
	}

	buf, err = rw.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading REPLCONF response: %w", err)
	}

	args, ready, err = parser.ParseToken(buf)
	if err != nil {
		return fmt.Errorf("error parsing REPLCONF response: %w", err)
	}
	if !ready || len(args) != 1 {
		return errors.New("invalid response from master")
	}
	if args[0] != "OK" {
		return errors.New("expected OK from master")
	}

	return nil
}

// recieveRDB (used by slave) receives RDB from master.
// During the final stage of first PSYNC handshake, master sends its RDB for syncing
// master's state with the slave.
func (r *Redis) recieveRDB(rw *bufio.ReadWriter) error {
	log.Println("Receiving RDB")
	// RDB received during handshaking has similar structure to a RESP bulk string but without
	// /r/n at the end. So we are not using REST parser here as technically it's not RESP encoded.
	buf, err := rw.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading PSYNC RDB response: %w", err)
	}
	buf = buf[1:]
	buf = bytes.TrimRight(buf, RESPDelimiter)
	rdbSize, err := strconv.Atoi(string(buf))
	if err != nil {
		return fmt.Errorf("error parsing RDB size: %w", err)
	}
	log.Println("RDB size:", rdbSize)
	buffer := make([]byte, rdbSize)
	_, err = rw.Read(buffer)
	if err != nil {
		return fmt.Errorf("error reading RDB: %w", err)
	}

	return nil
}

// handshakePsync is the 3rd step for handshaking with master.
// It sends a PSYNC command and waits for the replication id and offset.
func (r *Redis) handshakePsync(rw *bufio.ReadWriter, parser RESPParser) error {
	err := r.Write(rw.Writer, EncodeRESPArray([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return fmt.Errorf("error sending PSYNC: %w", err)
	}

	buf, err := rw.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading PSYNC response: %w", err)
	}

	args, ready, err := parser.ParseToken(buf)
	if err != nil {
		return fmt.Errorf("error parsing PSYNC response: %w", err)
	}
	if !ready || len(args) != 1 {
		return errors.New("invalid response from master")
	}
	// TODO: Properly extract replication id
	if err := r.recieveRDB(rw); err != nil {
		return fmt.Errorf("error receiving RDB: %w", err)
	}
	return nil
}

// StartReplication starts handshaking with master and is used by slave.
func (r *Redis) StartReplication() {
	master, err := net.Dial("tcp", fmt.Sprintf("%s:%d", r.rconfig.SlaveMasterHost, r.rconfig.SlaveMasterPort))
	if err != nil {
		log.Println("Error connecting to master:", err.Error())
		return
	}
	defer master.Close()

	log.Println("Connected to master")
	parser := RESPParser{}
	reader := bufio.NewReader(master)
	writer := bufio.NewWriter(master)
	rw := bufio.NewReadWriter(reader, writer)

	if err := r.handshakePing(rw, parser); err != nil {
		log.Println("Error during PIING handshake:", err.Error())
		return
	}
	log.Println("Ping Handshake with master completed")

	if err := r.handshakeReplconf(rw, parser); err != nil {
		log.Println("Error during REPLCONF handshake:", err.Error())
		return
	}
	log.Println("Replconf Handshake with master completed")

	if err := r.handshakePsync(rw, parser); err != nil {
		log.Println("Error during PSYNC handshake:", err.Error())
		return
	}
	log.Println("PSYNC Handshake with master completed")
	log.Println("Handshake with master completed")
	r.master = rw
	r.handleReplicationConnection(rw)
}

// Start starts the Redis server.
func (r *Redis) Start() {
	log.Printf("Starting server on port %d\n", r.config.Port)

	if r.rconfig.Role == "slave" {
		log.Println("Starting replication")
		go r.StartReplication()
	}

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", r.config.Address, r.config.Port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d\n", r.config.Port)
		os.Exit(1)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		log.Println("Accepted connection", c.RemoteAddr())
		go r.handleConnection(c)
	}
}

func main() {
	log.Println("Starting Application...")

	redisPort := flag.Int("port", 6379, "the port to listen on")
	redisAddr := flag.String("addr", "0.0.0.0", "the address to bind to")
	rdbDir := flag.String("dir", "/tmp/redis-files", "the path to the directory where the RDB file is stored")
	rdbFile := flag.String("dbfilename", "dump.rdb", "the name of the RDB file")
	replicaOf := flag.String("replicaof", "", "the address of the master to replicate from")
	flag.Parse()

	config := Config{
		Address:    *redisAddr,
		Port:       *redisPort,
		Dir:        *rdbDir,
		DBFileName: *rdbFile,
	}

	log.Println("Using config:", config)

	var r Redis
	rdb_path := *rdbDir + "/" + *rdbFile
	if _, err := os.Stat(rdb_path); errors.Is(err, os.ErrNotExist) {
		log.Println("RDB file does not exist")
		r = *NewRedis(config, NewInMemoryStorage(), NewInMemoryLinkedOrderedMap(), *replicaOf)
	} else {
		log.Println("RDB file exists")
		rdb_parser := NewRDBParser(NewInMemoryStorage())
		err := rdb_parser.Parse(rdb_path)
		if err != nil {
			log.Fatal(err)
		}
		r = *NewRedis(config, rdb_parser.Data, NewInMemoryLinkedOrderedMap(), *replicaOf)
	}

	r.Start()
}
