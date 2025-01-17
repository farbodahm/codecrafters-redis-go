package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var ErrInvalidCommand = errors.New("invalid command")
var ErrInvalidConfigParameter = errors.New("invalid config parameter")

// Redis is a simple Redis server implementation.
type Redis struct {
	storage Storage
	config  Config
	rconfig ReplicationConfig
}

// Config holds the configuration for the Redis server.
type Config struct {
	Address    string
	Port       int
	Dir        string
	DBFileName string
}

func NewRedis(config Config, storage Storage, replicaOf string) *Redis {
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
		config:  config,
		rconfig: rconfig,
	}
}

// HandleSetCommand handles the SET command including the optional TTL argument.
func (r *Redis) HandleSetCommand(args []string) error {
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
func (r *Redis) Write(c net.Conn, buf []byte) error {
	_, err := c.Write(buf)
	if err != nil {
		log.Println("Error writing:", err.Error())
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
func (r *Redis) HandleReplconfCommand(args []string) ([]byte, error) {
	if len(args) != 3 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}
	// TODO: Implement the actual REPLCONF command handling.
	return EncodeRESPSimpleString("OK"), nil
}

func (r *Redis) HandlePsyncCommand(args []string) ([]byte, error) {
	if len(args) != 3 {
		return EncodeRESPBulkString(""), ErrInvalidCommand
	}

	return EncodeRESPSimpleString(fmt.Sprintf("FULLRESYNC %s %d", r.rconfig.MasterReplicationID, r.rconfig.MasterReplicationOffset)), nil
}

// handleConnection handles a new connection to the Redis server.
func (r *Redis) handleConnection(c net.Conn) {
	defer c.Close()

	reader := bufio.NewReader(c)
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
				if err := r.Write(c, EncodeRESPSimpleString("PONG")); err != nil {
					break
				}
			case "ECHO":
				log.Println("Responding with ECHO")
				if err := r.Write(c, EncodeRESPBulkString(args[1])); err != nil {
					break
				}
			case "SET":
				err := r.HandleSetCommand(args)
				if err != nil {
					log.Println("Error setting value:", err.Error())
				}
				if err := r.Write(c, EncodeRESPSimpleString("OK")); err != nil {
					break
				}
			case "GET":
				resp, err := r.storage.Get(args[1])
				if err != nil && err != ErrKeyNotFound {
					log.Println("Error getting value:", err.Error())
					break
				}
				if err := r.Write(c, EncodeRESPBulkString(resp)); err != nil {
					break
				}

			case "CONFIG":
				resp, err := r.HandleConfigCommand(args)
				if err != nil {
					log.Println("Error handling CONFIG command:", err.Error())
				}
				if err := r.Write(c, resp); err != nil {
					break
				}
			case "KEYS":
				resp, err := r.HandleKeysCommand(args)
				if err != nil {
					log.Println("Error handling KEYS command:", err.Error())
				}
				if err := r.Write(c, resp); err != nil {
					break
				}
			case "INFO":
				resp, err := r.HandleInfoCommand(args)
				if err != nil {
					log.Println("Error handling INFO command:", err.Error())
				}
				if err := r.Write(c, resp); err != nil {
					break
				}
			case "REPLCONF":
				resp, err := r.HandleReplconfCommand(args)
				if err != nil {
					log.Println("Error handling INFO command:", err.Error())
				}
				if err := r.Write(c, resp); err != nil {
					break
				}
			case "PSYNC":
				resp, err := r.HandlePsyncCommand(args)
				if err != nil {
					log.Println("Error handling PSYNC command:", err.Error())
				}
				if err := r.Write(c, resp); err != nil {
					break
				}
			default:
				log.Println("Unknown command:", args[0])
			}
		}
	}
}

// handshakePing is the first step for handshaking with master.
// It will send a PING to the master and waits for a PONG as response.
func (r *Redis) handshakePing(rw *bufio.ReadWriter, parser RESPParser) error {
	_, err := rw.Write(EncodeRESPArray([]string{"PING"}))
	if err != nil {
		return fmt.Errorf("error sending PING: %w", err)
	}
	err = rw.Flush()
	if err != nil {
		return fmt.Errorf("error flushing PING: %w", err)

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
	_, err := rw.Write(EncodeRESPArray([]string{"REPLCONF", "listening-port", strconv.Itoa(r.config.Port)}))
	if err != nil {
		return fmt.Errorf("error sending REPLCONF: %w", err)
	}
	if err := rw.Flush(); err != nil {
		return fmt.Errorf("error flushing REPLCONF: %w", err)
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

	_, err = rw.Write(EncodeRESPArray([]string{"REPLCONF", "capa", "psync2"}))
	if err != nil {
		return fmt.Errorf("error sending REPLCONF: %w", err)
	}
	if err := rw.Flush(); err != nil {
		return fmt.Errorf("error flushing REPLCONF: %w", err)
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

// handshakePsync is the 3rd step for handshaking with master.
// It sends a PSYNC command and waits for the replication id and offset.
func (r *Redis) handshakePsync(rw *bufio.ReadWriter, parser RESPParser) error {
	_, err := rw.Write(EncodeRESPArray([]string{"PSYNC", "?", "-1"}))
	if err != nil {
		return fmt.Errorf("error sending PSYNC: %w", err)
	}
	if err := rw.Flush(); err != nil {
		return fmt.Errorf("error flushing PSYNC: %w", err)
	}

	buf, err := rw.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("error reading PSYNC response: %w", err)
	}

	args, ready, err := parser.ParseToken(buf)
	if err != nil {
		return fmt.Errorf("error parsing PSYNC response: %w", err)
	}
	if !ready || len(args) != 3 {
		return errors.New("invalid response from master")
	}
	if args[0] != "FULLRESYNC" {
		return errors.New("expected FULLRESYNC from master")
	}
	// TODO: Handle response

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
		r = *NewRedis(config, NewInMemoryStorage(), *replicaOf)
	} else {
		log.Println("RDB file exists")
		rdb_parser := NewRDBParser(NewInMemoryStorage())
		err := rdb_parser.Parse(rdb_path)
		if err != nil {
			log.Fatal(err)
		}
		r = *NewRedis(config, rdb_parser.Data, *replicaOf)
	}

	r.Start()
}
