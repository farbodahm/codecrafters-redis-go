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
)

var ErrInvalidCommand = errors.New("invalid command")
var ErrInvalidConfigParameter = errors.New("invalid config parameter")

// Redis is a simple Redis server implementation.
type Redis struct {
	storage Storage
	config  Config
}

// Config holds the configuration for the Redis server.
type Config struct {
	Address    string
	Port       int
	Dir        string
	DBFileName string
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
			default:
				log.Println("Unknown command:", args[0])
			}
		}
	}
}

// Start starts the Redis server.
func (r *Redis) Start() {
	log.Printf("Starting server on port %d\n", r.config.Port)

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

	redis_port := flag.Int("port", 6379, "the port to listen on")
	redis_addr := flag.String("addr", "0.0.0.0", "the address to bind to")
	rdb_dir := flag.String("dir", "/tmp/redis-files", "the path to the directory where the RDB file is stored")
	rdb_file := flag.String("dbfilename", "dump.rdb", "the name of the RDB file")
	flag.Parse()

	config := Config{
		Address:    *redis_addr,
		Port:       *redis_port,
		Dir:        *rdb_dir,
		DBFileName: *rdb_file,
	}

	log.Println("Using config:", config)

	var r Redis
	rdb_path := *rdb_dir + "/" + *rdb_file
	if _, err := os.Stat(rdb_path); errors.Is(err, os.ErrNotExist) {
		log.Println("RDB file does not exist")
		r = Redis{storage: NewInMemoryStorage(), config: config}
	} else {
		log.Println("RDB file exists")
		rdb_parser := NewRDBParser(NewInMemoryStorage())
		err := rdb_parser.Parse(rdb_path)
		if err != nil {
			log.Fatal(err)
		}
		r = Redis{storage: rdb_parser.Data, config: config}
	}

	r.Start()
}
