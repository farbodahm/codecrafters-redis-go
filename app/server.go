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

			default:
				log.Println("Unknown command:", args[0])
			}
		}
	}
}

// Start starts the Redis server.
func (r *Redis) Start() {
	log.Println("Starting server on port 6379")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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

	rdb_dir := flag.String("dir", "/tmp/redis-files", "the path to the directory where the RDB file is stored")
	rdb_file := flag.String("dbfilename", "dump.rdb", "the name of the RDB file")
	flag.Parse()

	config := Config{
		Dir:        *rdb_dir,
		DBFileName: *rdb_file,
	}

	log.Println("Using config:", config)

	r := Redis{storage: NewInMemoryStorage(), config: config}
	r.Start()
}
