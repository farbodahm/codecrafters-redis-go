package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

// Redis is a simple Redis server implementation.
type Redis struct {
	storage Storage
}

// Set sets the value of a key in the Redis server.
func (r *Redis) Set(key, value string) error {
	return r.storage.Set(key, value)
}

// Get retrieves the value of a key from the Redis server.
func (r *Redis) Get(key string) (string, error) {
	return r.storage.Get(key)
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
				err := r.Set(args[1], args[2])
				if err != nil {
					log.Println("Error setting value:", err.Error())
				}
				if err := r.Write(c, EncodeRESPSimpleString("OK")); err != nil {
					break
				}
			case "GET":
				v, err := r.Get(args[1])
				var resp string
				if err != nil {
					log.Println("Error getting value:", err.Error())
					resp = ""
				} else {
					resp = v
				}
				if err := r.Write(c, EncodeRESPBulkString(resp)); err != nil {
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
	r := Redis{storage: NewInMemoryStorage()}
	r.Start()
}
