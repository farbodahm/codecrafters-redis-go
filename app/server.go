package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func handleConnection(c net.Conn) {
	defer c.Close()

	reader := bufio.NewReader(c)

	for {
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			if err.Error() == "EOF" {
				continue
			}
			log.Println("Error reading:", err.Error())
			break
		}

		log.Println("Received:", string(buf))

		if string(buf) == "PING\r\n" {
			log.Println("Responding with PONG")
			_, writeErr := c.Write([]byte("+PONG\r\n"))
			if writeErr != nil {
				log.Println("Error writing:", writeErr.Error())
				break
			}
		}
	}
}

func main() {
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
		go handleConnection(c)
	}
}
