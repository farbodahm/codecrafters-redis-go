package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	log.Println("Starting server on port 6379")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	c, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	c.Write([]byte("+PONG\r\n"))
}
