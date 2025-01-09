package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

var RESPDelimiter = "\r\n"

var ErrUnexpectedStarter = errors.New("expected '*' as starting char of parser")
var ErrUnexpectedStateOfParser = errors.New("unexpected state of parser")
var ErrUnexpectedBulkStringStarter = errors.New("expected '$' as starting char of bull string")

type RESPParser struct {
	parsedArgs              []string
	nextArgLength           int8
	remainingExpectedTokens int8
}

func (p *RESPParser) ParseToken(buf []byte) ([]string, bool, error) {
	s := string(bytes.TrimRight(buf, RESPDelimiter))

	// Start of a new command
	if p.remainingExpectedTokens == 0 {
		if s[0] != '*' {
			return nil, false, ErrUnexpectedStarter
		}

		i, err := strconv.Atoi(s[1:])
		if err != nil {
			return nil, false, err
		}
		p.remainingExpectedTokens = int8(i)
		return nil, false, nil
	}
	// Parsing the length of the next argument
	if p.nextArgLength == 0 {
		if s[0] != '$' {
			return nil, false, ErrUnexpectedBulkStringStarter
		}

		i, err := strconv.Atoi(s[1:])
		if err != nil {
			return nil, false, err
		}
		p.nextArgLength = int8(i)
		return nil, false, nil
	}
	// Parsing the next argument
	if p.nextArgLength > 0 {
		p.parsedArgs = append(p.parsedArgs, s)
		p.nextArgLength = 0
		p.remainingExpectedTokens--
	}
	// If we have fully parsed a command
	if p.remainingExpectedTokens == 0 {
		return p.parsedArgs, true, nil
	}

	return p.parsedArgs, false, ErrUnexpectedStateOfParser
}

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
