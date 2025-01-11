package main

import (
	"bytes"
	"errors"
	"strconv"
)

var RESPDelimiter = "\r\n"

var ErrUnexpectedStarter = errors.New("expected '*' as starting char of parser")
var ErrUnexpectedStateOfParser = errors.New("unexpected state of parser")
var ErrUnexpectedBulkStringStarter = errors.New("expected '$' as starting char of bull string")

// RESPParser is a parser for the Redis Serialization Protocol (RESP).
type RESPParser struct {
	parsedArgs              []string
	nextArgLength           int8
	remainingExpectedTokens int8
}

// Reset resets the parser to its initial state.
func (p *RESPParser) Reset() {
	p.parsedArgs = nil
	p.nextArgLength = 0
	p.remainingExpectedTokens = 0
}

// ParseToken parses a token from the buffer and returns the parsed arguments, a boolean indicating if the command is ready to be executed, and an error if any.
// A token is a command or a part of a command that is separated by \n delimiter.
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
		result := p.parsedArgs
		p.Reset()
		return result, true, nil
	}

	return p.parsedArgs, false, nil
}

// EncodeSimpleString encodes a simple string into the RESP format.
func EncodeRESPSimpleString(s string) []byte {
	return []byte("+" + s + RESPDelimiter)
}

// EncodeBulkString encodes a bulk string into the RESP format.
func EncodeRESPBulkString(s string) []byte {
	if s == "" {
		return []byte("$-1" + RESPDelimiter)
	}

	return []byte("$" + strconv.Itoa(len(s)) + RESPDelimiter + s + RESPDelimiter)
}
