package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

var ErrInvalidHeader = errors.New("invalid RDB header")
var ErrInvalidMetadata = errors.New("invalid RDB metadata")
var ErrInvalidDatabase = errors.New("invalid RDB database")

const (
	RDB_METADATA   = 0xFA // Metadata
	RDB_DB_START   = 0xFE // Database selector
	RDB_DB_SIZE    = 0xFB // Hash table sizes
	RDB_STRING     = 0x00
	RDB_EXPIRES_MS = 0xFC // Expire time MS
	RDB_EXPIRES_S  = 0xFD // Expire time S
	RDB_EOF        = 0xFF // End of file
	RDB_MODULE_AUX = 0xF7 // Module auxiliary data
)

// RDBParser is a parser for the Redis Database (RDB) file format.
type RDBParser struct {
	RDBVersion      string
	Metadata        map[string]string
	DatabaseIndex   uint64
	TableSize       uint64
	ExpireTableSize uint64
	Data            Storage
}

// NewRDBParser creates a new RDBParser.
func NewRDBParser(storage Storage) *RDBParser {
	return &RDBParser{
		Metadata: make(map[string]string),
		Data:     storage,
	}
}

// Parse parses the RDB file at the given path.
// If parse finishes successfully, you can access the parsed data from the parser.
func (p *RDBParser) Parse(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	r := bufio.NewReader(f)

	if err := p.parseHeader(r); err != nil {
		log.Println("Error parsing header", err)
		return ErrInvalidHeader
	}

	if err := p.parseMetadata(r); err != nil {
		log.Println("Error parsing metadata", err)
		return ErrInvalidMetadata
	}

	if err := p.parseDatabase(r); err != nil {
		log.Println("Error parsing database", err)
		return ErrInvalidDatabase
	}

	log.Println("Parsing finished successfully")
	return nil
}

func (p *RDBParser) parseDatabase(r *bufio.Reader) error {
	// If there is anything between metadata and the database, skip it for now
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b == RDB_DB_START {
			break
		}
	}

	// Read the database index
	l, _, err := p.readLength(r)
	if err != nil {
		return err
	}
	p.DatabaseIndex = l

	// Verify the database size delimiter
	b, err := r.ReadByte()
	if err != nil {
		return err
	}
	if b != RDB_DB_SIZE {
		return fmt.Errorf("expected DB size delimiter, got 0x%02X", b)
	}

	// Read the database size
	l, _, err = p.readLength(r)
	if err != nil {
		return err
	}
	p.TableSize = l
	l, _, err = p.readLength(r)
	if err != nil {
		return err
	}
	p.ExpireTableSize = l

	// Read the key-value pairs
	for {
		b, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("error reading key-value pair type: %w", err)
		}

		switch b {
		case RDB_STRING:
			log.Println("Adding new key-value pair")
			if err := p.addKeyValue(r); err != nil {
				return err
			}
		case RDB_EXPIRES_MS, RDB_EXPIRES_S:
			log.Println("Adding new key-value pair with expire")
			if err := p.addKeyValueWithTTL(b, r); err != nil {
				return err
			}
		case RDB_MODULE_AUX:
			// Skip the module auxiliary data
			log.Println("Skipping module auxiliary data")
			return nil
		case RDB_EOF:
			log.Println("End of database section")
			return nil
		default:
			return fmt.Errorf("unknown key-value pair type: 0x%02X", b)
		}
	}

}

// addKeyValue adds a key-value pair to the storage of the parser.
func (p *RDBParser) addKeyValue(r *bufio.Reader) error {
	// Read the key
	key, err := p.readNextString(r)
	if err != nil {
		return fmt.Errorf("error reading db key: %w", err)
	}

	// Read the value
	value, err := p.readNextString(r)
	if err != nil {
		return fmt.Errorf("error reading db value: %w", err)
	}

	err = p.Data.Set(key, value)
	if err != nil {
		return fmt.Errorf("error setting key-value pair: %w", err)
	}

	return nil
}

// addKeyValueWithTTL adds a key-value and expects the underlying buffer points to a expire time.
func (p *RDBParser) addKeyValueWithTTL(kv_type byte, r *bufio.Reader) error {
	var expireIn time.Duration
	switch kv_type {
	case RDB_EXPIRES_MS:
		bytes := make([]byte, 8)
		_, err := r.Read(bytes)
		if err != nil {
			return fmt.Errorf("error reading expire time MS: %w", err)
		}
		expireAt := time.UnixMilli(int64(binary.LittleEndian.Uint64(bytes)))
		expireIn = time.Until(expireAt)

	case RDB_EXPIRES_S:
		bytes := make([]byte, 4)
		_, err := r.Read(bytes)
		if err != nil {
			return fmt.Errorf("error reading expire time S: %w", err)
		}
		expireAt := time.Unix(int64(binary.LittleEndian.Uint32(bytes)), 0)
		expireIn = time.Until(expireAt)
	default:
		return fmt.Errorf("unknown expire time type: 0x%02X", kv_type)
	}

	// Verify value type
	b, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading value type: %w", err)
	}
	if b != RDB_STRING {
		return fmt.Errorf("expected string value type, got 0x%02X", b)
	}

	// Read the key
	key, err := p.readNextString(r)
	if err != nil {
		return fmt.Errorf("error reading db key: %w", err)
	}

	// Read the value
	value, err := p.readNextString(r)
	if err != nil {
		return fmt.Errorf("error reading db value: %w", err)
	}

	if expireIn < 0 {
		log.Println("Key ", key, " expired")
		return nil
	}

	err = p.Data.SetWithTTL(key, value, expireIn.Milliseconds())
	if err != nil {
		return fmt.Errorf("error setting key-value pair: %w", err)
	}

	return nil
}

// parseHeader parses the header of the RDB file.
func (p *RDBParser) parseHeader(r *bufio.Reader) error {
	header := make([]byte, 9)
	_, err := r.Read(header)
	if err != nil {
		return err
	}

	if string(header)[:5] != "REDIS" {
		return ErrInvalidHeader
	}

	p.RDBVersion = string(header)[5:]

	return nil
}

// parseMetadata parses the metadata section of the RDB file.
func (p *RDBParser) parseMetadata(r *bufio.Reader) error {
	for {
		// Check if we are in the metadata section
		b, err := r.Peek(1)
		if err != nil {
			return err
		}

		if b[0] != RDB_METADATA {
			break
		}

		// Ignore the metadata byte
		_, err = r.ReadByte()
		if err != nil {
			return err
		}

		// Read the key
		key, err := p.readNextString(r)
		if err != nil {
			return err
		}

		// Read the value
		value, err := p.readNextString(r)
		if err != nil {
			return err
		}

		p.Metadata[key] = value
	}

	return nil
}

// readNextString reads the next string from the reader.
// It's expected that the next byte of the reader is the length of the string.
func (p *RDBParser) readNextString(r *bufio.Reader) (string, error) {
	l, stringEncoded, err := p.readLength(r)
	if err != nil {
		return "", err
	}

	if stringEncoded {
		return p.readStringEncoded(r)
	}

	buf := make([]byte, l)
	_, err = r.Read(buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

// readStringEncoded reads a string encoded in a special way.
func (p *RDBParser) readStringEncoded(r *bufio.Reader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", fmt.Errorf("error reading special encoding: %w", err)
	}

	switch b {
	case 0xC0:
		b, err := r.ReadByte()
		if err != nil {
			return "", fmt.Errorf("error reading special encoding 0xC0: %w", err)
		}
		return fmt.Sprintf("%d", int(b)), nil
	case 0xC1:
		b := make([]byte, 2)
		_, err := r.Read(b)
		if err != nil {
			return "", fmt.Errorf("error reading special encoding 0xC1: %w", err)
		}
		return fmt.Sprintf("%d", binary.LittleEndian.Uint16(b)), nil
	case 0xC2:
		b := make([]byte, 4)
		_, err := r.Read(b)
		if err != nil {
			return "", fmt.Errorf("error reading special encoding 0xC1: %w", err)
		}
		return fmt.Sprintf("%d", binary.LittleEndian.Uint16(b)), nil
	}

	return "", fmt.Errorf("unknown special encoding: 0x%02X", b)
}

// readLength reads the length of the next string from the reader.
func (p *RDBParser) readLength(r *bufio.Reader) (uint64, bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, false, fmt.Errorf("error reading length: %w", err)
	}

	lengthType := (b & 0xC0) >> 6

	switch lengthType {
	case 0x00: // 6 bits string length
		return uint64(b & 0x3F), false, nil
	case 0x01: // 14 bits string length
		b2, err := r.ReadByte()
		if err != nil {
			return 0, false, fmt.Errorf("error reading length in 14bit encoded: %w", err)
		}
		// TODO: Check if the length is correct
		return (uint64(b&0x3F) << 8) | uint64(b2), false, nil
	case 0x02: // 32 bits string length
		buf := make([]byte, 4)
		_, err := r.Read(buf)
		if err != nil {
			return 0, false, fmt.Errorf("error reading length in 32bit encoded: %w", err)
		}
		return uint64(binary.BigEndian.Uint32(buf)), false, nil
	case 0x03: // Special encoding
		// Unread the special byte, so we can read it again in the special encoding function
		err := r.UnreadByte()
		if err != nil {
			return 0, false, fmt.Errorf("error unread byte: %w", err)
		}
		return 0, true, nil
	}

	return 0, false, fmt.Errorf("unknown length type: 0x%02X", lengthType)
}
