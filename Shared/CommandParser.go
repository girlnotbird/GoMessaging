package serverUtils

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// Error in parser logic — should never occur, signals an error in code
type CommandParserLogicError struct {
	msgText string
}

func (self *CommandParserLogicError) Error() string {
	return self.msgText
}

func (self *CommandParserLogicError) Is(err error) bool {
	_, ok := err.(*CommandParserLogicError)
	return ok
}

func NewCommandParserLogicError(text string) error {
	return &CommandParserLogicError{
		msgText: text,
	}
}

// Error when parsing, e.g. read too many bytes without finding a header
type CommandParserParseError struct {
	msgText string
}

func (self *CommandParserParseError) Error() string {
	return self.msgText
}

func (self *CommandParserParseError) Is(err error) bool {
	_, ok := err.(*CommandParserParseError)
	return ok
}

func NewCommandParserParseError(text string) error {
	return &CommandParserParseError{
		msgText: text,
	}
}

// Struct that manages command parsing goroutine & passes readable commands back to the main thread
type CommandParser struct {
	pool            sync.Pool
	pluggableReader io.Reader
	NewCommands     chan Command
	seekingHeader   bool
}

var DEFAULT_COMMAND_PARSER_BACKLOG_SIZE uint32 = 1

func NewCommandParser(readFrom io.Reader) *CommandParser {
	return &CommandParser{
		pool:            NewReadBufferPool(),
		pluggableReader: readFrom,
		NewCommands:     make(chan Command, DEFAULT_COMMAND_PARSER_BACKLOG_SIZE),
		seekingHeader:   true,
	}
}

func (self *CommandParser) Parse() error {
	var remainingBytes []byte
	for {
		remainingBytes, err := self.SeekHeader(remainingBytes)
		if err != nil {
			return err
		}
		hdr, remainingBytes, err := self.parseHeaderFields(remainingBytes)
		if err != nil {
			return err
		}
		cmd, remainingBytes, err := self.parseCommandFromHeader(hdr, remainingBytes)
		if err != nil {
			return err
		}
		self.NewCommands <- cmd
	}
}

func (self *CommandParser) SeekHeader(preReadBytes []byte) ([]byte, error) {
	// Get and clear a readbuf, make sure to re-add readbuf at end
	readBuf, ok := self.pool.Get().(*[1024]byte)
	defer self.pool.Put(readBuf)
	if !ok {
		return nil, NewCommandParserLogicError("ERROR: CommandParser.pool returned a type other than a [1024]byte read buffer")
	}
	*readBuf = [1024]byte{}
	copy((*readBuf)[:], preReadBytes)

	var MAX_PARSED_BYTES_WITHOUT_HEADER uint32 = 1024

	headerFullyRead := false
	doRead := false
	if len(preReadBytes) == 0 {
		doRead = true
	}
	totalBytesRead := 0
	readOffsetIdx := len(preReadBytes)
	startOffset := 0
	endOffset := 0
	for !headerFullyRead {
		// fill up to len(readBuf) bytes of readBuf from pluggableReader
		var bytesRead int = 0
		var err error = nil
		// sometimes we skip the first read (if we passed in some preexisting bytes to parse, parse those first)
		if doRead {
			bytesRead, err = self.pluggableReader.Read(readBuf[readOffsetIdx:])
			totalBytesRead += bytesRead
			if err != nil {
				return nil, err
			}
		}
		endOffset = readOffsetIdx + bytesRead
		if !doRead {
			doRead = true
		}

		// parse over readBuf to find whole MAGIC_BYTES
		for i := 0; i < endOffset-(len(MAGIC_BYTES)-1); i += 1 {
			check := readBuf[i : i+4]
			if check[0] == MAGIC_BYTES[0] &&
				check[1] == MAGIC_BYTES[1] &&
				check[2] == MAGIC_BYTES[2] &&
				check[3] == MAGIC_BYTES[3] {
				// if found whole MAGIC_BYTES, return all bytes read including/after the header
				startOffset = i
				headerFullyRead = true
				break
			}
		}
		if headerFullyRead {
			break
		}

		// if no whole magic bytes, evaluate last 3 bytes for partial MAGIC_BYTES
		// if found, issue additional reads up to the max amount of parsed bytes, prepending
		// the newly-read bytes with the candidates bytes from the last read.
		i := 0
		if i < (endOffset - (len(MAGIC_BYTES) - 1)) {
			i = (endOffset - (len(MAGIC_BYTES) - 1))
		}
		mayBeMessage := false
		for ; i < endOffset; i += 1 {
			check := readBuf[i:endOffset]
			mayBeMessage = true
			for j, byteVal := range check {
				if byteVal != MAGIC_BYTES[j] {
					mayBeMessage = false
					break
				}
			}

			if mayBeMessage {
				// Copy partial MAGIC_BYTES to head of read buffer, and perform an additional read to check
				//  if MAGIC_BYTES ends correctly indicating that the read boundary split a header
				copy(readBuf[:], check)
				readOffsetIdx = len(check)
				break
			}
		}
		if !mayBeMessage {
			readOffsetIdx = 0
		}

		// If we have not yet read out softcap of bytes, then we can issue another read and see if there is
		//  a header in THAT chunk of bytes
		if totalBytesRead < int(MAX_PARSED_BYTES_WITHOUT_HEADER) {
			continue
		}

		// IF WE GET HERE AND HAVE NOT FULLY READ A HEADER, we have gone too far, and we issue
		// a parse error indicating that we're getting garbage from the socket.
		return nil, NewCommandParserParseError(fmt.Sprintf("PARSE ERROR: Read %d bytes (> softcap of %d bytes) into parser and did not locate a message header", totalBytesRead, MAX_PARSED_BYTES_WITHOUT_HEADER))
	}

	out := make([]byte, endOffset-startOffset)
	copy(out, readBuf[startOffset:endOffset])
	return out, nil
}

func (self *CommandParser) parseHeaderFields(preReadBytes []byte) (CmdHdrInfo, []byte, error) {
	hdr := CmdHdrInfo{}

	// Get buffer to read into if we need it
	readBuf, ok := self.pool.Get().(*[1024]byte)
	defer self.pool.Put(readBuf)
	if !ok {
		return hdr, preReadBytes, NewCommandParserLogicError("ERROR: CommandParser.pool returned a type other than a [1024]byte read buffer")
	}
	*readBuf = [1024]byte{}
	copy((*readBuf)[:], preReadBytes)

	doRead := false
	if len(preReadBytes) < int(CMD_HDR_INFO_SIZE_BYTES) {
		doRead = true
	}
	totalBytesRead := 0
	readOffsetIdx := len(preReadBytes)
	endOffset := 0
	for {
		var bytesRead int = 0
		var err error = nil
		// sometimes we skip the first read (if we passed in some preexisting bytes to parse, parse those first)
		if doRead {
			bytesRead, err = self.pluggableReader.Read(readBuf[readOffsetIdx:])
			totalBytesRead += bytesRead
			if err != nil {
				out := make([]byte, endOffset)
				copy(out, readBuf[:endOffset])
				return hdr, out, err
			}
		}
		endOffset = readOffsetIdx + bytesRead
		if endOffset < int(CMD_HDR_INFO_SIZE_BYTES) {
			readOffsetIdx = endOffset
			continue
		}
		break
	}

	// extract fields from header
	copy(hdr.Magic[:], readBuf[:4])
	hdr.MsgLen = binary.BigEndian.Uint16(readBuf[4:6])
	hdr.MsgType = readBuf[6]

	out := make([]byte, endOffset-int(CMD_HDR_INFO_SIZE_BYTES))
	copy(out, readBuf[CMD_HDR_INFO_SIZE_BYTES:endOffset])

	if hdr.MsgLen > uint16(CMD_HDR_INFO_MSGLEN_MAX) {
		return hdr, out, NewCommandParserParseError(fmt.Sprintf("PARSE ERROR: Header.MsgLen of %d greater than max allowable message length of %d", hdr.MsgLen, CMD_HDR_INFO_MSGLEN_MAX))
	}
	if hdr.MsgType >= uint8(MSGTYPE_MAX) {
		return hdr, out, NewCommandParserParseError(fmt.Sprintf("PARSE ERROR: Header.MsgType of %d greater than MSGTYPE_MAX (%d)", hdr.MsgType, MSGTYPE_MAX))
	}

	return hdr, out, nil

}

func (self *CommandParser) parseCommandFromHeader(hdr CmdHdrInfo, remainingBytes []byte) (Command, []byte, error) {
	switch hdr.MsgType {
	case MSGTYPE_TEXT:
		return self.parseTextMessageFromHeader(hdr, remainingBytes)
	default:
		return nil, remainingBytes, NewCommandParserParseError(fmt.Sprintf("PARSE ERROR: Header.MsgType of %d not handled in (*CommandParser).parseCommandFromHeader", hdr.MsgType))
	}
}

func (self *CommandParser) parseTextMessageFromHeader(hdr CmdHdrInfo, remainingBytes []byte) (Command, []byte, error) {
	cmd := &CmdSendTextMessage{}

	// Get buffer to read into if we need it
	readBuf, ok := self.pool.Get().(*[1024]byte)
	defer self.pool.Put(readBuf)
	if !ok {
		return nil, remainingBytes, NewCommandParserLogicError("ERROR: CommandParser.pool returned a type other than a [1024]byte read buffer")
	}
	*readBuf = [1024]byte{}
	copy((*readBuf)[:], remainingBytes)

	doRead := false
	if len(remainingBytes) < int(hdr.MsgLen-uint16(CMD_HDR_INFO_SIZE_BYTES)) {
		doRead = true
	}
	totalBytesRead := 0
	readOffsetIdx := len(remainingBytes)
	endOffset := 0
	for {
		var bytesRead int = 0
		var err error = nil
		// sometimes we skip the first read (if we passed in some preexisting bytes to parse, parse those first)
		if doRead {
			bytesRead, err = self.pluggableReader.Read(readBuf[readOffsetIdx:])
			totalBytesRead += bytesRead
			if err != nil {
				out := make([]byte, endOffset)
				copy(out, readBuf[:endOffset])
				return nil, out, err
			}
		}
		endOffset = readOffsetIdx + bytesRead
		if endOffset < int(hdr.MsgLen-uint16(CMD_HDR_INFO_SIZE_BYTES)) {
			readOffsetIdx = endOffset
			continue
		}
		break
	}

	// extract string as UTF-8 text
	textLen := int(hdr.MsgLen - uint16(CMD_HDR_INFO_SIZE_BYTES))
	cmd.text = string(readBuf[:textLen])

	out := make([]byte, endOffset-textLen)
	copy(out, readBuf[textLen:endOffset])

	return cmd, out, nil

}
