package serverUtils

import "fmt"

type Receiver interface {
	Accept(v Visitor)
}

type Visitor interface {
	Visit(r Receiver)
}

type Command = Visitor

var CMD_HDR_INFO_SIZE_BYTES uint32 = 7
var CMD_HDR_INFO_MSGLEN_MAX uint32 = 1024
var MAGIC_BYTES [4]byte = [4]byte{0xBA, 0x5E, 0xBA, 0x77}

type MessageType = uint8

const (
	MSGTYPE_UNKNOWN = MessageType(iota)
	MSGTYPE_TEXT

	MSGTYPE_MAX
)

type CmdHdrInfo = struct {
	Magic   [4]byte
	MsgLen  uint16
	MsgType MessageType
}

type CmdSendTextMessage struct {
	text string
}

func (self *CmdSendTextMessage) Visit(r Receiver) {
	switch rt := r.(type) {
	case *ManagedConnection:
		{
			_, _ = rt.Write([]byte(self.text))
		}
	case *ServerWorker:
		for conn := range rt.ManagedConnections {
			self.Visit(conn)
		}
	default:
		fmt.Printf("Command has no implemented behavior for receiver type %s\n", rt)
	}
}
