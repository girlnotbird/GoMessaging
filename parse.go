package gomessaging

import (
	"encoding/binary"
	"fmt"
)

func Parse(bytes []byte) (Message, error) {
	if len(bytes) <= MSGHEAD_SERIAL_BYTES {
		return nil, fmt.Errorf(
			"Cannot Parse() from a bytestream of less than %d bytes (found %d)\n",
			MSGHEAD_SERIAL_BYTES,
			len(bytes),
		)
	}
	head := MsgHead{}
	copy(head.Magic[:], bytes[0:4])
	if head.Magic[0] != MSG_MAGIC[0] ||
		head.Magic[1] != MSG_MAGIC[1] ||
		head.Magic[2] != MSG_MAGIC[2] ||
		head.Magic[3] != MSG_MAGIC[3] {
		return nil, fmt.Errorf("Cannot Parse() from a bytestream that does not start with MSG_MAGIC!")
	}
	head.MsgLen = binary.BigEndian.Uint16(bytes[4:6])
	head.MsgType = binary.BigEndian.Uint16(bytes[6:8])

	var msg Message
	switch head.MsgType {
	case MSGTYPE_SENDTEXT:
		msg = &Msg_SENDTEXT{}
		tm := msg.(*Msg_SENDTEXT)
		tm.Text = string(bytes[8:head.MsgLen])
	default:
		return nil, fmt.Errorf(
			"Cannot Parse() from a bytestream with MsgType %d (expected less than %d)",
			head.MsgType,
			MSGTYPE_MAX,
		)
	}

	return msg, nil
}
