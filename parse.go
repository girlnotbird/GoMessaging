package gomessaging

import (
	"encoding/binary"
	"fmt"
)

func Parse(bytes []byte) (Message, error) {
	if len(bytes) <= MsgHeadSerialBytes {
		return nil, fmt.Errorf(
			"Cannot Parse() from a bytestream of less than %d bytes (found %d)\n",
			MsgHeadSerialBytes,
			len(bytes),
		)
	}
	head := MsgHead{}
	copy(head.Magic[:], bytes[0:4])
	if head.Magic[0] != MsgMagic[0] ||
		head.Magic[1] != MsgMagic[1] ||
		head.Magic[2] != MsgMagic[2] ||
		head.Magic[3] != MsgMagic[3] {
		return nil, fmt.Errorf("Cannot Parse() from a bytestream that does not start with MSG_MAGIC!")
	}
	head.MsgLen = binary.BigEndian.Uint16(bytes[4:6])
	head.MsgType = binary.BigEndian.Uint16(bytes[6:8])

	var msg Message
	switch head.MsgType {
	case TypeSendText:
		msg = &MsgSendText{}
		tm := msg.(*MsgSendText)
		tm.Text = string(bytes[8:head.MsgLen])
	default:
		return nil, fmt.Errorf(
			"Cannot Parse() from a bytestream with MsgType %d (expected less than %d)",
			head.MsgType,
			TypeMax,
		)
	}

	return msg, nil
}
