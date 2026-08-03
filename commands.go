package gomessaging

type MsgType = uint16
type Magic = [4]byte

const MSGHEAD_SERIAL_BYTES = int(8)

var MSG_MAGIC Magic = Magic{0xBA, 0x5E, 0xBA, 0x77}

type MsgHead struct {
	Magic   Magic
	MsgLen  uint16
	MsgType uint16
}

type Message interface {
	IsMessage() bool
}

const (
	MSGTYPE_SENDTEXT MsgType = iota

	MSGTYPE_MAX
)

type Msg_SENDTEXT struct {
	Text string
}

func (*Msg_SENDTEXT) IsMessage() bool { return true }
