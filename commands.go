package gomessaging

type MsgType = uint16
type Magic = [4]byte

const MsgHeadSerialBytes = int(8)

var MsgMagic Magic = Magic{0xBA, 0x5E, 0xBA, 0x77}

type MsgHead struct {
	Magic   Magic
	MsgLen  uint16
	MsgType uint16
}

type Message interface {
	isMessage() bool
}

const (
	TypeSendText MsgType = iota

	TypeMax
)

type MsgSendText struct {
	Text string
}

func (*MsgSendText) isMessage() bool { return true }
