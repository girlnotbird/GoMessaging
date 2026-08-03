package main

import (
	"encoding/binary"
	"errors"
	"net"

	shared "github.com/girlnotbird/gomessaging"
)

type Client struct {
	c        net.Conn
	addr     string
	name     string
	Messages chan shared.Message
}

func (self *Client) Join(addr string, name string) error {
	if self == nil {
		return errors.New("Cannot Join() from a nil *Client!")
	}
	if self.c != nil {
		return errors.New("Cannot Join() from a *Client that is already connected to a Host!")
	}
	self.name = name
	self.addr = addr
	newConn, err := net.Dial("tcp4", addr)
	if err != nil {
		self.name = ""
		self.addr = ""
		return err
	}
	self.c = newConn
	return nil
}

func (self *Client) Send(text string) error {
	if self == nil {
		return errors.New("Cannot Send() from a nil *Client!")
	}
	if self.c == nil {
		return errors.New("Cannot Send() from a *Client that is not connected to a Host!")
	}

	outBuf := make([]byte, len(text)+8)
	copy(outBuf[:4], shared.MsgMagic[:])
	binary.BigEndian.PutUint16(outBuf[4:6], uint16(len(text)+8))
	binary.BigEndian.PutUint16(outBuf[6:8], shared.TypeSendText)
	copy(outBuf[8:], []byte(text))

	_, err := (self.c).Write(outBuf)
	if err != nil {
		return err
	}
	return nil
}

func (self *Client) Read() (shared.Message, error) {
	if self == nil {
		return nil, errors.New("Cannot Read() from a nil *Client!")
	}
	if self.c == nil {
		return nil, errors.New("Cannot Read() from a *Client that is not connected to a Host!")
	}
	buf := make([]byte, 1024)
	bytesRead, err := (self.c).Read(buf)
	if err != nil {
		return nil, err
	}
	msg, err := self.Parse(buf[:bytesRead])
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (self *Client) Parse(bytes []byte) (shared.Message, error) {
	return shared.Parse(bytes)
}

func (self *Client) Close() error {
	err := (self.c).Close()
	self.addr = ""
	self.name = ""
	self.c = nil
	return err
}
