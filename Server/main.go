package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	serverUtils "github.com/girlnotbird/GoMessagingShared"
)

type WritesHeaders struct {
	mu     sync.Mutex
	Closed bool
}

func (self *WritesHeaders) Read(b []byte) (int, error) {
	if !self.Closed {
		self.mu.Lock()
		defer self.mu.Unlock()
		fmt.Println("Read bytes from WritesHeaders!")
		b[0] = serverUtils.MAGIC_BYTES[0]
		b[1] = serverUtils.MAGIC_BYTES[1]
		b[2] = serverUtils.MAGIC_BYTES[2]
		b[3] = serverUtils.MAGIC_BYTES[3]
		binary.BigEndian.PutUint16(b[4:6], 18)
		b[6] = serverUtils.MSGTYPE_TEXT
		copy(b[7:18], []byte("Hello World"))
		return 18, nil
	}
	return 0, io.EOF
}

func (self *WritesHeaders) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.Closed = true
	return nil
}

func main() {
	Readable := &WritesHeaders{}
	Parser := serverUtils.NewCommandParser(Readable)
	// Readable.Close()
	// Parser.Parse()
	wg := sync.WaitGroup{}
	Finished := make(chan bool)
	wg.Go(func() {
		Finished <- (Parser.Parse() != nil)
	})

	done := false
	doner := false
	for !done || !doner {
		select {
		case <-Finished:
			{
				doner = true
			}
		default:
			{
				select {
				case cmd := <-Parser.NewCommands:
					{
						Readable.Close()
						fmt.Printf("%v\n", cmd)
						done = false
					}
				default:
					{
						done = true
					}
				}
			}
		}
	}
	wg.Wait()
}
