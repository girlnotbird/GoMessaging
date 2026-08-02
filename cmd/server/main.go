package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	shared "github.com/girlnotbird/gomessaging"
)

func main() {
	listener, err := net.Listen("tcp4", "127.0.0.1:"+os.Getenv("PORT"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Now Listening on %v...\n", "127.0.0.1:"+os.Getenv("PORT"))

	wg := sync.WaitGroup{}
	mu := sync.RWMutex{}
	activeConns := make(map[net.Conn]bool)
	countConns := 0

	Broadcast := func(msg *shared.Msg_SENDTEXT, src net.Conn) {
		clientsToDelete := make(map[net.Conn]bool)
		for activeClient, _ := range activeConns {
			mu.RLock()
			if activeClient != src {
				_, err := activeClient.Write(MakeMessage(msg.Text, activeClient))
				if err != nil {
					fmt.Printf("Lost connection to %v\n", activeClient.RemoteAddr())
					clientsToDelete[activeClient] = true
				}
			}
			mu.RUnlock()
			if len(clientsToDelete) != 0 {
				mu.Lock()
				for ctd, _ := range clientsToDelete {
					ctd.Close()
					countConns -= 1
					if countConns == 0 {
						listener.Close()
					}
				}
				mu.Unlock()
			}
		}
	}

	ListenToNewConnection := func(conn net.Conn) {
		onError := func() {
			fmt.Printf("Lost connection to %v\n", conn.RemoteAddr())
			conn.Close()
			mu.Lock()
			countConns -= 1
			if countConns == 0 {
				listener.Close()
			}
			mu.Unlock()
		}
		buf := make([]byte, 1024)
		for {
			bytesRead, err := conn.Read(buf)
			if err != nil {
				onError()
				return
			}

			msg, err := shared.Parse(buf[:bytesRead])
			if err != nil {
				onError()
				return
			}

			switch msg := msg.(type) {
			case *shared.Msg_SENDTEXT:
				Broadcast(msg, conn)
			default:
				onError()
				return
			}
		}
	}

	ListenForNewConnections := func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					break
				}
				log.Fatal(err)
			}
			fmt.Printf("Connected to %v\n", conn.RemoteAddr())
			mu.Lock()
			countConns += 1
			activeConns[conn] = true
			wg.Go(func() { ListenToNewConnection(conn) })
			mu.Unlock()
		}
	}

	// Listen and add connections to map
	wg.Go(ListenForNewConnections)

	wg.Wait()
	fmt.Printf("All clients closed. Goodbye!\n")
}

func MakeMessage(text string, src net.Conn) []byte {
	remoteAddr := src.RemoteAddr().String()
	payload := fmt.Sprintf("%s: %s", remoteAddr, text)
	outBuf := make([]byte, len(payload)+8)
	copy(outBuf[:4], shared.MSG_MAGIC[:])
	binary.BigEndian.PutUint16(outBuf[4:6], uint16(len(payload))+8)
	binary.BigEndian.PutUint16(outBuf[6:8], shared.MSGTYPE_SENDTEXT)
	copy(outBuf[8:], []byte(payload))
	return outBuf
}
