package serverUtils

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"
)

type NetworkListenerLogicError struct {
	errMesg string
}

func (self *NetworkListenerLogicError) Error() string {
	return self.errMesg
}

func (self *NetworkListenerLogicError) Is(err error) bool {
	_, ok := err.(*NetworkListenerLogicError)
	return ok
}

func NewNetworkListenerLogicError(text string) error {
	return &NetworkListenerLogicError{
		errMesg: text,
	}
}

type NetworkListener struct {
	pool           sync.Pool
	ipaddr         [4]byte
	port           uint16
	listener       net.Listener
	wait           sync.WaitGroup
	NewConnections chan *ManagedConnection
	ErrorState     error
}

var DEFAULT_NEWCONNECTION_NONBLOCKING_BACKLOG_SIZE uint32 = 1

func NewNetworkListener(ip [4]byte, port uint16) *NetworkListener {
	newListener := &NetworkListener{
		pool:           NewReadBufferPool(),
		ipaddr:         ip,
		port:           port,
		listener:       nil,
		wait:           sync.WaitGroup{},
		NewConnections: make(chan *ManagedConnection, DEFAULT_NEWCONNECTION_NONBLOCKING_BACKLOG_SIZE),
	}
	listeningSemaphores := make(chan bool, 1)
	newListener.wait.Go(func() {
		newListener.listen(listeningSemaphores)
	})
	// Wait for initial synchronization step so listener is either in error state or in pending accept state
	// TODO: Log some pertinent information about whether or not listener start succeeded or failed
	_ = <-listeningSemaphores
	return newListener
}

func (self *NetworkListener) GetAddress() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", self.ipaddr[0], self.ipaddr[1], self.ipaddr[2], self.ipaddr[3], self.port)
}

func (self *NetworkListener) listenRaw(outConns chan *ManagedConnection) error {
	if self.listener == nil {
		return NewNetworkListenerLogicError("Cannot call listenRaw() on a NetworkListener with a null listener (self.listener)!")
	}
	if outConns == nil {
		return NewNetworkListenerLogicError("Cannot call listenRaw() on a NetworkListener with no output channel for connections!")
	}
	for {
		newConn, err := self.listener.Accept()
		if err != nil {
			return err
		}

		// Wait on all threads of connection to terminate (pretty immediate after conn.Close called elsewhere)
		managedConn := NewManagedConnection(newConn, &self.pool)
		self.wait.Go(func() {
			managedConn.Wait()
		})

		outConns <- managedConn
	}
}

func (self *NetworkListener) listen(listeningSemaphores chan bool) {
	if self.listener != nil {
		self.ErrorState = NewNetworkListenerLogicError("Cannot call listen() on a NetworkListener with a non-nil listener (self.listener)—already listening!")
		listeningSemaphores <- false
		return
	}
	if self.NewConnections == nil {
		self.ErrorState = NewNetworkListenerLogicError("Cannot call listen() on a NetworkListener with a null output channel (self.NewConnections)—do something with your new connections!")
		listeningSemaphores <- false
		return
	}

	newListener, err := net.Listen("tcp", self.GetAddress())
	if err != nil {
		self.ErrorState = err
		listeningSemaphores <- false
		return
	}
	self.listener = newListener

	for {
		listeningSemaphores <- true
		err = self.listenRaw(self.NewConnections)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			if errors.Is(err, syscall.ECONNABORTED) {
				break
			}
			if errors.Is(err, syscall.EINTR) {
				break
			}
			if errors.Is(err, (*NetworkListenerLogicError)(nil)) {
				break
			}
			err = nil
			continue
		}
	}

	self.listener = nil
	self.ErrorState = err
}

func (self *NetworkListener) Close() error {
	return self.listener.Close()
}

func (self *NetworkListener) Wait() {
	self.wait.Wait()
}

func (self *NetworkListener) IsListening() bool {
	return self.listener != nil
}
