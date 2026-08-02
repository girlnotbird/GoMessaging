package serverUtils

import (
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"syscall"
)

type ReadBuf = [1024]byte

func NewReadBufferPool() sync.Pool {
	return sync.Pool{
		New: func() any {
			return &ReadBuf{}
		},
	}
}

func GetBuffer(bufs *sync.Pool) (*ReadBuf, error) {
	poolObj := bufs.Get()
	buf, ok := poolObj.(*ReadBuf)
	if !ok {
		return nil, fmt.Errorf("ERROR: Pulled an invalid buffer type out of the buffer pool in listenToConn(): %s", reflect.TypeOf(poolObj))
	}
	return buf, nil
}

func listenToConn(conn net.Conn, bufs *sync.Pool, dst *io.PipeWriter) error {
	defer (conn).Close()

	// Get a fixed-size buffer from the pool to read into
	buf, err := GetBuffer(bufs)
	if err != nil {
		return err
	}

	// loop blocking reads on conn; returns io.EOF or syscall.ECONNRESET when conn closed
	//  automatically terminates connection if it reads more than a kilobyte in one pass
	for {
		// Compiler optimizes this into a memory-clearing instruction; clears the buffer for a new read from conn
		*buf = ReadBuf{}

		// Blocks until conn closes or read becomes available
		bytesRead, err := (conn).Read((*buf)[:])

		// Terminates on connection close (stdlib handles temporary interrupts without returning error)
		if err != nil && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
			return io.EOF
		}

		// Terminates on too large of a packet sent without acknowledgement from listener
		if bytesRead >= len(*buf) {
			return io.EOF
		}

		// Exfiltrates data from goroutine to parser using a blocking write mechanism
		dst.Write((*buf)[:bytesRead])
	}
}

func writeToConn(conn net.Conn, bufs *sync.Pool, src *io.PipeReader) error {
	defer (conn).Close()

	// Get a fixed-size buffer from the pool to read src bytes into
	buf, err := GetBuffer(bufs)
	if err != nil {
		return err
	}

	for {
		// Compiler optimizes this into a memory-clearing instruction; clears the buffer for a new read from src
		*buf = ReadBuf{}

		// Blocks until src closes or read becomes available
		bytesRead, err := src.Read((*buf)[:])

		if err != nil {
			// Any internal error from the writer should terminate the connection
			return err
		}

		// Writes buffered chunks of 1kB to the connection at a time
		(conn).Write((*buf)[:bytesRead])
	}
}

type ManagedConnection struct {
	conn   net.Conn
	pool   *sync.Pool
	wlock  sync.Mutex
	writer *io.PipeWriter
	rlock  sync.Mutex
	reader *io.PipeReader
	wait   sync.WaitGroup
}

func (self *ManagedConnection) runReaderThread(out *io.PipeWriter) {
	if self == nil {
		return
	}
	self.wait.Go(func() {
		listenToConn(self.conn, self.pool, out)
	})
}

func (self *ManagedConnection) runWriterThread(in *io.PipeReader) {
	if self == nil {
		return
	}
	self.wait.Go(func() {
		writeToConn(self.conn, self.pool, in)
	})
}

func (self *ManagedConnection) Write(buf []byte) (int, error) {
	if self == nil {
		return 0, errors.New("Cannot call Write([]byte) on a nil *ManagedConnection")
	}
	self.wlock.Lock()
	defer self.wlock.Unlock()
	return self.writer.Write(buf)
}

func (self *ManagedConnection) Read(buf []byte) (int, error) {
	if self == nil {
		return 0, errors.New("Cannot call Read([]byte) on a nil *ManagedConnection")
	}
	self.rlock.Lock()
	defer self.rlock.Unlock()
	return self.reader.Read(buf)
}

func (self *ManagedConnection) Wait() {
	if self == nil {
		return
	}
	self.wait.Wait()
}

func (self *ManagedConnection) Close() error {
	if self == nil {
		return errors.New("Cannot call Close() on a nil *ManagedConnection—there is nothing to close")
	}
	return self.conn.Close()
}

func (self *ManagedConnection) Accept(v Visitor) {
	v.Visit(self)
}

func NewManagedConnection(conn net.Conn, pool *sync.Pool) *ManagedConnection {
	if conn == nil {
		return nil
	}
	bytesInReader, bytesInWriter := io.Pipe()
	bytesOutReader, bytesOutWriter := io.Pipe()
	mc := &ManagedConnection{
		conn:   conn,
		pool:   pool,
		writer: bytesInWriter,
		reader: bytesOutReader,
	}
	mc.runWriterThread(bytesInReader)
	mc.runReaderThread(bytesOutWriter)
	return mc
}
