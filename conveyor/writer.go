package conveyor

import (
	"io"
	"sync"
)

type ConcurrentWriter struct {
	keepOrder        bool
	lastChunkWritten int
	firstWrite       bool
	handle           io.Writer
	sync.Mutex
}

func NewConcurrentWriter(writer io.Writer, keepOrder bool) *ConcurrentWriter {
	return &ConcurrentWriter{
		keepOrder:  keepOrder,
		handle:     writer,
		firstWrite: true,
	}
}

func (c *ConcurrentWriter) WriteBuff(chunk *Chunk, buff []byte) error {
	c.Lock()
	defer c.Unlock()

	var err error

	if c.firstWrite {
		_, err = c.handle.Write(buff[1:len(buff)-1])
		c.firstWrite = false
	} else {
		_, err = c.handle.Write(buff)
	}

	return err
}
