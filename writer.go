package conveyor

import (
	"io"
	"sync"
)

// The ConcurrentWriter type is a thread-safe wrapper for
// io.Writer which is able to keep the order of lines across all chunks.
type ConcurrentWriter struct {
	handle io.Writer

	keepOrder        bool
	lastChunkWritten int
	cache            map[int][]byte
	firstWrite       bool

	sync.Mutex
}

// NewConcurrentWriter returns a new ConcurrentWriter
func NewConcurrentWriter(writer io.Writer, keepOrder bool) *ConcurrentWriter {
	return &ConcurrentWriter{
		keepOrder:  keepOrder,
		handle:     writer,
		cache:      make(map[int][]byte),
		firstWrite: true,
	}
}

func (c *ConcurrentWriter) Write(chunk *Chunk, buff []byte) error {
	c.Lock()
	defer c.Unlock()

	if !c.keepOrder {
		return c.writeBuff(buff)
	}

	c.addToCache(chunk.Id, buff)
	return c.writeCache()
}

func (c *ConcurrentWriter) addToCache(id int, buff []byte) {
	c.cache[id] = make([]byte, len(buff))
	copy(c.cache[id], buff)
}

func (c *ConcurrentWriter) writeCache() error {
	for {
		currentIndex := c.lastChunkWritten + 1
		buff, set := c.cache[currentIndex]
		if !set {
			return nil
		}

		if err := c.writeBuff(buff); err != nil {
			return err
		}

		delete(c.cache, currentIndex)
		c.lastChunkWritten++
	}
}

func (c *ConcurrentWriter) writeBuff(buff []byte) error {
	if len(buff) == 0 {
		return nil
	}

	if c.firstWrite {
		c.firstWrite = false
	} else {
		if _, err := c.handle.Write([]byte{'\n'}); err != nil {
			return err
		}
	}

	if _, err := c.handle.Write(buff); err != nil {
		return err
	}

	return nil
}
