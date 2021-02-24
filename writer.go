package conveyor

import (
	"io"
	"sync"
)

type ConcurrentWriter struct {
	handle io.Writer

	keepOrder        bool
	lastChunkWritten int
	cache            map[int][]byte

	sync.Mutex
}

func NewConcurrentWriter(writer io.Writer, keepOrder bool) *ConcurrentWriter {
	return &ConcurrentWriter{
		keepOrder: keepOrder,
		handle:    writer,
		cache:     make(map[int][]byte),
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

	if _, err := c.handle.Write(buff); err != nil {
		return err
	}

	if _, err := c.handle.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}
