package conveyor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// All buffs and handles are kept allocated for all iterations of Worker.Process.
type Worker struct {
	TasksChan     chan Chunk
	resultChan    chan ChunkResult
	waitGroup     *sync.WaitGroup
	chunkSize     int64
	lineProcessor LineProcessor

	handle       *os.File
	chunk        *Chunk
	buff         []byte
	overflowBuff []byte
	outBuff      []byte
	buffHead     int
	outBuffHead  int
}

// NewWorker returns a new Worker
func NewWorker(tasks chan Chunk, result chan ChunkResult, lineProcessor LineProcessor, chunkSize, overflowScanSize int64, waitGroup *sync.WaitGroup) *Worker {
	return &Worker{
		TasksChan:     tasks,
		resultChan:    result,
		waitGroup:     waitGroup,
		chunkSize:     chunkSize,
		lineProcessor: lineProcessor,
		buff:          make([]byte, chunkSize),
		outBuff:       make([]byte, chunkSize),
		overflowBuff:  make([]byte, overflowScanSize),
		buffHead:      0,
		outBuffHead:   0,
	}
}

// Work processes chunks from Worker.TasksChan until queue is empty
func (w *Worker) Work() {
	defer w.waitGroup.Done()
	var err error

	for chunk := range w.TasksChan {
		w.chunk = &chunk

		err = w.Process()
		w.resultChan <- ChunkResult{
			Chunk: *w.chunk,
			Err:   err,
		}
	}
}

var (
	ErrNoLinebreakInChunk = errors.New("no linebreak found in buff")
)

func (w *Worker) Process() error {
	defer w.resetBuffers()

	err := w.prepareFileHandles()
	if err != nil {
		return err
	}

	err = w.readChunkInBuff()
	if err != nil {
		return err
	}

	err = w.prepareBuff()
	if err != nil {
		return err
	}

	err = w.processBuff()
	if err != nil {
		return err
	}

	err = w.writeOutBuff()
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) prepareBuff() error {
	if w.chunk.Offset != 0 {
		i := bytes.IndexByte(w.buff, '\n')
		if i == -1 {
			return ErrNoLinebreakInChunk
		}

		w.buffHead += i + 1
		w.chunk.RealOffset = w.chunk.Offset + int64(i)
	}

	err := w.readOverflowInBuff()
	if err != nil {
		return err
	}

	w.chunk.RealSize += len(w.overflowBuff)

	return nil
}

// prepareFileHandles creates the main read handle and sets
// the read offset.
func (w *Worker) prepareFileHandles() (err error) {
	if w.handle == nil || w.handle.Name() != w.chunk.File {
		w.handle, err = os.Open(w.chunk.File)
	}

	_, err = w.handle.Seek(w.chunk.Offset, io.SeekStart)
	return
}

// resetBuffers extend the size of all buffers to their cap and
// resets all buffer heads.
func (w *Worker) resetBuffers() {
	w.buff = w.buff[:cap(w.buff)]
	w.overflowBuff = w.overflowBuff[:cap(w.overflowBuff)]
	w.outBuff = w.outBuff[:cap(w.outBuff)]
	w.buffHead = 0
	w.outBuffHead = 0
}

// readChunkInBuff reads up to len(worker.buff) bytes from the file.
func (w *Worker) readChunkInBuff() (err error) {
	w.chunk.RealSize, err = w.handle.Read(w.buff)
	w.buff = w.buff[:w.chunk.RealSize]
	return
}

// readOverflowInBuff reads chunks of size overflowScanSize until the next
// linebreak has been found.
func (w *Worker) readOverflowInBuff() error {
	var (
		buffHead = 0
		buffSize = len(w.overflowBuff)
	)

	for {
		scanBuff := w.overflowBuff[buffHead:buffSize]

		if _, err := w.handle.Read(scanBuff); err != nil {
			return err
		}

		i := bytes.IndexByte(scanBuff, '\n')
		if i > 0 {
			w.overflowBuff = w.overflowBuff[:buffHead+i]
			break
		}

		buffHead = buffSize
		buffSize += buffSize
		newBuff := make([]byte, buffSize)

		copy(newBuff, w.overflowBuff)
		w.overflowBuff = newBuff
	}

	return nil
}

// processBuff converts all the json content in Worker.buff and
// Worker.overflowBuff to csv and safes it into Worker.outBuff
func (w *Worker) processBuff() error {
	var (
		line            []byte
		noLinebreakLeft bool
		relativeIndex   int
	)

	for {
		relativeIndex = bytes.IndexByte(w.buff[w.buffHead:], '\n')
		noLinebreakLeft = relativeIndex == -1

		if noLinebreakLeft {

			remainingBuff := w.buff[w.buffHead:]
			line = make([]byte, len(remainingBuff)+len(w.overflowBuff))
			copy(line[:len(remainingBuff)], remainingBuff)
			copy(line[len(remainingBuff):], w.overflowBuff)

			convertedLine, err := w.lineProcessor.Process(line)
			if err != nil {
				return fmt.Errorf("processBuff error: %w", err)
			}

			w.outBuff = w.outBuff[:w.outBuffHead+len(convertedLine)]
			w.chunk.LinesProcessed++

			if len(convertedLine) != 0 {
				copy(w.outBuff[w.outBuffHead:], convertedLine)
			}

			break
		}

		convertedLine, err := w.lineProcessor.Process(w.buff[w.buffHead : w.buffHead+relativeIndex])
		if err != nil {
			return fmt.Errorf("processBuff error: %w", err)
		}

		if len(convertedLine) != 0 {
			copy(w.outBuff[w.outBuffHead:], convertedLine)
		}

		w.outBuffHead += len(convertedLine)
		w.buffHead += relativeIndex + 1
		w.chunk.LinesProcessed++

		if w.buffHead == w.chunk.RealSize {
			break
		}
	}

	return nil
}

func (w *Worker) writeOutBuff() (err error) {
	_, err = w.chunk.out.Write(w.outBuff)
	return
}
