package conveyor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Chosen by a fair dice roll.
const DefaultOverflowScanSize = 1024

var (
	ErrNoLinebreakInChunk = errors.New("no linebreak found in buff")
)

// All buffs and handles are kept allocated for all iterations of Worker.Process.
type Worker struct {
	Id            int
	TasksChan     chan Chunk
	resultChan    chan ChunkResult
	waitGroup     *sync.WaitGroup
	chunkSize     int64
	lineProcessor LineProcessor

	handle       io.ReadSeekCloser
	handleName   string
	chunk        *Chunk
	chunkResult  *ChunkResult
	buff         []byte
	overflowBuff []byte
	outBuff      []byte

	buffHead         int
	overflowBuffHead int
	outBuffHead      int
}

// NewWorker returns a new Worker
func NewWorker(
	id int,
	tasks chan Chunk,
	result chan ChunkResult,
	lineProcessor LineProcessor,
	chunkSize int64,
	overflowScanSize int,
	waitGroup *sync.WaitGroup,
) *Worker {
	return &Worker{
		Id:            id,
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

	for chunk := range w.TasksChan {
		w.chunk = &chunk
		w.chunkResult = &ChunkResult{Chunk: chunk}

		w.chunkResult.Err = w.Process()

		w.resultChan <- *w.chunkResult
	}
}

func (w *Worker) Process() error {
	defer w.resetBuffers()

	err := w.prepareFileHandles()
	if err != nil {
		return fmt.Errorf("error while preparing file handles: %w", err)
	}

	err = w.readChunkInBuff()
	if err != nil {
		return fmt.Errorf("error while reading Chunk in buff: %w", err)
	}

	err = w.prepareBuff()
	if err != nil {
		return fmt.Errorf("error while preparing buff: %w", err)
	}

	err = w.processBuff()
	if err != nil {
		return fmt.Errorf("error while processing buff: %w", err)
	}

	err = w.writeOutBuff()
	if err != nil {
		return fmt.Errorf("error while writing output: %w", err)
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
		w.chunkResult.RealOffset = w.chunk.Offset + int64(i)
	}

	if !w.chunkResult.EOF {
		err := w.readOverflowInBuff()
		if err != nil {
			return err
		}

		w.chunkResult.RealSize += len(w.overflowBuff)
	}

	return nil
}

// prepareFileHandles creates the main read handle and sets
// the read offset.
func (w *Worker) prepareFileHandles() (err error) {
	if w.handle == nil || w.chunk.In.GetHandleID() != w.handleName {
		w.handle, err = w.chunk.In.OpenHandle()
		if err != nil {
			return
		}
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
	w.overflowBuffHead = 0
}

// readChunkInBuff reads up to len(worker.buff) bytes from the file.
func (w *Worker) readChunkInBuff() (err error) {
	w.chunkResult.RealSize, err = w.handle.Read(w.buff)

	if w.chunkResult.RealSize != w.chunk.Size {
		w.buff = w.buff[:w.chunkResult.RealSize]
		w.chunkResult.EOF = true
	}

	return
}

// readOverflowInBuff reads chunks of size DefaultOverflowScanSize until the next
// linebreak has been found.
func (w *Worker) readOverflowInBuff() error {
	buffSize := len(w.overflowBuff)

	for {
		scanBuff := w.overflowBuff[w.overflowBuffHead:]

		if _, err := w.handle.Read(scanBuff); err != nil {
			return err
		}

		i := bytes.IndexByte(scanBuff, '\n')
		if i > 0 {
			w.overflowBuffHead += i
			w.overflowBuff = w.overflowBuff[:w.overflowBuffHead]
			break
		}

		w.overflowBuffHead = buffSize
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
	var relativeIndex int

	for {
		relativeIndex = bytes.IndexByte(w.buff[w.buffHead:], '\n')

		if relativeIndex == -1 {
			if err := w.processOverflowLine(); err != nil {
				return fmt.Errorf("error while processing last Line of Chunk: %w", err)
			}

			break
		}

		if err := w.processLine(relativeIndex + 1); err != nil {
			return fmt.Errorf("error while processing Line of Chunk: %w", err)
		}

		if w.buffHead == w.chunkResult.RealSize {
			break
		}
	}

	return nil
}

func (w *Worker) processLine(relativeIndex int) error {
	line := w.buff[w.buffHead : w.buffHead+relativeIndex]
	convertedLine, err := w.lineProcessor.Process(
		line, LineMetadata{
			Line:  w.chunkResult.Lines + 1,
			Chunk: w.chunk,
		},
	)

	if err != nil {
		return err
	}

	w.addToOutBuff(convertedLine)

	w.buffHead += relativeIndex
	w.chunkResult.Lines++
	return nil
}

func (w *Worker) processOverflowLine() error {
	remainingBuff := w.buff[w.buffHead:]
	line := make([]byte, len(remainingBuff)+w.overflowBuffHead)
	copy(line[:len(remainingBuff)], remainingBuff)
	copy(line[len(remainingBuff):], w.overflowBuff)

	convertedLine, err := w.lineProcessor.Process(
		line,
		LineMetadata{
			WorkerId: w.Id,
			Line:     w.chunkResult.Lines + 1,
			Chunk:    w.chunk,
		},
	)
	if err != nil {
		return err
	}

	w.addToOutBuff(convertedLine)

	w.chunkResult.Lines++
	return nil
}

func (w *Worker) addToOutBuff(b []byte) {
	if len(b) == 0 {
		return
	}

	if w.outBuffHead+len(b) > len(w.outBuff) {
		w.outBuff = append(w.outBuff[:w.outBuffHead], b...)
		w.outBuff = w.outBuff[:cap(w.outBuff)]
	} else {
		copy(w.outBuff[w.outBuffHead:], b)
	}

	w.outBuffHead += len(b)
}

func (w *Worker) writeOutBuff() (err error) {
	if w.outBuffHead > 0 && w.chunk.Out != nil {
		outBuff := w.outBuff[:w.outBuffHead]
		err = w.chunk.Out.Write(w.chunk, outBuff)
	}

	return
}
