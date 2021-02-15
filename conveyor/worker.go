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
	lineProcessor LineProcessor
	chunkSize     int64

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
func (worker *Worker) Work() {
	defer worker.waitGroup.Done()
	var err error

	for chunk := range worker.TasksChan {
		worker.chunk = &chunk

		err = worker.Process()
		worker.resultChan <- ChunkResult{
			Chunk: *worker.chunk,
			Err:   err,
		}
	}
}

var (
	ErrNotPartialLastLineButIncompleteLine = errors.New("no linebreak but isn't a partial last line")
	ErrPartialOnlyOneIncompleteLine        = errors.New("no linebreak found in buff but partial first line")
)

func (worker *Worker) Process() error {
	defer worker.resetBuffers()

	var err error

	err = worker.prepareFileHandles()
	if err != nil {
		return err
	}

	err = worker.readChunkInBuff()
	if err != nil {
		return err
	}

	err = worker.prepareBuff()
	if err != nil {
		return err
	}

	err = worker.processBuff()
	if err != nil {
		return err
	}

	err = worker.writeOutBuff()
	if err != nil {
		return err
	}

	return nil
}

func (worker *Worker) prepareBuff() error {
	worker.chunk.partialFirstLine = worker.buff[0] != '{'
	worker.chunk.partialLastLine = worker.buff[len(worker.buff)-1] != '\n'

	if worker.chunk.partialFirstLine {
		i := bytes.IndexByte(worker.buff, '\n')
		if i == -1 {
			return ErrPartialOnlyOneIncompleteLine
		}

		worker.buffHead += i + 1
		worker.chunk.RealOffset = worker.chunk.Offset + int64(i)
	}

	if worker.chunk.partialLastLine {
		err := worker.readOverflowInBuff()
		if err != nil {
			return err
		}

		worker.chunk.RealSize += len(worker.overflowBuff)
	}

	return nil
}

// prepareFileHandles creates the main read handle and sets
// the read offset.
func (worker *Worker) prepareFileHandles() (err error) {
	if worker.handle == nil || worker.handle.Name() != worker.chunk.File {
		worker.handle, err = os.Open(worker.chunk.File)
	}

	_, err = worker.handle.Seek(worker.chunk.Offset, io.SeekStart)
	return
}

// resetBuffers extend the size of all buffers to their cap and
// resets all buffer heads.
func (worker *Worker) resetBuffers() {
	worker.buff = worker.buff[:cap(worker.buff)]
	worker.overflowBuff = worker.overflowBuff[:cap(worker.overflowBuff)]
	worker.outBuff = worker.outBuff[:cap(worker.outBuff)]
	worker.buffHead = 0
	worker.outBuffHead = 0
}

// readChunkInBuff reads up to len(worker.buff) bytes from the file.
func (worker *Worker) readChunkInBuff() (err error) {
	worker.chunk.RealSize, err = worker.handle.Read(worker.buff)
	worker.buff = worker.buff[:worker.chunk.RealSize]
	return
}

// readOverflowInBuff reads overflowScanSize chunks until the next
// linebreak has been found.
func (worker *Worker) readOverflowInBuff() error {
	var (
		buffHead = 0
		buffSize = len(worker.overflowBuff)
	)

	for {
		scanBuff := worker.overflowBuff[buffHead:buffSize]

		if _, err := worker.handle.Read(scanBuff); err != nil {
			return err
		}

		i := bytes.IndexByte(scanBuff, '\n')
		if i > 0 {
			worker.overflowBuff = worker.overflowBuff[:buffHead+i]
			break
		}

		buffHead = buffSize
		buffSize += buffSize
		newBuff := make([]byte, buffSize)

		copy(newBuff, worker.overflowBuff)
		worker.overflowBuff = newBuff
	}

	return nil
}

// processBuff converts all the json content in Worker.buff and
// Worker.overflowBuff to csv and safes it into Worker.outBuff
func (worker *Worker) processBuff() error {
	var (
		line            []byte
		noLinebreakLeft bool
		relativeIndex   int
	)

	for {
		relativeIndex = bytes.IndexByte(worker.buff[worker.buffHead:], '\n')
		noLinebreakLeft = relativeIndex == -1

		if noLinebreakLeft {
			if !worker.chunk.partialLastLine {
				return ErrNotPartialLastLineButIncompleteLine
			}

			remainingBuff := worker.buff[worker.buffHead:]
			line = make([]byte, len(remainingBuff)+len(worker.overflowBuff))
			copy(line[:len(remainingBuff)], remainingBuff)
			copy(line[len(remainingBuff):], worker.overflowBuff)

			convertedLine, err := worker.lineProcessor.Process(line)
			if err != nil {
				return fmt.Errorf("processBuff error: %w", err)
			}

			worker.outBuff = worker.outBuff[:worker.outBuffHead+len(convertedLine)]
			worker.chunk.LinesProcessed++

			if len(convertedLine) != 0 {
				copy(worker.outBuff[worker.outBuffHead:], convertedLine)
			}

			break
		}

		convertedLine, err := worker.lineProcessor.Process(worker.buff[worker.buffHead : worker.buffHead+relativeIndex])
		if err != nil {
			return fmt.Errorf("processBuff error: %w", err)
		}

		if len(convertedLine) != 0 {
			copy(worker.outBuff[worker.outBuffHead:], convertedLine)
		}

		worker.outBuffHead += len(convertedLine)
		worker.buffHead += relativeIndex + 1
		worker.chunk.LinesProcessed++

		if worker.buffHead == worker.chunk.RealSize {
			break
		}
	}

	return nil
}

func (worker *Worker) writeOutBuff() (err error) {
	_, err = worker.chunk.out.Write(worker.outBuff)
	return
}
