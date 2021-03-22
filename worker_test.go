package conveyor_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func GetSingleChunkChan(file string, size int, out conveyor.ChunkWriter) chan conveyor.Chunk {
	tasks := make(chan conveyor.Chunk, 1)

	tasks <- conveyor.Chunk{
		Id:   1,
		In:   &conveyor.FileReader{FilePath: file},
		Size: size,
		Out:  out,
	}

	close(tasks)
	return tasks
}

func TestWorkerReturnsErrorFromLineProcessor(t *testing.T) {
	assertion := assert.New(t)
	expectedErr := errors.New("test error")
	chunkSize := 8000

	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	conveyor.NewWorker(
		GetSingleChunkChan("testdata/data.txt", chunkSize, nil),
		results,
		conveyor.LineProcessorFunc(func(i []byte, metadata conveyor.LineMetadata) ([]byte, error) {
			return i, expectedErr
		}),
		int64(chunkSize),
		1024,
		wg,
	).Work()

	close(results)

	var result conveyor.ChunkResult
	for item := range results {
		result = item
	}

	assertion.ErrorIs(result.Err, expectedErr)
}

func TestWorkerReturnsErrorFromLastLineProcessed(t *testing.T) {
	assertion := assert.New(t)
	expectedErr := errors.New("test error")
	chunkSize := 6900

	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	errLine := 100
	conveyor.NewWorker(
		GetSingleChunkChan("testdata/data.txt", chunkSize, nil),
		results,
		conveyor.LineProcessorFunc(func(i []byte, metadata conveyor.LineMetadata) ([]byte, error) {
			if metadata.Line == errLine {
				return nil, expectedErr
			}

			return i, nil
		}),
		int64(chunkSize),
		1024,
		wg,
	).Work()

	close(results)

	var result conveyor.ChunkResult
	for item := range results {
		result = item
	}

	assertion.ErrorIs(result.Err, expectedErr)
}

func TestWorkerFailsForInvalidOutputHandle(t *testing.T) {
	assertion := assert.New(t)
	tWriter := &TestWriter{FailAt: 0}
	chunkSize := 8000

	tasks := GetSingleChunkChan("testdata/data.txt", chunkSize, tWriter)
	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	conveyor.NewWorker(tasks, results, NullLineProcessor, int64(chunkSize), 1024, wg).Work()

	close(results)
	wg.Wait()

	for result := range results {
		assertion.False(result.Ok())
		assertion.ErrorIs(result.Err, ErrInvalidWrite)
	}
}

func TestWorkerFailsForReadAfterSeek(t *testing.T) {
	assertion := assert.New(t)
	tWriter := &TestWriter{FailAt: 0}
	chunkSize := 8000

	tasks := make(chan conveyor.Chunk, 1)

	tasks <- conveyor.Chunk{
		Id:   1,
		In:   &FailureFileReader{Filename: "Test"},
		Size: chunkSize,
		Out:  tWriter,
	}

	close(tasks)

	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	conveyor.NewWorker(tasks, results, NullLineProcessor, int64(chunkSize), 1024, wg).Work()

	close(results)
	wg.Wait()

	for result := range results {
		assertion.False(result.Ok())
		assertion.ErrorIs(result.Err, ErrInvalidRead)
	}
}

func TestWorkerFailsOverflowBuff(t *testing.T) {
	assertion := assert.New(t)
	chunkSize := 6900

	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	tasks := make(chan conveyor.Chunk, 1)

	tasks <- conveyor.Chunk{
		Id: 1,
		In: &FailureAtReader{
			Filename: "testdata/data.txt",
			FailAt:   1,
		},
		Size: chunkSize,
		Out:  nil,
	}

	close(tasks)

	conveyor.NewWorker(
		tasks,
		results,
		NullLineProcessor,
		int64(chunkSize),
		1024,
		wg,
	).Work()

	close(results)
	wg.Wait()

	for result := range results {
		assertion.False(result.Ok())
		assertion.ErrorIs(result.Err, ErrInvalidRead)
	}
}
