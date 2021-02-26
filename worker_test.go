package conveyor_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"sync"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	assertion := assert.New(t)

	out := &TestChunkWriter{&bytes.Buffer{}}
	chunk := conveyor.Chunk{
		Id:   1,
		In:   &conveyor.FileReader{Filename: "testdata/data.txt"},
		Size: 8000,
		Out:  out,
	}

	expectedChunk := conveyor.Chunk{
		Id:             1,
		In:             &conveyor.FileReader{Filename: "testdata/data.txt"},
		Offset:         0,
		Size:           8000,
		RealSize:       6936,
		RealOffset:     0,
		LinesProcessed: 100,
		EOF:            true,
		Out:            out,
	}

	tasks := make(chan conveyor.Chunk, 1)
	tasks <- chunk
	close(tasks)

	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	conveyor.NewWorker(
		tasks,
		results,
		conveyor.LineProcessorFunc(Redact),
		8000,
		1024,
		wg,
	).Work()

	close(results)

	var result conveyor.ChunkResult
	for item := range results {
		result = item
	}

	assertion.Equal(expectedChunk, result.Chunk)

	actualFile, err := ioutil.ReadFile("testdata/converted_data.txt")
	assertion.NoError(err)

	assertion.Equal(actualFile, out.Buff.Bytes())
}

func TestWorkerReturnsInvalidLineProcessor(t *testing.T) {
	assertion := assert.New(t)

	chunk := conveyor.Chunk{
		Id:   1,
		In:   &conveyor.FileReader{Filename: "testdata/data.txt"},
		Size: 8000,
		Out:  nil,
	}

	expectedErr := errors.New("test error")

	tasks := make(chan conveyor.Chunk, 1)
	tasks <- chunk
	close(tasks)

	results := make(chan conveyor.ChunkResult, 1)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	conveyor.NewWorker(
		tasks,
		results,
		conveyor.LineProcessorFunc(func(i []byte, metadata conveyor.LineMetadata) ([]byte, error) {
			return i, expectedErr
		}),
		8000,
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
