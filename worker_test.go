package conveyor_test

import (
	"bytes"
	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
)

var textToRedact = []string{
	"testmail@test.com",
	"test@mail.de",
	"ullamcorper",
	"Lorem",
}

func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := string(line)

	for _, word := range textToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}

type TestChunkWriter struct {
	Buff *bytes.Buffer
}

func (t *TestChunkWriter) Write(chunk *conveyor.Chunk, buff []byte) error {
	t.Buff.Write(buff)
	return nil
}

func TestWorker(t *testing.T) {
	assertion := assert.New(t)

	out := &TestChunkWriter{&bytes.Buffer{}}
	chunk := conveyor.Chunk{
		Id:   1,
		File: "testdata/data.txt",
		Size: 8000,
		Out:  out,
	}

	expectedChunk := conveyor.Chunk{
		Id:             1,
		File:           "testdata/data.txt",
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
