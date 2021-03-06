package conveyor_test

import (
	"bytes"
	"errors"
	"log"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func TestLogChunkResult(t *testing.T) {
	assertion := assert.New(t)

	loggerOutput := bytes.Buffer{}
	errorLoggerOutput := bytes.Buffer{}

	queue := conveyor.NewQueue(
		generateTestChunks(100, 1024, chunkTestFile),
		1,
		nil,
		&conveyor.QueueOpts{
			Logger:    log.New(&loggerOutput, "", 0),
			ErrLogger: log.New(&errorLoggerOutput, "", 0),
		},
	)

	tt := []struct {
		ChunkResult         conveyor.ChunkResult
		currentChunkCount   int
		ExpectedOutput      string
		ExpectedErrorOutput string
	}{
		{
			ChunkResult:       conveyor.ChunkResult{Chunk: conveyor.Chunk{Id: 1}, Lines: 100},
			currentChunkCount: 1,
			ExpectedOutput:    "[  1/100]   1.00 % done. lines: 100\n",
		},
		{
			ChunkResult:       conveyor.ChunkResult{Chunk: conveyor.Chunk{Id: 10}, Lines: 100},
			currentChunkCount: 10,
			ExpectedOutput:    "[ 10/100]  10.00 % done. lines: 100\n",
		},
		{
			ChunkResult:       conveyor.ChunkResult{Chunk: conveyor.Chunk{Id: 50}, Lines: 100},
			currentChunkCount: 50,
			ExpectedOutput:    "[ 50/100]  50.00 % done. lines: 100\n",
		},
		{
			ChunkResult:       conveyor.ChunkResult{Chunk: conveyor.Chunk{Id: 99}, Lines: 100},
			currentChunkCount: 99,
			ExpectedOutput:    "[ 99/100]  99.00 % done. lines: 100\n",
		},
		{
			ChunkResult:       conveyor.ChunkResult{Chunk: conveyor.Chunk{Id: 100}, Lines: 100},
			currentChunkCount: 100,
			ExpectedOutput:    "[100/100] 100.00 % done. lines: 100\n",
		},
		{
			ChunkResult: conveyor.ChunkResult{
				Chunk: conveyor.Chunk{Id: 10},
				Lines: 100,
				Err:   errors.New("chunk error: test error"),
			},
			currentChunkCount:   10,
			ExpectedErrorOutput: "[ 10/100] chunk error: test error\n",
		},
	}

	for _, test := range tt {
		loggerOutput.Reset()
		errorLoggerOutput.Reset()

		conveyor.LogChunkResult(queue, test.ChunkResult, test.currentChunkCount)

		if test.ExpectedOutput != "" {
			loggerOut := loggerOutput.String()
			assertion.Equal(test.ExpectedOutput, loggerOut)
		}

		if test.ExpectedErrorOutput != "" {
			errLoggerOut := errorLoggerOutput.String()
			assertion.Equal(test.ExpectedErrorOutput, errLoggerOut)
		}

	}

}
