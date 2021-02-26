package conveyor_test

import (
	"bytes"
	"io/ioutil"
	"log"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func TestQueue_Work(t *testing.T) {
	var (
		assertion      = assert.New(t)
		testFile       = "testdata/data.txt"
		testResultFile = "testdata/converted_data.txt"
		linesInFile    = 100
	)

	tt := []struct {
		ChunkSize      int
		Workers        int
		ExpectedChunks int
	}{
		{
			ChunkSize:      200,
			Workers:        1,
			ExpectedChunks: 35,
		},
		{
			ChunkSize:      200,
			Workers:        10,
			ExpectedChunks: 35,
		},
		{
			ChunkSize:      512,
			Workers:        1,
			ExpectedChunks: 14,
		},
		{
			ChunkSize:      512,
			Workers:        10,
			ExpectedChunks: 14,
		},
		{
			ChunkSize:      16384,
			Workers:        1,
			ExpectedChunks: 1,
		},
		{
			ChunkSize:      16384,
			Workers:        10,
			ExpectedChunks: 1,
		},
	}

	for _, test := range tt {
		buff := &bytes.Buffer{}
		concurrentWriter := conveyor.NewConcurrentWriter(buff, true)
		chunks, err := conveyor.GetChunksFromFile(testFile, test.ChunkSize, concurrentWriter)
		assertion.NoError(err)
		assertion.Equal(test.ExpectedChunks, len(chunks))

		result := conveyor.NewQueue(
			chunks,
			test.Workers,
			conveyor.LineProcessorFunc(Redact),
			&conveyor.QueueOpts{
				Logger:    NullLogger(),
				ErrLogger: NullLogger(),
			},
		).Work()

		assertion.Empty(result.FailedChunks)
		assertion.Equal(int64(linesInFile), result.Lines)
		assertion.Equal(test.ExpectedChunks, len(result.Results))

		expectedFile, err := ioutil.ReadFile(testResultFile)
		assertion.NoError(err)

		actualFile := buff.Bytes()
		assertion.Equal(expectedFile, actualFile)
	}

}

func TestQueueDefaultOptions(t *testing.T) {
	assertion := assert.New(t)

	queue := conveyor.NewQueue(
		generateTestChunks(1, 100, "test"),
		1,
		nil,
	)

	assertion.IsType(&log.Logger{}, queue.Logger)
	assertion.IsType(&log.Logger{}, queue.ErrLogger)
	assertion.Equal(conveyor.DefaultOverflowScanSize, queue.OverflowScanBuffSize)
}

func TestQueueFailsForInvalidChunks(t *testing.T) {
	assertion := assert.New(t)
	chunks := 10

	result := conveyor.NewQueue(
		generateTestChunks(chunks, 100, "non_existing_file"),
		5,
		nil,
		&conveyor.QueueOpts{
			ErrLogger: NullLogger(),
		},
	).Work()

	assertion.Equal(chunks, result.FailedChunks)

	for _, chunk := range result.Results {
		assertion.False(chunk.Ok())
	}
}

func TestQueueErrorsForTooSmallChunks(t *testing.T) {
	var (
		assertion = assert.New(t)
		testFile  = "testdata/data.txt"
	)

	chunks, err := conveyor.GetChunksFromFile(testFile, 10, nil)
	assertion.NoError(err)

	result := conveyor.NewQueue(
		chunks,
		4,
		conveyor.LineProcessorFunc(Redact),
		&conveyor.QueueOpts{
			Logger:    NullLogger(),
			ErrLogger: NullLogger(),
		},
	).Work()

	assertion.Equal(694, len(result.Results))
	assertion.Equal(600, result.FailedChunks)
}
