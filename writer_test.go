package conveyor_test

import (
	"bytes"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func TestWriteChunksNoOrder(t *testing.T) {
	var buff bytes.Buffer
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&buff, false)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 3}, testChunks[3]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, testChunks[1]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 2}, testChunks[2]))

	output := buff.String()
	for i := 1; i <= 3; i++ {
		chunk := string(testChunks[i])
		assertion.Containsf(output, chunk, "Buff doesnt contain chunk %d", i)
	}
}

func TestWriteChunksInOrder(t *testing.T) {
	var buff bytes.Buffer
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&buff, true)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 3}, testChunks[3]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 2}, testChunks[2]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, testChunks[1]))

	output := buff.String()

	var expectedOutput string
	for i := 1; i <= 3; i++ {
		expectedOutput += string(testChunks[i])
		if i != 3 {
			expectedOutput += "\n"
		}
	}

	assertion.Equal(expectedOutput, output, "Chunks buff not in Order")
}

func TestDoesNotWriteEmptyCache(t *testing.T) {
	var buff bytes.Buffer
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&buff, false)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, nil))
	assertion.Empty(buff.Bytes())
}

func TestFailingWriterPassesError(t *testing.T) {
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&InvalidWriter{FailAt: 0}, true)

	assertion.ErrorIs(
		writer.Write(&conveyor.Chunk{Id: 1}, testChunks[1]),
		ErrInvalidWrite,
	)

	writer = conveyor.NewConcurrentWriter(&InvalidWriter{FailAt: 1}, true)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, testChunks[1]))

	assertion.ErrorIs(
		writer.Write(&conveyor.Chunk{Id: 2}, testChunks[1]),
		ErrInvalidWrite,
	)
}
