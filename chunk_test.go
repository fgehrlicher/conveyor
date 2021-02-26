package conveyor_test

import (
	"errors"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

const chunkTestFile = "testdata/5_lines.txt"

func TestGetChunksReturnsCorrectChunks(t *testing.T) {
	assertion := assert.New(t)
	chunks, err := conveyor.GetChunksFromFile(chunkTestFile, 100, nil)

	assertion.NoError(err)
	assertion.Equal(generateTestChunks(4, 100, chunkTestFile), chunks)
}

func TestGetChunksFailsForInvalidFile(t *testing.T) {
	assertion := assert.New(t)
	_, err := conveyor.GetChunksFromFile("Invalid File", 100, nil)

	assertion.Error(err)
}

func TestChunkResultIsNotOkForErr(t *testing.T) {
	assertion := assert.New(t)
	chunkResult := conveyor.ChunkResult{
		Chunk: conveyor.Chunk{},
		Err:   errors.New(""),
	}

	assertion.False(chunkResult.Ok())
}
