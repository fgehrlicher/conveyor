package conveyor

import (
	"io"
	"os"
)

type Chunk struct {
	Id             int
	File           string
	Offset         int64
	Size           int64
	RealOffset     int64
	RealSize       int
	LinesProcessed int

	out              io.Writer
	partialFirstLine bool
	partialLastLine  bool
}

type ChunkResult struct {
	Chunk Chunk
	Err   error
}

func (c ChunkResult) Ok() bool {
	return c.Err == nil
}

func FileInChunks(fileIn string, chunkSize int64, fileOut io.Writer) ([]Chunk, error) {
	info, err := os.Stat(fileIn)
	if err != nil {
		return nil, err
	}

	var (
		currentOffset int64 = 0
		currentChunk        = 1
		chunks        []Chunk
	)

	for currentOffset <= info.Size() {
		chunks = append(chunks, Chunk{
			Id:         currentChunk,
			Offset:     currentOffset,
			RealOffset: currentOffset,
			Size:       chunkSize,
			File:       fileIn,
			out:        fileOut,
		})

		currentOffset += chunkSize
		currentChunk++
	}

	return chunks, err
}
