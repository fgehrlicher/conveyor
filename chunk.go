package convert

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

func SplitFileInChunks(chunkSize int64, fileIn string, fileOut io.Writer) ([]Chunk, error) {
	var (
		currentOffset int64 = 0
		currentChunk        = 1
		chunks        []Chunk
	)

	info, err := os.Stat(fileIn)
	if err != nil {
		return nil, err
	}

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
