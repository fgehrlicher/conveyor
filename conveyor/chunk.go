package conveyor

import (
	"io"
	"os"
)

type Chunk struct {
	Id     int
	File   string
	Offset int64
	Size   int

	RealSize       int
	RealOffset     int64
	LinesProcessed int
	EOF            bool

	out io.Writer
}

type ChunkResult struct {
	Chunk Chunk
	Err   error
}

func (c ChunkResult) Ok() bool {
	return c.Err == nil
}

func GetChunks(filePath string, chunkSize int, out io.Writer) ([]Chunk, error) {
	info, err := os.Stat(filePath)
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
			File:       filePath,
			out:        out,
		})

		currentOffset += int64(chunkSize)
		currentChunk++
	}

	return chunks, err
}
