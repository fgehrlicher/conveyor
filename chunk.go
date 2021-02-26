package conveyor

import (
	"io"
	"os"
)

type Chunk struct {
	Id     int
	Offset int64
	Size   int

	RealSize       int
	RealOffset     int64
	LinesProcessed int
	EOF            bool

	In  ChunkReader
	Out ChunkWriter
}

type ChunkWriter interface {
	Write(chunk *Chunk, buff []byte) error
}

type ChunkReader interface {
	OpenHandle() (io.ReadSeekCloser, error)
	GetName() string
}

type ChunkResult struct {
	Chunk Chunk
	Err   error
}

func (c *ChunkResult) Ok() bool {
	return c.Err == nil
}

func GetChunksFromFile(filePath string, chunkSize int, out ChunkWriter) ([]Chunk, error) {
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
			Id:     currentChunk,
			Offset: currentOffset,
			Size:   chunkSize,
			Out:    out,
			In:     &FileReader{Filename: filePath},
		})

		currentOffset += int64(chunkSize)
		currentChunk++
	}

	return chunks, err
}
