package conveyor

import (
	"io"
	"os"
)

// ChunkWriter is the interface that wraps the basic Write method.
// Write writes len(buff) bytes from buff to the underlying data stream.
type ChunkWriter interface {
	Write(chunk *Chunk, buff []byte) error
}

// ChunkReader is the interface that wraps OpenHandle and GetHandleID.
// OpenHandle opens a resource and returns a io.ReadSeekCloser
// GetHandleID returns the name / id of the underlying resource. That strings is used for
// for caching purposes inside Worker.
type ChunkReader interface {
	OpenHandle() (io.ReadSeekCloser, error)
	GetHandleID() string
}

type Chunk struct {
	Id     int
	Offset int64
	Size   int

	In  ChunkReader
	Out ChunkWriter
}

type ChunkResult struct {
	Chunk *Chunk

	Err        error
	RealSize   int
	RealOffset int64
	Lines      int
	EOF        bool
}

func (c *ChunkResult) Ok() bool {
	return c.Err == nil
}

// GetChunksFromFile generates a slice of Chunk for a given file path and ChunkWriter
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
