package conveyor

import (
	"fmt"
	"os"
	"strconv"
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

	out ChunkWriter
}

type ChunkWriter interface {
	WriteBuff(chunk *Chunk, buff []byte) error
}

type ChunkResult struct {
	Chunk Chunk
	Err   error
}

func (c *ChunkResult) Ok() bool {
	return c.Err == nil
}

func GetChunks(filePath string, chunkSize int, out ChunkWriter) ([]Chunk, error) {
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

func logChunkResult(queue *Queue, result ChunkResult, chunksProcessed int) {
	percent := float32(chunksProcessed) / float32(queue.chunkCount) * 100

	if result.Err == nil {
		percentPadding := ""
		if percent < 10 {
			percentPadding = "  "
		}
		if percent >= 10 && percent != 100 {
			percentPadding = " "
		}

		queue.Logger.Println(
			fmt.Sprintf(
				"[%*d/%d] %s%.2f %% done. lines: %d",
				len(strconv.Itoa(queue.chunkCount)),
				result.Chunk.Id,
				queue.chunkCount,
				percentPadding,
				percent,
				result.Chunk.LinesProcessed,
			),
		)
	} else {
		queue.ErrLogger.Println(
			fmt.Printf(
				"[%*d/%d] %s",
				len(strconv.Itoa(queue.chunkCount)),
				result.Chunk.Id,
				queue.chunkCount,
				result.Err,
			),
		)
	}
}
