package conveyor

import "strconv"

// The ChunkResultLogger type is the function that formats and logs
// the chunk result to Queue.Logger and QueueErrLogger.
type ChunkResultLogger func(queue *Queue, result ChunkResult, currentChunkNumber int)

// DefaultChunkResultLogger is the default logger used by Queue.
var DefaultChunkResultLogger = LogChunkResult

// LogChunkResult is the default ChunkResultLogger.
func LogChunkResult(queue *Queue, result ChunkResult, currentChunkNumber int) {
	percent := float32(currentChunkNumber) / float32(queue.chunkCount) * 100

	if result.Err == nil {
		percentPadding := ""
		if percent < 10 {
			percentPadding = "  "
		}
		if percent >= 10 && percent != 100 {
			percentPadding = " "
		}

		queue.Logger.Printf(
			"[%*d/%d] %s%.2f %% done. lines: %d\n",
			len(strconv.Itoa(queue.chunkCount)),
			result.Chunk.Id,
			queue.chunkCount,
			percentPadding,
			percent,
			result.Lines,
		)
	} else {
		queue.ErrLogger.Printf(
			"[%*d/%d] %s\n",
			len(strconv.Itoa(queue.chunkCount)),
			result.Chunk.Id,
			queue.chunkCount,
			result.Err,
		)
	}
}
