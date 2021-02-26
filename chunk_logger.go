package conveyor

import "strconv"

type ChunkResultLogger func(queue *Queue, result ChunkResult, currentChunkNumber int)

var DefaultChunkResultLogger = LogChunkResult

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
			result.Chunk.LinesProcessed,
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

