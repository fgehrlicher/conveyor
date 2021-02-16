package conveyor

import (
	"fmt"
	"strconv"
	"sync"
)

type Queue struct {
	workers       int
	chunkCount    int
	chunkSize     int64
	lineProcessor LineProcessor

	tasks  chan Chunk
	result chan ChunkResult
}

type QueueResult struct {
	Results      []ChunkResult
	Lines        int64
	FailedChunks int
}

func NewQueue(chunks []Chunk, workers int, lineProcessor LineProcessor) *Queue {
	tasks := make(chan Chunk, len(chunks))
	for _, chunk := range chunks {
		tasks <- chunk
	}
	close(tasks)

	return &Queue{
		workers:       workers,
		tasks:         tasks,
		result:        make(chan ChunkResult, workers),
		chunkCount:    len(chunks),
		chunkSize:     int64(chunks[0].Size),
		lineProcessor: lineProcessor,
	}
}

func (queue *Queue) Work() QueueResult {
	var (
		wg      sync.WaitGroup
		results = make([]ChunkResult, 0, queue.chunkCount)
	)

	wg.Add(queue.workers + queue.chunkCount)

	for i := 0; i < queue.workers; i++ {
		go NewWorker(queue.tasks, queue.result, queue.lineProcessor, queue.chunkSize, 1024, &wg).Work()
	}

	quit := make(chan int)
	go func() {
		chunksProcessed := 0
		var percent float32

		for {
			select {
			case result := <-queue.result:
				chunksProcessed++
				percent = float32(chunksProcessed) / float32(queue.chunkCount) * 100

				if result.Err == nil {
					percentPadding := ""
					if percent < 10 {
						percentPadding = "  "
					}
					if percent >= 10 && percent != 100 {
						percentPadding = " "
					}

					fmt.Printf(
						"[%*d/%d] %s%.2f %% done. lines in chunk: %d \n",
						len(strconv.Itoa(queue.chunkCount)),
						result.Chunk.Id,
						queue.chunkCount,
						percentPadding,
						percent,
						result.Chunk.LinesProcessed,
					)
				} else {
					fmt.Printf(
						"[%*d/%d] error in chunk :%s\n",
						len(strconv.Itoa(queue.chunkCount)),
						result.Chunk.Id,
						queue.chunkCount,
						result.Err,
					)
				}
				results = append(results, result)

				wg.Done()

			case <-quit:
				return
			}
		}
	}()

	wg.Wait()
	quit <- 0

	var (
		totalLines   int64
		failedChunks int
	)

	for _, result := range results {
		totalLines += int64(result.Chunk.LinesProcessed)
		if result.Err != nil {
			failedChunks++
		}
	}

	return QueueResult{
		Results:      results,
		Lines:        totalLines,
		FailedChunks: failedChunks,
	}
}
