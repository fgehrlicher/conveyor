package convert

import (
	"fmt"
	"strconv"
	"sync"
)

type Queue struct {
	workers    int
	chunkCount int
	chunkSize  int64

	tasks  chan Chunk
	result chan ChunkResult
}

func NewQueue(chunks []Chunk, workers int, chunkSize int64) *Queue {
	tasks := make(chan Chunk, len(chunks))
	for _, chunk := range chunks {
		tasks <- chunk
	}
	close(tasks)

	return &Queue{
		workers:    workers,
		tasks:      tasks,
		result:     make(chan ChunkResult, workers),
		chunkCount: len(chunks),
		chunkSize:  chunkSize,
	}
}

func (queue *Queue) Work() []ChunkResult {
	var (
		waitGroup sync.WaitGroup
		results   = make([]ChunkResult, 0, queue.chunkCount)
	)

	waitGroup.Add(queue.workers)
	for i := 0; i < queue.workers; i++ {
		go NewWorker(queue.tasks, queue.result, queue.chunkSize, &waitGroup).Work()
	}

	quit := make(chan int)
	go func() {
		chunksProcessed := 0

		for {
			select {
			case result := <-queue.result:
				chunksProcessed++

				if result.Err == nil {
					percent := float32(chunksProcessed) / float32(queue.chunkCount) * 100
					percentPadding := ""
					if percent < 10.0 {
						percentPadding = "  "
					}
					if percent > 10 && percent != 100 {
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

			case <-quit:
				return
			}
		}
	}()

	waitGroup.Wait()
	quit <- 0

	return results
}
