package conveyor

import (
	"log"
	"os"
	"sync"
)

type Queue struct {
	workers       int
	chunkCount    int
	chunkSize     int64
	lineProcessor LineProcessor
	*QueueOpts

	tasks  chan Chunk
	result chan ChunkResult
}

type QueueOpts struct {
	ChunkResultLogger    ChunkResultLogger
	Logger               *log.Logger
	ErrLogger            *log.Logger
	OverflowScanBuffSize int
}

type QueueResult struct {
	Results      []ChunkResult
	Lines        int64
	FailedChunks int
}

func NewQueue(chunks []Chunk, workers int, lineProcessor LineProcessor, opts ...*QueueOpts) *Queue {
	tasks := make(chan Chunk, len(chunks))
	for _, chunk := range chunks {
		tasks <- chunk
	}
	close(tasks)

	var opt *QueueOpts
	if len(opts) > 0 && opts[0] != nil {
		opt = opts[0]
	} else {
		opt = &QueueOpts{}
	}

	if opt.ChunkResultLogger == nil {
		opt.ChunkResultLogger = DefaultChunkResultLogger
	}

	if opt.OverflowScanBuffSize == 0 {
		opt.OverflowScanBuffSize = DefaultOverflowScanSize
	}

	if opt.Logger == nil {
		opt.Logger = log.New(os.Stdout, "", log.LstdFlags)
	}

	if opt.ErrLogger == nil {
		opt.ErrLogger = log.New(os.Stderr, "", log.LstdFlags)
	}

	return &Queue{
		workers:       workers,
		tasks:         tasks,
		result:        make(chan ChunkResult, workers),
		chunkCount:    len(chunks),
		chunkSize:     int64(chunks[0].Size),
		lineProcessor: lineProcessor,
		QueueOpts:     opt,
	}
}

func (queue *Queue) Work() QueueResult {
	var (
		wg      sync.WaitGroup
		results = make([]ChunkResult, 0, queue.chunkCount)
	)

	wg.Add(queue.workers + queue.chunkCount)

	for i := 0; i < queue.workers; i++ {
		go NewWorker(
			queue.tasks,
			queue.result,
			queue.lineProcessor,
			queue.chunkSize,
			queue.OverflowScanBuffSize,
			&wg,
		).Work()
	}

	quit := make(chan int)
	go func() {
		currentChunkNumber := 0

		for {
			select {
			case result := <-queue.result:
				currentChunkNumber++

				queue.ChunkResultLogger(queue, result, currentChunkNumber)

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
		totalLines += int64(result.Lines)
		if !result.Ok() {
			failedChunks++
		}
	}

	return QueueResult{
		Results:      results,
		Lines:        totalLines,
		FailedChunks: failedChunks,
	}
}
