package main

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/fgehrlicher/conveyor"
)

func main() {
	chunks, err := conveyor.GetChunks("../../testdata/data.txt", 512, nil)
	if err != nil {
		log.Fatal(err)
	}

	rc := NewRuneCounter([]rune{'a', 'b', 'c', ' ', '.'})

	result := conveyor.NewQueue(chunks, 4, rc).Work()

	fmt.Printf(
		"processed %d lines.\n%d chunks failed.\n%s",
		result.Lines,
		result.FailedChunks,
		rc.Result(),
	)
}

type RuneCounter struct {
	runes  []rune
	result map[rune]int

	sync.Mutex
}

func NewRuneCounter(runes []rune) *RuneCounter {
	result := make(map[rune]int)
	for _, s := range runes {
		result[s] = 0
	}

	return &RuneCounter{
		runes:  runes,
		result: result,
	}
}

func (c *RuneCounter) Process(line []byte, metadata conveyor.LineMetadata) (out []byte, err error) {
	for _, r := range c.runes {
		if count := strings.Count(string(line), string(r)); count > 0 {
			c.Lock()
			c.result[r] += count
			c.Unlock()
		}
	}

	return nil, err
}

func (c *RuneCounter) Result() string {
	result := fmt.Sprint("Found occurrences of runes: \n")

	for r, count := range c.result {
		result += fmt.Sprintf("%q: %d \n", r, count)
	}

	return result
}
