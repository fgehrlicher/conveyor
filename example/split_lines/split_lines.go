package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fgehrlicher/conveyor"
)

func main() {
	resultFile, err := os.Create("converted_data.txt")
	if err != nil {
		log.Fatal(err)
	}

	concurrentWriter := conveyor.NewConcurrentWriter(resultFile, true)

	chunks, err := conveyor.GetChunksFromFile("../../testdata/data.txt", 512, concurrentWriter)
	if err != nil {
		log.Fatal(err)
	}

	result := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(SplitLines)).Work()

	fmt.Printf(
		"processed %d lines.\n%d chunks failed.\n",
		result.Lines,
		result.FailedChunks,
	)
}

func SplitLines(line []byte, _ conveyor.LineMetadata) ([]byte, error) {
	return []byte(
		strings.ReplaceAll(
			strings.TrimSpace(string(line)),
			" ", "\n",
		),
	), nil
}
