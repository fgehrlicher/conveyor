package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fgehrlicher/conveyor"
)

func main() {
	resultFile, err := os.Create("redacted_data.txt")
	if err != nil {
		log.Fatal(err)
	}

	concurrentWriter := conveyor.NewConcurrentWriter(resultFile, true)

	chunks, err := conveyor.GetChunksFromFile("../../testdata/data.txt", 512, concurrentWriter)
	if err != nil {
		log.Fatal(err)
	}

	result := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(Redact)).Work()

	fmt.Printf(
		"processed %d lines.\n%d chunks failed.\n",
		result.Lines,
		result.FailedChunks,
	)
}

var textToRedact = []string{
	"testmail@test.com",
	"test@mail.de",
	"ullamcorper",
	"Lorem",
}

func Redact(line []byte, _ conveyor.LineMetadata) ([]byte, error) {
	result := string(line)

	for _, word := range textToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}
