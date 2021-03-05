package main

import (
	"log"
	"os"
	"strings"

	"github.com/fgehrlicher/conveyor"
)

var textToRedact = []string{
	"testmail@test.com",
	"test@mail.de",
}

func main() {
	resultFile, _ := os.Create("redacted_data.txt")
	concurrentWriter := conveyor.NewConcurrentWriter(resultFile, true)

	chunks, _ := conveyor.GetChunksFromFile("data.txt", 512, concurrentWriter)

	result := conveyor.NewQueue(
		chunks,
		4,
		conveyor.LineProcessorFunc(func(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
			result := string(line)

			for _, word := range textToRedact {
				result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
			}

			return []byte(result), nil
		}),
	).Work()

	log.Printf("processed %d lines", result.Lines)
}
