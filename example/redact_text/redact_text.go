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
	"ullamcorper",
	"Lorem",
}

func main() {
	resultFile, err := os.Create("../redacted_data.txt")
	checkError(err)

	concurrentWriter := conveyor.NewConcurrentWriter(resultFile, true)

	chunks, err := conveyor.GetChunks("../data.txt", 512, concurrentWriter)
	checkError(err)

	result := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(Redact)).Work()

	log.Printf("processed %d lines", result.Lines)
}

func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := string(line)

	for _, word := range textToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
