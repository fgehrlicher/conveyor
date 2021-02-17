package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fgehrlicher/conveyor/conveyor"
)

func main() {
	resultFile, err := os.Create("../redacted_data.txt")
	if err != nil {
		log.Fatal(err)
	}

	chunks, err := conveyor.GetChunks(
		"../data.txt",
		512,
		conveyor.NewConcurrentWriter(
			resultFile,
			true,
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	result := conveyor.NewQueue(
		chunks,
		4,
		conveyor.LineProcessorFunc(Redact),
	).Work()

	log.Println(fmt.Sprintf("processed %d lines", result.Lines),
	)
}

var wordsToRedact = []string{
	"testmail@test.com",
	"test@mail.de",
	"ullamcorper",
	"Lorem",
}

func Redact(line []byte) ([]byte, error) {
	result := string(line)

	for _, word := range wordsToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}
