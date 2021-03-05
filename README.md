# Conveyor

[![Go Test](https://github.com/fgehrlicher/conveyor/actions/workflows/test.yml/badge.svg)](https://github.com/fgehrlicher/conveyor/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/fgehrlicher/conveyor/branch/main/graph/badge.svg?token=pC3OdgbO6V)](https://codecov.io/gh/fgehrlicher/conveyor)
[![Go Report Card](https://goreportcard.com/badge/github.com/fgehrlicher/conveyor)](https://goreportcard.com/report/github.com/fgehrlicher/conveyor)

conveyor is a lightweight multithreaded file processing library.

## ⚠️ WIP ⚠️ 



## Example Usage

```go
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
	w := conveyor.NewConcurrentWriter(resultFile, true)

	chunks, _ := conveyor.GetChunksFromFile("data.txt", 512, w)

	queue := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(Redact))
	
	result := queue.Work()

	log.Printf("processed %d lines", result.Lines)
}

func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := string(line)

	for _, word := range textToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}

```

