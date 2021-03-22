# Conveyor

[![Go Test](https://github.com/fgehrlicher/conveyor/actions/workflows/test.yml/badge.svg)](https://github.com/fgehrlicher/conveyor/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/fgehrlicher/conveyor/branch/main/graph/badge.svg?token=pC3OdgbO6V)](https://codecov.io/gh/fgehrlicher/conveyor)
[![Go Report Card](https://goreportcard.com/badge/github.com/fgehrlicher/conveyor)](https://goreportcard.com/report/github.com/fgehrlicher/conveyor)

conveyor is a lightweight multithreaded file processing library.

## ⚠️ WIP ⚠️

## Example Usage

```go
func main() {
	// Create the output file
	resultFile, _ := os.Create("redacted_data.txt")

	// Instantiate a new ConcurrentWriter which wraps the resultFile handle.
	// The ConcurrentWriter type is just a small thread-safe wrapper for 
	// io.Writer which is able to keep the order of lines across all chunks.
	w := conveyor.NewConcurrentWriter(resultFile, true)

	// Split the input file into chunks of 512 bytes with 
	// the concurrent writer as output ChunkWriter.
	chunks, _ := conveyor.GetChunksFromFile("data.txt", 512, w)

	// Creates and executes a Queue with 4 workers and the Redact function as LineProcessor.
	result := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(Redact)).Work()

	// Prints the number of lines processed.
	log.Printf("processed %d lines", result.Lines)
}

// Email that should be redacted
var emailToRedact = "testmail@test.com"

// Redact replaces all occurrences of "testmail@test.com" with x
func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := strings.ReplaceAll(
		string(line),
		emailToRedact,
		strings.Repeat("x", len(emailToRedact)),
	)

	return []byte(result), nil
}
```

See [examples](https://github.com/fgehrlicher/conveyor/tree/main/example) for detailed information on usage.



