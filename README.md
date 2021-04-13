# Conveyor

[![Go Reference](https://pkg.go.dev/badge/github.com/fgehrlicher/conveyor.svg)](https://pkg.go.dev/github.com/fgehrlicher/conveyor)
[![Go Test](https://github.com/fgehrlicher/conveyor/actions/workflows/test.yml/badge.svg)](https://github.com/fgehrlicher/conveyor/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/fgehrlicher/conveyor/branch/main/graph/badge.svg?token=pC3OdgbO6V)](https://codecov.io/gh/fgehrlicher/conveyor)
[![Go Report Card](https://goreportcard.com/badge/github.com/fgehrlicher/conveyor)](https://goreportcard.com/report/github.com/fgehrlicher/conveyor)

**Conveyor** is a lightweight multithreaded file processing library.  
Think of it as a simple way to apply a function/method to every line in 1 - n file(s).

This library can be used:

* As a file-wide [Map](https://en.wikipedia.org/wiki/Map_(higher-order_function)) function (see [Example Usage](#example-usage)).

  


## ⚠️ WIP ⚠️

## Installation
```
go get github.com/fgehrlicher/conveyor
```

<a id="example-usage"></a>
## Example Usage
Redact all occurrences of a given email:

```go
func main() {
	// Creat the output file
	resultFile, _ := os.Create("redacted_data.txt")

	// Instantiate a new ConcurrentWriter which wraps the resultFile handle.
	// The ConcurrentWriter type is just a small thread-safe wrapper for 
	// io.Writer which is able to keep the chunk output in order.
	w := conveyor.NewConcurrentWriter(resultFile, true)

	// Split the input file into chunks of 512 bytes with 
	// the concurrent writer as output ChunkWriter.
	chunks, _ := conveyor.GetChunksFromFile("data.txt", 512, w)

	// Creat and execute a Queue with 4 workers and the Redact function as LineProcessor.
	result := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(Redact)).Work()

	// Print the number of processed lines.
	log.Printf("processed %d lines", result.Lines)
}

// Text that should be redacted
const emailToRedact = "testmail@test.com"

// Redact replaces all occurrences of "testmail@test.com" with "x"
func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := strings.ReplaceAll(
		string(line),
		emailToRedact,
		strings.Repeat("x", len(emailToRedact)),
	)

	return []byte(result), nil
}
```

Additional Examples:  
* [Rune Counter](https://github.com/fgehrlicher/conveyor/tree/main/example/rune_counter)
  counts and prints the number of occurrences of certain runes. 
* [Animal Sorter](https://github.com/fgehrlicher/conveyor/tree/main/example/animal_sorter)
  sorts csv entries by field and puts them into separate files.
* [Split Lines](https://github.com/fgehrlicher/conveyor/tree/main/example/split_lines)
  replaces all occurrences of spaces with line breaks.

## Limitations 

## Logging

## Performance


