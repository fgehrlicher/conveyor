package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/fgehrlicher/conveyor"
)

const (
	inputFilePath = "../../testdata/animals.csv"
	chunkSize = 512
	workerCount = 4

	header = "id,name,scientific_name"
	colorFieldIndex = 3
)

func main() {
	// Get Chunks from animals.csv
	chunks, err := conveyor.GetChunksFromFile(inputFilePath, chunkSize, nil)
	checkErr(err)

	// Run Queue
	result := conveyor.NewQueue(chunks, workerCount, NewAnimalSorter()).Work()

	// Print results
	fmt.Printf(
		"processed %d lines.\n%d chunks failed.\n",
		result.Lines,
		result.FailedChunks,
	)
}

type AnimalSorter struct {
	handles     map[string]io.Writer

	sync.Mutex
}

func NewAnimalSorter() *AnimalSorter {
	return &AnimalSorter{
		handles:     make(map[string]io.Writer),
	}
}

func (c *AnimalSorter) Process(line []byte, _ conveyor.LineMetadata) ([]byte, error) {
	c.Lock()
	defer c.Unlock()

	// get row slice
	row := strings.Split(strings.TrimSuffix(string(line), "\n"), ",")

	// get color
	color := row[colorFieldIndex]

	// Early return if row is header
	if color == "color_code" {
		return nil, nil
	}

	// create handle if color handle does not exist yet
	handle, ok := c.handles[color]
	if !ok {
		var err error
		handle, err = os.Create(fmt.Sprintf("out/%s-animals.csv", color))
		checkErr(err)

		_, err = handle.Write([]byte(header + "\n") )
		checkErr(err)

		c.handles[color] = handle
	}

	// write result to color handle
	resultRow := []byte(strings.Join(row[:colorFieldIndex], ",") + "\n")
	_, err := handle.Write(resultRow)
	checkErr(err)

	return nil, nil
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
