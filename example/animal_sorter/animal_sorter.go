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

	header         = "id,name,scientific_name"
	sortFieldIndex = 3
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

	// get sortField
	sortField := row[sortFieldIndex]

	// Early return if row is header
	if sortField == "code" {
		return nil, nil
	}

	// create handle if sortField handle does not exist yet
	handle, ok := c.handles[sortField]
	if !ok {
		var err error
		handle, err = os.Create(fmt.Sprintf("out/%s-animals.csv", sortField))
		checkErr(err)

		_, err = handle.Write([]byte(header + "\n") )
		checkErr(err)

		c.handles[sortField] = handle
	}

	// write result to sortField handle
	resultRow := []byte(strings.Join(row[:sortFieldIndex], ",") + "\n")
	_, err := handle.Write(resultRow)
	checkErr(err)

	return nil, nil
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
