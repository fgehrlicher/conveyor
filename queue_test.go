package conveyor_test

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func TestQueue_Work(t *testing.T) {
	assertion := assert.New(t)
	buff := &bytes.Buffer{}

	concurrentWriter := conveyor.NewConcurrentWriter(buff, true)
	chunks, err := conveyor.GetChunks("testdata/data.txt", 512, concurrentWriter)
	assertion.NoError(err)
	assertion.Equal(14, len(chunks))


	result := conveyor.NewQueue(chunks, 4, conveyor.LineProcessorFunc(Redact)).Work()

	assertion.Empty(result.FailedChunks)
	assertion.Equal(int64(100), result.Lines)
	assertion.Equal(14, len(result.Results))

	actualFile, err := ioutil.ReadFile("testdata/converted_data.txt")
	assertion.NoError(err)

	assertion.Equal(actualFile, buff.Bytes())
}
