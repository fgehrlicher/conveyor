package conveyor_test

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/fgehrlicher/conveyor"
)

func TestLineProcessorFunc(t *testing.T) {
	testFunc := conveyor.LineProcessorFunc(func(bytes []byte, metadata conveyor.LineMetadata) ([]byte, error) {
		return bytes, nil
	})


	testData := []byte("123")
	out, _ := testFunc.Process(testData, conveyor.LineMetadata{})
	assert.Equal(t, testData, out)
}