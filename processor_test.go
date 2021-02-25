package conveyor_test

import (
	"testing"

	"github.com/fgehrlicher/conveyor"
	"github.com/stretchr/testify/assert"
)

func TestLineProcessorFunc(t *testing.T) {
	testFunc := conveyor.LineProcessorFunc(func(bytes []byte, metadata conveyor.LineMetadata) ([]byte, error) {
		return bytes, nil
	})

	testData := []byte("123")
	out, _ := testFunc.Process(testData, conveyor.LineMetadata{})
	assert.Equal(t, testData, out)
}
