package conveyor_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/fgehrlicher/conveyor"
)

var testChunks = map[int][]byte{
	1: []byte(
		`lyLztdptPSsSmyhFJNFQ
mXAgfwIVMqxUEwFhxpSw
PcnMvgAeDEbbVXMPXSOR
YzLmCAXVxPilRydPJkjb
eWOvSqMIlNvpubnVpzEX
EYUgrVMATCHczAdSwUIy
yCTJNSmdxGumROOiOBlT
IXUtvSpmbiEbvkQLdmVh
XCEHtaJZrJYjmWSyDUdl`),
	2: []byte(
		`CZuConYNNJqKGlzGkDvF
DukJstpKNfbaLaWrqPQl
qWdnTAWUXkbjasAXbnyt
RCyTjpjjcZgDlpXEVUQu
zytCncspCpvmcIuLWfqf
qaHKgJjmTVwKpCSmwfLa
iWIZemuuygQlvuADiOJd
XepQsbPdCiibyZQnaKXl
kZnHUbOfGgYHZecDXdrs
qpRuMEDqzWLjoFMkNjDh`),
	3: []byte(
		`hBLAinVBpobEFyZkTdWb
ljGRjCeCvqfuCGWHNdBY
yWaHsmIzTaxtQnFkrtSv
kqcfcfAKDmCYyeWwujfY
KGzuUUMFgQGdgwApUcCV
cifpEFCVBEuImbYYXNdB
YrPMaSzKSPoGZNCfJfMS
AbEdIWcXGklgUCvRLoui
yImoNFJYHmyRFoGLTYbM
bkFmBnUNzURsenzAJWAF
ZkoUdVOZsxoXublmWfnB
KkZlrxScUXfUirjMZuoG`),
}

func TestWriteChunksNoOrder(t *testing.T) {
	var buff bytes.Buffer
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&buff, false)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 3}, testChunks[3]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, testChunks[1]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 2}, testChunks[2]))

	output := string(buff.Bytes())
	for i := 1; i <= 3; i++ {
		chunk := string(testChunks[i])
		assertion.Containsf(output,chunk, "Buff doesnt contain chunk %d", i)
	}
}

func TestWriteChunksInOrder(t *testing.T) {
	var buff bytes.Buffer
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&buff, true)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 3}, testChunks[3]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 2}, testChunks[2]))
	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, testChunks[1]))

	output := string(buff.Bytes())

	var expectedOutput string
	for i := 1; i <= 3; i++ {
		expectedOutput += string(testChunks[i]) + "\n"
	}

	assertion.Equal(expectedOutput, output, "Chunks buff not in Order")
}

func TestDoesNotWriteEmptyCache(t *testing.T) {
	var buff bytes.Buffer
	assertion := assert.New(t)
	writer := conveyor.NewConcurrentWriter(&buff, false)

	assertion.NoError(writer.Write(&conveyor.Chunk{Id: 1}, nil))
	assertion.Empty(buff.Bytes())
}
