package conveyor_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"strings"

	"github.com/fgehrlicher/conveyor"
)

var textToRedact = []string{
	"testmail@test.com",
	"test@mail.de",
	"ullamcorper",
	"Lorem",
}

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

var ErrInvalidWrite = errors.New("invalid write")

func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := string(line)

	for _, word := range textToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}

type TestChunkWriter struct {
	Buff *bytes.Buffer
}

func (t *TestChunkWriter) Write(chunk *conveyor.Chunk, buff []byte) error {
	t.Buff.Write(buff)
	return nil
}

func generateTestChunks(count, size int, file string) []conveyor.Chunk {
	var (
		result []conveyor.Chunk
		offset = 0
	)

	for i := 0; i < count; i++ {
		result = append(result, conveyor.Chunk{
			Id:     i + 1,
			In:     &conveyor.FileReader{Filename: file},
			Offset: int64(offset),
			Size:   size,
		})

		offset += size
	}

	return result
}

type InvalidWriter struct {
	FailAt int

	write int
}

func (i *InvalidWriter) Write(p []byte) (int, error) {
	if i.FailAt == i.write {
		return 0, ErrInvalidWrite
	}

	i.write++
	return 0, nil
}

func NullLogger() *log.Logger {
	return log.New(ioutil.Discard, "", 0)
}
