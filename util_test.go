package conveyor_test

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
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

var (
	ErrInvalidWrite = errors.New("invalid write")
	ErrInvalidRead  = errors.New("invalid read")
)

func Redact(line []byte, metadata conveyor.LineMetadata) ([]byte, error) {
	result := string(line)

	for _, word := range textToRedact {
		result = strings.ReplaceAll(result, word, strings.Repeat("x", len(word)))
	}

	return []byte(result), nil
}

func generateTestChunks(count, size int, file string) []conveyor.Chunk {
	var (
		result []conveyor.Chunk
		offset = 0
	)

	for i := 0; i < count; i++ {
		result = append(result, conveyor.Chunk{
			Id:     i + 1,
			In:     &conveyor.FileReader{FilePath: file},
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

var NullLineProcessor = conveyor.LineProcessorFunc(
	func(bytes []byte, metadata conveyor.LineMetadata) ([]byte, error) {
		return bytes, nil
	},
)

type TestWriter struct {
	FailAt int

	write int
}

func (t *TestWriter) Write(chunk *conveyor.Chunk, buff []byte) error {
	if t.FailAt == t.write {
		return ErrInvalidWrite
	}

	t.write++
	return nil
}

type FailureAtFile struct {
	Filename string
	FailAt   int
	read     int

	Handle *os.File
}

func (f *FailureAtFile) Read(p []byte) (n int, err error) {
	if f.FailAt == f.read {
		return 0, ErrInvalidRead
	}

	f.read++
	return f.Handle.Read(p)
}

func (f *FailureAtFile) Seek(offset int64, whence int) (int64, error) {
	return f.Handle.Seek(offset, whence)
}

func (f *FailureAtFile) Close() error {
	return f.Handle.Close()
}

type FailureAtReader struct {
	Filename string
	FailAt   int
}

func (f *FailureAtReader) OpenHandle() (io.ReadSeekCloser, error) {
	file, _ := os.Open(f.Filename)
	return &FailureAtFile{
		FailAt: f.FailAt,
		Handle: file,
	}, nil
}

func (f *FailureAtReader) GetHandleID() string {
	return f.Filename
}

type FailureReadSeekCloser struct {
}

func (f FailureReadSeekCloser) Read(p []byte) (n int, err error) {
	return 0, ErrInvalidRead
}

func (f FailureReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	return offset, nil
}

func (f FailureReadSeekCloser) Close() error {
	return nil
}

type FailureFileReader struct {
	Filename string
}

func (f *FailureFileReader) OpenHandle() (io.ReadSeekCloser, error) {
	return FailureReadSeekCloser{}, nil
}

func (f *FailureFileReader) GetHandleID() string {
	return f.Filename
}
