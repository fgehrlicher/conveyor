package conveyor

import (
	"io"
	"os"
)

type FileReader struct {
	Filename string
}

func (f *FileReader) OpenHandle() (io.ReadSeekCloser, error) {
	return os.Open(f.Filename)
}

func (f *FileReader) GetHandleID() string {
	return f.Filename
}
