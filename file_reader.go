package conveyor

import (
	"io"
	"os"
)

// FileReader is a basic ChunkReader implementation for file resources.
type FileReader struct {
	FilePath string
}

// OpenHandle wraps os.Open.
func (f *FileReader) OpenHandle() (io.ReadSeekCloser, error) {
	return os.Open(f.FilePath)
}

// GetHandleID returns the file path which can be used as
// unique ID across multiple handles.
func (f *FileReader) GetHandleID() string {
	return f.FilePath
}
