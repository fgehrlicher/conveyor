package conveyor

// LineProcessor is the interface that wraps the Process method.
//
// Process get the line that needs to be processed and line metadata
// and returns the converted line or an error if the conversion failed.
// It is also valid to return an empty result if the line should be excluded from
// the output e.g when the file should not be mapped but processed in
// another way.
type LineProcessor interface {
	Process(line []byte, metadata LineMetadata) (out []byte, err error)
}

// LineMetadata is the metadata passed to LineProcessor.Process.
// Line is the line number relative to the chunk.
// Chunk is a pointer to the chunk which contains that line.
type LineMetadata struct {
	Line  int
	Chunk *Chunk
}

// The LineProcessorFunc type is an adapter to allow the use of
// ordinary functions as LineProcessor.
type LineProcessorFunc func([]byte, LineMetadata) ([]byte, error)

// Process calls the underlying LineProcessorFunc.
func (f LineProcessorFunc) Process(line []byte, metadata LineMetadata) (out []byte, err error) {
	return f(line, metadata)
}
