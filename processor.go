package conveyor

type LineProcessor interface {
	Process(line []byte, metadata LineMetadata) (out []byte, err error)
}

type LineMetadata struct {
	Line  int
	Chunk *Chunk
}

type LineProcessorFunc func([]byte, LineMetadata) ([]byte, error)

func (f LineProcessorFunc) Process(line []byte, metadata LineMetadata) (out []byte, err error) {
	return f(line, metadata)
}
