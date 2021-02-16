package conveyor

type LineProcessor interface  {
	Process(line []byte) (out []byte, err error)
}

type LineProcessorFunc func([]byte) ([]byte, error)

func (f LineProcessorFunc) Process(line []byte) (out []byte, err error) {
	return f(line)
}
