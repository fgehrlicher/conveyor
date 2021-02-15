package conveyor

type LineProcessor interface  {
	Process(line []byte) (out []byte, err error)
}
