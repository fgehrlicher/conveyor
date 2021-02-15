package convert

type LineProcessor interface  {
	Process(line []byte) (out []byte, err error)
}