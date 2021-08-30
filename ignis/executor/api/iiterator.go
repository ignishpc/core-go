package api

type IReadIterator interface {
	HasNext() bool
	Next() (interface{}, error)
}

type IWriteIterator interface {
	Write(v interface{}) error
}
