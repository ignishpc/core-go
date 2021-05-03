package api

type IReadIterator interface {
	Next() (interface{}, error)
}

type IWriteIterator interface {
	Write(v interface{}) error
}
