package iterator

type IReadIterator[T any] interface {
	HasNext() bool
	Next() (T, error)
}

type IWriteIterator[T any] interface {
	Write(v T) error
}
