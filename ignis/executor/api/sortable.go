package api

type Sortable[T any] interface {
	Less(T, T) bool
}
