package api

type Optional[T any] interface {
	Get() T
	GetOr(def T) T
	Present() bool
}

func Of[T any](value T) Optional[T] {
	return &optionalImpl[T]{&value}
}

func Empty[T any]() Optional[T] {
	return &optionalImpl[T]{nil}
}

type optionalImpl[T any] struct {
	value *T
}

func (this *optionalImpl[T]) Get() T {
	return *this.value
}

func (this *optionalImpl[T]) GetOr(def T) T {
	if this.value == nil {
		return def
	}
	return *this.value
}

func (this *optionalImpl[T]) Present() bool {
	return this.value != nil
}
