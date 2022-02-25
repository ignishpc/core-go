package base

import (
	"ignis/executor/api/function"
	"ignis/executor/core/modules/impl"
)

//map
type IMapAbs interface {
	RunMap(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMap[T any, R any] struct {
}

func (this *IMap[T, R]) map_(i *impl.IPipeImpl, f function.IFunction[T, R]) error {
	return impl.Map(i, f)
}

func (this *IMap[T, R]) RunMap(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.Map(i, f.(function.IFunction[T, R]))
}

//filter
type IFilterAbs interface {
	RunFilter(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IFilter[T any] struct {
}

func (this *IFilter[T]) filter(i *impl.IPipeImpl, f function.IFunction[T, bool]) error {
	return impl.Filter(i, f)
}

func (this *IFilter[T]) RunFilter(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.Filter(i, f.(function.IFunction[T, bool]))
}
