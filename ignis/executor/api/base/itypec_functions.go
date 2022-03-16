package base

import (
	"ignis/executor/core/ierror"
	"ignis/executor/core/modules/impl"
)

type iTypeC[T comparable] struct {
	iTypeA[T]
}

func typeCError() error {
	return ierror.RaiseMsg("TypeC functions does not implement pair functions.")
}

/*ICommImpl*/

/*IIOImpl*/

/*ISortImpl*/

func (this *iTypeC[T]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	return typeCError()
}

func (this *iTypeC[T]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return typeCError()
}

/*IPipeImpl*/

func (this *iTypeC[T]) Keys(pipeImpl *impl.IPipeImpl) error {
	return typeCError()
}

func (this *iTypeC[T]) Values(pipeImpl *impl.IPipeImpl) error {
	return typeCError()
}

/*IMathImpl*/

func (this *iTypeC[T]) SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error) {
	return 0, typeCError()
}

func (this *iTypeC[T]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	return typeCError()
}

func (this *iTypeC[T]) CountByKey(mathImpl *impl.IMathImpl) error {
	return typeCError()
}

func (this *iTypeC[T]) CountByValue(mathImpl *impl.IMathImpl) error {
	return typeCError()
}

/*IReduceImpl*/

func (this *iTypeC[T]) GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return typeCError()
}

func (this *iTypeC[T]) Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return impl.Distinct[T](reduceImpl, numPartitions)
}

/*IRepartitionImpl*/
