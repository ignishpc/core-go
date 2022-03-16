package base

import (
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
	"ignis/executor/core/modules/impl"
)

func registerTypeCA[T1 comparable, T2 any]() {
	registerTypeA[T1]()
	registerTypeA[T2]()
	iio.AddKeyType[T1, T2]()
}

type iTypeCA[T1 comparable, T2 any] struct {
	iTypeA[ipair.IPair[T1, T2]]
}

func typeCAError() error {
	return ierror.RaiseMsg("TypeAC functions only implement pair functions with comparable keys.")
}

/*ICommImpl*/

/*IIOImpl*/

/*ISortImpl*/

func (this *iTypeCA[T1, T2]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	return impl.SortByKey[T2, T1](sortImpl, ascending)
}

func (this *iTypeCA[T1, T2]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return impl.SortByKeyWithPartitions[T2, T1](sortImpl, ascending, partitions)
}

/*IPipeImpl*/

func (this *iTypeCA[T1, T2]) Keys(pipeImpl *impl.IPipeImpl) error {
	return impl.Keys[T1, T2](pipeImpl)
}

func (this *iTypeCA[T1, T2]) Values(pipeImpl *impl.IPipeImpl) error {
	return impl.Values[T1, T2](pipeImpl)
}

/*IMathImpl*/

func (this *iTypeCA[T1, T2]) SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error) {
	return impl.SampleByKeyFilter[T2, T1](mathImpl)
}

func (this *iTypeCA[T1, T2]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	return impl.SampleByKey[T2, T1](mathImpl, withReplacement, seed)
}

func (this *iTypeCA[T1, T2]) CountByKey(mathImpl *impl.IMathImpl) error {
	return impl.CountByKey[T2, T1](mathImpl)
}

func (this *iTypeCA[T1, T2]) CountByValue(mathImpl *impl.IMathImpl) error {
	return typeCAError()
}

/*IReduceImpl*/

func (this *iTypeCA[T1, T2]) GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return impl.GroupByKey[T2, T1](reduceImpl, numPartitions)
}

func (this *iTypeCA[T1, T2]) Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return typeCAError()
}

/*IRepartitionImpl*/
