package base

import (
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/modules/impl"
)

func registerTypeAC[T1 any, T2 any]() {
	registerTypeA[T1]()
	registerTypeA[T2]()
}

type iTypeAC[T1 any, T2 comparable] struct {
	iTypeA[ipair.IPair[T1, T2]]
}

func (this *iTypeAC[T1, T2]) LoadType() {
	this.iTypeA.LoadType()
	registerTypeAC[T1, T2]()
}

func typeACError() error {
	return ierror.RaiseMsg("TypeAC functions only implement pair functions with comparable values.")
}

/*ICommImpl*/

/*IIOImpl*/

/*ISortImpl*/

func (this *iTypeAC[T1, T2]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	return impl.SortByKey[T2, T1](sortImpl, ascending)
}

func (this *iTypeAC[T1, T2]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return impl.SortByKeyWithPartitions[T2, T1](sortImpl, ascending, partitions)
}

/*IPipeImpl*/

func (this *iTypeAC[T1, T2]) Keys(pipeImpl *impl.IPipeImpl) error {
	return impl.Keys[T1, T2](pipeImpl)
}

func (this *iTypeAC[T1, T2]) Values(pipeImpl *impl.IPipeImpl) error {
	return impl.Values[T1, T2](pipeImpl)
}

/*IMathImpl*/

func (this *iTypeAC[T1, T2]) SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error) {
	return 0, typeACError()
}

func (this *iTypeAC[T1, T2]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	return typeACError()
}

func (this *iTypeAC[T1, T2]) CountByKey(mathImpl *impl.IMathImpl) error {
	return typeACError()
}

func (this *iTypeAC[T1, T2]) CountByValue(mathImpl *impl.IMathImpl) error {
	return typeACError()
}

/*IReduceImpl*/

func (this *iTypeAC[T1, T2]) GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return typeACError()
}

func (this *iTypeAC[T1, T2]) Join(reduceImpl *impl.IReduceImpl, other string, numPartitions int64) error {
	return typeACError()
}

func (this *iTypeAC[T1, T2]) Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return typeACError()
}

/*IRepartitionImpl*/
