package base

import (
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/modules/impl"
)

func registerTypeAA[T1 any, T2 any]() {
	registerTypeA[T1]()
	registerTypeA[T2]()
}

type iTypeAA[T1 any, T2 any] struct {
	iTypeA[ipair.IPair[T1, T2]]
}

func (this *iTypeAA[T1, T2]) LoadType() {
	registerTypeAA[T1, T2]()
	this.iTypeA.LoadType()
}

func typeAAError() error {
	return ierror.RaiseMsg("TypeAA functions only implement non-comparable pair functions.")
}

/*ICommImpl*/

/*IIOImpl*/

/*ISortImpl*/

func (this *iTypeAA[T1, T2]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	return impl.SortByKey[T2, T1](sortImpl, ascending)
}

func (this *iTypeAA[T1, T2]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return impl.SortByKeyWithPartitions[T2, T1](sortImpl, ascending, partitions)
}

/*IPipeImpl*/

func (this *iTypeAA[T1, T2]) Keys(pipeImpl *impl.IPipeImpl) error {
	return impl.Keys[T1, T2](pipeImpl)
}

func (this *iTypeAA[T1, T2]) Values(pipeImpl *impl.IPipeImpl) error {
	return impl.Values[T1, T2](pipeImpl)
}

/*IMathImpl*/

func (this *iTypeAA[T1, T2]) SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error) {
	if this.next != nil {
		return this.next.SampleByKeyFilter(mathImpl)
	}
	return 0, typeAAError()
}

func (this *iTypeAA[T1, T2]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	if this.next != nil {
		return this.next.SampleByKey(mathImpl, withReplacement, seed)
	}
	return typeAAError()
}

func (this *iTypeAA[T1, T2]) CountByKey(mathImpl *impl.IMathImpl) error {
	if this.next != nil {
		return this.next.CountByKey(mathImpl)
	}
	return typeAAError()
}

func (this *iTypeAA[T1, T2]) CountByValue(mathImpl *impl.IMathImpl) error {
	if this.next != nil {
		return this.next.CountByValue(mathImpl)
	}
	return typeAAError()
}

/*IReduceImpl*/

func (this *iTypeAA[T1, T2]) GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	if this.next != nil {
		return this.next.GroupByKey(reduceImpl, numPartitions)
	}
	return typeAAError()
}

func (this *iTypeAA[T1, T2]) Join(reduceImpl *impl.IReduceImpl, other string, numPartitions int64) error {
	if this.next != nil {
		return this.next.Join(reduceImpl, other, numPartitions)
	}
	return typeAAError()
}

func (this *iTypeAA[T1, T2]) Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	if this.next != nil {
		return this.next.Distinct(reduceImpl, numPartitions)
	}
	return typeAAError()
}

/*IRepartitionImpl*/
