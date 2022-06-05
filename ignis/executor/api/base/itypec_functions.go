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
	if this.next != nil {
		return this.next.SortByKey(sortImpl, ascending)
	}
	return typeCError()
}

func (this *iTypeC[T]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	if this.next != nil {
		return this.next.SortByKeyWithPartitions(sortImpl, ascending, partitions)
	}
	return typeCError()
}

/*IPipeImpl*/

func (this *iTypeC[T]) Keys(pipeImpl *impl.IPipeImpl) error {
	if this.next != nil {
		return this.next.Keys(pipeImpl)
	}
	return typeCError()
}

func (this *iTypeC[T]) Values(pipeImpl *impl.IPipeImpl) error {
	if this.next != nil {
		return this.next.Values(pipeImpl)
	}
	return typeCError()
}

/*IMathImpl*/

func (this *iTypeC[T]) SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error) {
	if this.next != nil {
		return this.next.SampleByKeyFilter(mathImpl)
	}
	return 0, typeCError()
}

func (this *iTypeC[T]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	if this.next != nil {
		return this.next.SampleByKey(mathImpl, withReplacement, seed)
	}
	return typeCError()
}

func (this *iTypeC[T]) CountByKey(mathImpl *impl.IMathImpl) error {
	if this.next != nil {
		return this.next.CountByKey(mathImpl)
	}
	return typeCError()
}

func (this *iTypeC[T]) CountByValue(mathImpl *impl.IMathImpl) error {
	if this.next != nil {
		return this.next.CountByValue(mathImpl)
	}
	return typeCError()
}

/*IReduceImpl*/

func (this *iTypeC[T]) GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	if this.next != nil {
		return this.next.GroupByKey(reduceImpl, numPartitions)
	}
	return typeCError()
}

func (this *iTypeC[T]) Join(reduceImpl *impl.IReduceImpl, other string, numPartitions int64) error {
	if this.next != nil {
		return this.next.Join(reduceImpl, other, numPartitions)
	}
	return typeCError()
}

func (this *iTypeC[T]) Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	return impl.Distinct[T](reduceImpl, numPartitions)
}

/*IRepartitionImpl*/
