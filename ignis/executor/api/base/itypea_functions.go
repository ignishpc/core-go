package base

import (
	"ignis/executor/api"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
	"ignis/executor/core/modules/impl"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
)

func registerTypeA[T any]() {
	iio.AddBasicType[T]()
	storage.CreateList[T]()
}

type iTypeA[T any] struct {
	tp   string
	next ITypeFunctions
}

func (this *iTypeA[T]) Name() string {
	return utils.TypeName[T]()
}

func (this *iTypeA[T]) Type() string {
	return this.tp
}

func (this *iTypeA[T]) AddType(tp api.IContextType) {
	rtp := tp.(ITypeFunctions)
	if rtp.Type() != this.Type() {
		if this.next != nil {
			this.next.AddType(tp)
		} else {
			this.next = rtp
		}
	}
}

func (this *iTypeA[T]) LoadType() {
	registerTypeA[T]()
}

func typeAError() error {
	return ierror.RaiseMsg("TypeA functions does not implement pair or comparable functions.")
}

/*ICacheImpl*/

func (this *iTypeA[T]) LoadFromDisk(cacheImpl *impl.ICacheImpl, group []string) error {
	return impl.LoadFromDisk[T](cacheImpl, group)
}

/*ICommImpl*/

func (this *iTypeA[T]) GetPartitions(commImpl *impl.ICommImpl, protocol int8, minPartitions int64) ([][]byte, error) {
	return impl.GetPartitions[T](commImpl, protocol, minPartitions)
}

func (this *iTypeA[T]) SetPartitions(commImpl *impl.ICommImpl, partitions [][]byte) error {
	return impl.SetPartitions[T](commImpl, partitions)
}

func (this *iTypeA[T]) DriverGather(commImpl *impl.ICommImpl, group string) error {
	return impl.DriverGather[T](commImpl, group)
}

func (this *iTypeA[T]) DriverGather0(commImpl *impl.ICommImpl, group string) error {
	return impl.DriverGather0[T](commImpl, group)
}

func (this *iTypeA[T]) DriverScatter(commImpl *impl.ICommImpl, group string, partitions int64) error {
	return impl.DriverScatter[T](commImpl, group, partitions)
}

func (this *iTypeA[T]) ImportData(commImpl *impl.ICommImpl, group string, source bool, threads int64) error {
	return impl.ImportData[T](commImpl, group, source, threads)
}

/*IIOImpl*/

func (this *iTypeA[T]) PartitionApproxSize(ioImpl *impl.IIOImpl) (int64, error) {
	return impl.PartitionApproxSize[T](ioImpl)
}

func (this *iTypeA[T]) PartitionObjectFile(ioImpl *impl.IIOImpl, path string, first int64, partitions int64) error {
	return impl.PartitionObjectFile[T](ioImpl, path, first, partitions)
}

func (this *iTypeA[T]) SaveAsObjectFile(ioImpl *impl.IIOImpl, path string, compression int8, first int64) error {
	return impl.SaveAsObjectFile[T](ioImpl, path, compression, first)
}

func (this *iTypeA[T]) SaveAsTextFile(ioImpl *impl.IIOImpl, path string, first int64) error {
	return impl.SaveAsTextFile[T](ioImpl, path, first)
}

func (this *iTypeA[T]) SaveAsJsonFile(ioImpl *impl.IIOImpl, path string, first int64, pretty bool) error {
	return impl.SaveAsJsonFile[T](ioImpl, path, first, pretty)
}

/*ISortImpl*/

func (this *iTypeA[T]) Sort(sortImpl *impl.ISortImpl, ascending bool) error {
	return impl.Sort[T](sortImpl, ascending)
}

func (this *iTypeA[T]) SortWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return impl.SortWithPartitions[T](sortImpl, ascending, partitions)
}

func (this *iTypeA[T]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	if this.next != nil {
		return this.next.SortByKey(sortImpl, ascending)
	}
	return typeAError()
}

func (this *iTypeA[T]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	if this.next != nil {
		return this.next.SortByKeyWithPartitions(sortImpl, ascending, partitions)
	}
	return typeAError()
}

func (this *iTypeA[T]) Top(sortImpl *impl.ISortImpl, n int64) error {
	return impl.Top[T](sortImpl, n)
}

func (this *iTypeA[T]) TakeOrdered(sortImpl *impl.ISortImpl, n int64) error {
	return impl.TakeOrdered[T](sortImpl, n)
}

func (this *iTypeA[T]) Max(sortImpl *impl.ISortImpl) error {
	return impl.Max[T](sortImpl)
}

func (this *iTypeA[T]) Min(sortImpl *impl.ISortImpl) error {
	return impl.Min[T](sortImpl)
}

/*IPipeImpl*/

func (this *iTypeA[T]) Take(pipeImpl *impl.IPipeImpl, num int64) error {
	return impl.Take[T](pipeImpl, num)
}

func (this *iTypeA[T]) Keys(pipeImpl *impl.IPipeImpl) error {
	if this.next != nil {
		return this.next.Keys(pipeImpl)
	}
	return typeAError()
}

func (this *iTypeA[T]) Values(pipeImpl *impl.IPipeImpl) error {
	if this.next != nil {
		return this.next.Values(pipeImpl)
	}
	return typeAError()
}

/*IMathImpl*/

func (this *iTypeA[T]) Sample(mathImpl *impl.IMathImpl, withReplacement bool, num []int64, seed int32) error {
	return impl.Sample[T](mathImpl, withReplacement, num, seed)
}

func (this *iTypeA[T]) SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error) {
	if this.next != nil {
		return this.next.SampleByKeyFilter(mathImpl)
	}
	return 0, typeAError()
}

func (this *iTypeA[T]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	if this.next != nil {
		return this.next.SampleByKey(mathImpl, withReplacement, seed)
	}
	return typeAError()
}

func (this *iTypeA[T]) CountByKey(mathImpl *impl.IMathImpl) error {
	if this.next != nil {
		return this.next.CountByKey(mathImpl)
	}
	return typeAError()
}

func (this *iTypeA[T]) CountByValue(mathImpl *impl.IMathImpl) error {
	if this.next != nil {
		return this.next.CountByValue(mathImpl)
	}
	return typeAError()
}

/*IReduceImpl*/

func (this *iTypeA[T]) GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	if this.next != nil {
		return this.next.GroupByKey(reduceImpl, numPartitions)
	}
	return typeAError()
}

func (this *iTypeA[T]) Union(reduceImpl *impl.IReduceImpl, other string, preserveOrder bool) error {
	return impl.Union[T](reduceImpl, other, preserveOrder)
}

func (this *iTypeA[T]) Join(reduceImpl *impl.IReduceImpl, other string, numPartitions int64) error {
	if this.next != nil {
		return this.next.Join(reduceImpl, other, numPartitions)
	}
	return typeAError()
}

func (this *iTypeA[T]) Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error {
	if this.next != nil {
		return this.next.Distinct(reduceImpl, numPartitions)
	}
	return typeAError()
}

/*IRepartitionImpl*/

func (this *iTypeA[T]) Repartition(repartitionImpl *impl.IRepartitionImpl, numPartitions int64, preserveOrdering bool, global bool) error {
	return impl.Repartition[T](repartitionImpl, numPartitions, preserveOrdering, global)
}

func (this *iTypeA[T]) PartitionByRandom(repartitionImpl *impl.IRepartitionImpl, numPartitions int64, seed int32) error {
	return impl.PartitionByRandom[T](repartitionImpl, numPartitions, seed)
}

func (this *iTypeA[T]) PartitionByHash(repartitionImpl *impl.IRepartitionImpl, numPartitions int64) error {
	if this.next != nil {
		return this.next.PartitionByHash(repartitionImpl, numPartitions)
	}
	return typeAError()
}
