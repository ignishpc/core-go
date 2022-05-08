package base

import (
	"ignis/executor/core/modules/impl"
)

type ITypeFunctions interface {
	Name() string
	LoadType()

	GetPartitions(commImpl *impl.ICommImpl, protocol int8, minPartitions int64) ([][]byte, error)
	SetPartitions(commImpl *impl.ICommImpl, partitions [][]byte) error
	DriverGather(commImpl *impl.ICommImpl, group string) error
	DriverGather0(commImpl *impl.ICommImpl, group string) error
	DriverScatter(commImpl *impl.ICommImpl, group string, partitions int64) error
	ImportData(commImpl *impl.ICommImpl, group string, source bool, threads int64) error

	PartitionApproxSize(ioImpl *impl.IIOImpl) (int64, error)
	PartitionObjectFile(ioImpl *impl.IIOImpl, path string, first int64, partitions int64) error
	SaveAsObjectFile(ioImpl *impl.IIOImpl, path string, compression int8, first int64) error
	SaveAsTextFile(ioImpl *impl.IIOImpl, path string, first int64) error
	SaveAsJsonFile(ioImpl *impl.IIOImpl, path string, first int64, pretty bool) error

	Sort(sortImpl *impl.ISortImpl, ascending bool) error
	SortWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error
	SortByKey(sortImpl *impl.ISortImpl, ascending bool) error
	SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error
	Top(sortImpl *impl.ISortImpl, n int64) error
	TakeOrdered(sortImpl *impl.ISortImpl, n int64) error
	Max(sortImpl *impl.ISortImpl) error
	Min(sortImpl *impl.ISortImpl) error

	Take(pipeImpl *impl.IPipeImpl, num int64) error
	Keys(pipeImpl *impl.IPipeImpl) error
	Values(pipeImpl *impl.IPipeImpl) error

	Sample(mathImpl *impl.IMathImpl, withReplacement bool, num []int64, seed int32) error
	SampleByKeyFilter(mathImpl *impl.IMathImpl) (int64, error)
	SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error
	CountByKey(mathImpl *impl.IMathImpl) error
	CountByValue(mathImpl *impl.IMathImpl) error

	GroupByKey(reduceImpl *impl.IReduceImpl, numPartitions int64) error
	Union(reduceImpl *impl.IReduceImpl, other string, preserveOrder bool) error
	Join(reduceImpl *impl.IReduceImpl, other string, numPartitions int64) error
	Distinct(reduceImpl *impl.IReduceImpl, numPartitions int64) error

	Repartition(repartitionImpl *impl.IRepartitionImpl, numPartitions int64, preserveOrdering bool, global bool) error
	PartitionByRandom(repartitionImpl *impl.IRepartitionImpl, numPartitions int64, seed int32) error
	PartitionByHash(repartitionImpl *impl.IRepartitionImpl, numPartitions int64) error
}

func NewTypeA[T any]() ITypeFunctions {
	return &iTypeA[T]{}
}

func NewTypeC[T comparable]() ITypeFunctions {
	return &iTypeC[T]{}
}

func NewTypeAA[T1 any, T2 any]() ITypeFunctions {
	return &iTypeAA[T1, T2]{}
}

func NewTypeAC[T1 any, T2 comparable]() ITypeFunctions {
	return &iTypeAC[T1, T2]{}
}

func NewTypeCA[T1 comparable, T2 any]() ITypeFunctions {
	return &iTypeCA[T1, T2]{}
}

func NewTypeCC[T1 comparable, T2 comparable]() ITypeFunctions {
	return &iTypeCC[T1, T2]{}
}
