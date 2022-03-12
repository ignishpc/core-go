package base

import (
	"ignis/executor/api"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
	"ignis/executor/core/modules/impl"
	"ignis/executor/core/storage"
)

func RegisterType[T any](ctx api.IContext) {
	iio.AddBasicType[T]()
	storage.CreateList[T]()
	ctx.(ITypeBaseIContext).AddType(iio.TypeName[T](), &ITypeBaseImpl[T]{})
}

func RegisterTypeWithKey[K comparable, V any](ctx api.IContext) {
	RegisterType[K](ctx)
	RegisterType[V](ctx)
	RegisterType[ipair.IPair[K, V]](ctx)
	RegisterType[map[K]V](ctx)
	iio.AddKeyType[K, V]()
	ctx.(ITypeBaseIContext).AddType(iio.TypeName[ipair.IPair[K, V]](), &IPairTypeBaseImpl[V, K]{})
}

type ITypeBaseIContext interface {
	AddType(name string, tp any)
}

type ITypeBase interface {
	Name() string
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

	Sample(mathImpl *impl.IMathImpl, withReplacement bool, num []int64, seed int32) error
	SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error
	CountByKey(mathImpl *impl.IMathImpl) error
	CountByValue(mathImpl *impl.IMathImpl) error
}

type ITypeBaseImpl[T any] struct {
}

func (this *ITypeBaseImpl[T]) Name() string {
	return iio.TypeName[T]()
}

func (this *ITypeBaseImpl[T]) GetPartitions(commImpl *impl.ICommImpl, protocol int8, minPartitions int64) ([][]byte, error) {
	return impl.GetPartitions[T](commImpl, protocol, minPartitions)
}

func (this *ITypeBaseImpl[T]) SetPartitions(commImpl *impl.ICommImpl, partitions [][]byte) error {
	return impl.SetPartitions[T](commImpl, partitions)
}

func (this *ITypeBaseImpl[T]) DriverGather(commImpl *impl.ICommImpl, group string) error {
	return impl.DriverGather[T](commImpl, group)
}

func (this *ITypeBaseImpl[T]) DriverGather0(commImpl *impl.ICommImpl, group string) error {
	return impl.DriverGather0[T](commImpl, group)
}

func (this *ITypeBaseImpl[T]) DriverScatter(commImpl *impl.ICommImpl, group string, partitions int64) error {
	return impl.DriverScatter[T](commImpl, group, partitions)
}

func (this *ITypeBaseImpl[T]) ImportData(commImpl *impl.ICommImpl, group string, source bool, threads int64) error {
	return impl.ImportData[T](commImpl, group, source, threads)
}

func (this *ITypeBaseImpl[T]) PartitionApproxSize(ioImpl *impl.IIOImpl) (int64, error) {
	return impl.PartitionApproxSize[T](ioImpl)
}

func (this *ITypeBaseImpl[T]) PartitionObjectFile(ioImpl *impl.IIOImpl, path string, first int64, partitions int64) error {
	return impl.PartitionObjectFile[T](ioImpl, path, first, partitions)
}

func (this *ITypeBaseImpl[T]) SaveAsObjectFile(ioImpl *impl.IIOImpl, path string, compression int8, first int64) error {
	return impl.SaveAsObjectFile[T](ioImpl, path, compression, first)
}

func (this *ITypeBaseImpl[T]) SaveAsTextFile(ioImpl *impl.IIOImpl, path string, first int64) error {
	return impl.SaveAsTextFile[T](ioImpl, path, first)
}

func (this *ITypeBaseImpl[T]) SaveAsJsonFile(ioImpl *impl.IIOImpl, path string, first int64, pretty bool) error {
	return impl.SaveAsJsonFile[T](ioImpl, path, first, pretty)
}

func (this *ITypeBaseImpl[T]) Sort(sortImpl *impl.ISortImpl, ascending bool) error {
	return impl.Sort[T](sortImpl, ascending)
}

func (this *ITypeBaseImpl[T]) SortWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return impl.SortWithPartitions[T](sortImpl, ascending, partitions)
}

func (this *ITypeBaseImpl[T]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	return typeError[T]()
}

func (this *ITypeBaseImpl[T]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return typeError[T]()
}

func (this *ITypeBaseImpl[T]) Top(sortImpl *impl.ISortImpl, n int64) error {
	return impl.Top[T](sortImpl, n)
}

func (this *ITypeBaseImpl[T]) TakeOrdered(sortImpl *impl.ISortImpl, n int64) error {
	return impl.TakeOrdered[T](sortImpl, n)
}

func (this *ITypeBaseImpl[T]) Max(sortImpl *impl.ISortImpl) error {
	return impl.Max[T](sortImpl)
}

func (this *ITypeBaseImpl[T]) Min(sortImpl *impl.ISortImpl) error {
	return impl.Min[T](sortImpl)
}

func (this *ITypeBaseImpl[T]) Sample(mathImpl *impl.IMathImpl, withReplacement bool, num []int64, seed int32) error {
	return impl.Sample[T](mathImpl, withReplacement, num, seed)
}

func (this *ITypeBaseImpl[T]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	return typeError[T]()
}

func (this *ITypeBaseImpl[T]) CountByKey(mathImpl *impl.IMathImpl) error {
	return typeError[T]()
}

func (this *ITypeBaseImpl[T]) CountByValue(mathImpl *impl.IMathImpl) error {
	return typeError[T]()
}

func typeError[T any]() error {
	if iio.TypeGenericName[T]() == iio.TypeGenericName[ipair.IPair[any, any]]() {
		return ierror.RaiseMsg("To execute this function, this type must be register using RegisterTypeWithKey")
	}
	return ierror.RaiseMsg("Only pairs can execute this functions")
}

type IPairTypeBaseImpl[T any, K comparable] struct {
	ITypeBaseImpl[ipair.IPair[K, T]]
}

func (this *IPairTypeBaseImpl[T, K]) SortByKey(sortImpl *impl.ISortImpl, ascending bool) error {
	return impl.SortByKey[T, K](sortImpl, ascending)
}

func (this *IPairTypeBaseImpl[T, K]) SortByKeyWithPartitions(sortImpl *impl.ISortImpl, ascending bool, partitions int64) error {
	return impl.SortByKeyWithPartitions[T, K](sortImpl, ascending, partitions)
}

func (this *IPairTypeBaseImpl[T, K]) SampleByKey(mathImpl *impl.IMathImpl, withReplacement bool, seed int32) error {
	return impl.SampleByKey[T, K](mathImpl, withReplacement, seed)
}

func (this *IPairTypeBaseImpl[T, K]) CountByKey(mathImpl *impl.IMathImpl) error {
	return impl.CountByKey[T, K](mathImpl)
}

func (this *IPairTypeBaseImpl[T, K]) CountByValue(mathImpl *impl.IMathImpl) error {
	return impl.CountByValue[T, K](mathImpl)
}
