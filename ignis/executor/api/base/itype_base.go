package base

import (
	"ignis/executor/api"
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
	RegisterType[map[K]V](ctx)
	iio.AddKeyType[K, V]()
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
