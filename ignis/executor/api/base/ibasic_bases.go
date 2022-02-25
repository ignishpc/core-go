package base

import "ignis/executor/core/modules/impl"

type IBasicBase interface {
	GetPartitions(commImpl *impl.ICommImpl, protocol int8, minPartitions int64) ([][]byte, error)
	SetPartitions(commImpl *impl.ICommImpl, partitions [][]byte) error
	DriverGather(commImpl *impl.ICommImpl, group string) error
	DriverGather0(commImpl *impl.ICommImpl, group string) error
	DriverScatter(commImpl *impl.ICommImpl, group string, partitions int64) error
	ImportData(commImpl *impl.ICommImpl, group string, source bool, threads int64) error
}

type IBasicBaseImpl[T any] struct {
}

func (this *IBasicBaseImpl[T]) GetPartitions(commImpl *impl.ICommImpl, protocol int8, minPartitions int64) ([][]byte, error) {
	return impl.GetPartitions[T](commImpl, protocol, minPartitions)
}

func (this *IBasicBaseImpl[T]) SetPartitions(commImpl *impl.ICommImpl, partitions [][]byte) error {
	return impl.SetPartitions[T](commImpl, partitions)
}

func (this *IBasicBaseImpl[T]) DriverGather(commImpl *impl.ICommImpl, group string) error {
	return impl.DriverGather[T](commImpl, group)
}

func (this *IBasicBaseImpl[T]) DriverGather0(commImpl *impl.ICommImpl, group string) error {
	return impl.DriverGather0[T](commImpl, group)
}

func (this *IBasicBaseImpl[T]) DriverScatter(commImpl *impl.ICommImpl, group string, partitions int64) error {
	return impl.DriverScatter[T](commImpl, group, partitions)
}

func (this *IBasicBaseImpl[T]) ImportData(commImpl *impl.ICommImpl, group string, source bool, threads int64) error {
	return impl.ImportData[T](commImpl, group, source, threads)
}
