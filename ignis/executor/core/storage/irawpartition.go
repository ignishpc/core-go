package storage

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/iterator"
	"ignis/executor/core/ierror"
)

type IRawPartition[T any] struct {
	native bool
}

func (this *IRawPartition[T]) Read(transport thrift.TTransport) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}
func (this *IRawPartition[T]) Write(transport thrift.TTransport, compression int8) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) WriteWithNative(transport thrift.TTransport, compression int8, native bool) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) Clone() (IPartitionBase, error) {
	return nil, ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) CopyFrom(source IPartitionBase) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) CopyTo(target IPartitionBase) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) MoveFrom(source IPartitionBase) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) MoveTo(target IPartitionBase) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) Size() int64 {
	return 0
}

func (this *IRawPartition[T]) Empty() bool {
	return true
}

func (this *IRawPartition[T]) Bytes() int64 {
	return 0
}

func (this *IRawPartition[T]) Clear() error {
	return ierror.RaiseMsg("Not implemented yet")
}

func (this *IRawPartition[T]) Fit() error {
	return ierror.RaiseMsg("Not implemented yet")
}

func (this *IRawPartition[T]) Sync() error {
	return ierror.RaiseMsg("Not implemented yet")
}

func (this *IRawPartition[T]) Type() string {
	return ""
}

func (this *IRawPartition[T]) Inner() any {
	return nil
}

func (this *IRawPartition[T]) First() any {
	return nil
}

func (this *IRawPartition[T]) Native() bool {
	return false
}

func (this *IRawPartition[T]) ReadIterator() (iterator.IReadIterator[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *IRawPartition[T]) WriteIterator() (iterator.IWriteIterator[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet") //TODO
}

func DisableNative[T any](this IPartitionBase) {
	this.(*IRawPartition[T]).native = false
}
