package storage

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api"
)

type IPartitionBase interface {
	Read(transport thrift.TTransport) error

	Write(transport thrift.TTransport, compression int8) error

	WriteWithNative(transport thrift.TTransport, compression int8, native bool) error

	Clone() (IPartitionBase, error)

	CopyFrom(source IPartitionBase) error

	CopyTo(target IPartitionBase) error

	MoveFrom(source IPartitionBase) error

	MoveTo(target IPartitionBase) error

	Size() int64

	Empty() bool

	Bytes() int64

	Clear() error

	Fit() error

	Type() string
}

type IPartition[T any] interface {
	IPartitionBase

	ReadIterator() (api.IReadIterator[T], error)

	WriteIterator() (api.IWriteIterator[T], error)
}

type IPartitionGroup[T any] struct {
	partitions []IPartition[T]
	_cache     bool
}

func NewIPartitionGroup[T any]() *IPartitionGroup[T] {
	return &IPartitionGroup[T]{
		partitions: make([]IPartition[T], 0, 10),
		_cache:     false,
	}
}

func (this *IPartitionGroup[T]) Set(index int, value IPartition[T]) {
	this.partitions[index] = value
}

func (this *IPartitionGroup[T]) Get(index int) IPartition[T] {
	return this.partitions[index]
}

func (this *IPartitionGroup[T]) Remove(index int) {
	this.partitions = append(this.partitions[:index], this.partitions[index+1:]...)
}

func (this *IPartitionGroup[T]) Iter() []IPartition[T] {
	return this.partitions
}

func (this *IPartitionGroup[T]) Size() int {
	return len(this.partitions)
}

func (this *IPartitionGroup[T]) Empty() bool {
	return len(this.partitions) == 0
}

func (this *IPartitionGroup[T]) Add(part IPartition[T]) {
	this.partitions = append(this.partitions, part)
}

func (this *IPartitionGroup[T]) Clear() {
	this.partitions = make([]IPartition[T], 0, 10)
}

func (this *IPartitionGroup[T]) Clone() (*IPartitionGroup[T], error) {
	group := NewIPartitionGroup[T]()
	for _, p := range this.partitions {
		other, err := p.Clone()
		if err != nil {
			return nil, err
		}
		group.Add(other.(IPartition[T]))
	}
	return group, nil
}

func (this *IPartitionGroup[T]) ShadowCopy() (*IPartitionGroup[T], error) {
	group := NewIPartitionGroup[T]()
	for _, p := range this.partitions {
		group.Add(p)
	}
	return group, nil
}

func (this *IPartitionGroup[T]) Cache() bool {
	return this._cache
}

func (this *IPartitionGroup[T]) setCache(e bool) {
	this._cache = e
}

func Copy[T any](rit api.IReadIterator[T], wit api.IWriteIterator[T]) error {
	for elem, err := rit.Next(); rit.HasNext(); elem, err = rit.Next() {
		if err != nil {
			return err
		}
		if err = wit.Write(elem); err != nil {
			return err
		}
	}
	return nil
}
