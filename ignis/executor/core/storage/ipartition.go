package storage

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/iterator"
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
	Inner() any
	Native() bool
	First() any
}

type IPartition[T any] interface {
	IPartitionBase

	ReadIterator() (iterator.IReadIterator[T], error)
	WriteIterator() (iterator.IWriteIterator[T], error)
}

type IPartitionGroupBase interface {
	GetBase(index int) IPartitionBase
	SetBase(index int, value IPartitionBase)
	Remove(index int)
	Size() int
	Empty() bool
	AddBase(part IPartitionBase)
	Clear()
	CloneBase() (IPartitionGroupBase, error)
	ShadowCopyBase() IPartitionGroupBase
	Cache() bool
	SetCache(e bool)
	NewGroup() IPartitionGroupBase
	AddMemoryPartition(sz int64)
	//AddRawMemoryPartition() error TODO
	//AddDiskPartition() error TODO
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

func (this *IPartitionGroup[T]) SetBase(index int, value IPartitionBase) {
	this.partitions[index] = value.(IPartition[T])
}

func (this *IPartitionGroup[T]) Get(index int) IPartition[T] {
	return this.partitions[index]
}

func (this *IPartitionGroup[T]) GetBase(index int) IPartitionBase {
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

func (this *IPartitionGroup[T]) AddBase(part IPartitionBase) {
	this.partitions = append(this.partitions, part.(IPartition[T]))
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

func (this *IPartitionGroup[T]) CloneBase() (IPartitionGroupBase, error) {
	return this.Clone()
}

func (this *IPartitionGroup[T]) ShadowCopy() *IPartitionGroup[T] {
	group := NewIPartitionGroup[T]()
	for _, p := range this.partitions {
		group.Add(p)
	}
	return group
}

func (this *IPartitionGroup[T]) ShadowCopyBase() IPartitionGroupBase {
	return this.ShadowCopy()
}

func (this *IPartitionGroup[T]) Cache() bool {
	return this._cache
}

func (this *IPartitionGroup[T]) SetCache(e bool) {
	this._cache = e
}

func (this *IPartitionGroup[T]) NewGroup() IPartitionGroupBase {
	return NewIPartitionGroup[T]()
}

func (this *IPartitionGroup[T]) AddMemoryPartition(sz int64) {
	this.Add(NewIMemoryPartition[T](sz, false))
}

func Copy[T any](rit iterator.IReadIterator[T], wit iterator.IWriteIterator[T]) error {
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
