package storage

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api"
)

type IPartition interface {
	ReadIterator() (api.IReadIterator, error)

	WriteIterator() (api.IWriteIterator, error)

	Read(transport thrift.TTransport) error

	Write(transport thrift.TTransport, compression int8, native *bool) error

	Clone() (IPartition, error)

	CopyFrom(source IPartition) error

	CopyTo(target IPartition) error

	MoveFrom(source IPartition) error

	MoveTo(target IPartition) error

	Size() int64

	Empty() bool

	Bytes() int64

	Clear() error

	Fit() error

	Type() string
}

type IPartitionGroup struct {
	partitions []IPartition
	_cache     bool
}

func NewIPartitionGroup() *IPartitionGroup {
	return &IPartitionGroup{
		partitions: make([]IPartition, 0, 10),
		_cache:     false,
	}
}

func (this *IPartitionGroup) Set(index int64, value IPartition) {
	this.partitions[index] = value
}

func (this *IPartitionGroup) Get(index int64) IPartition {
	return this.partitions[index]
}

func (this *IPartitionGroup) Remove(index int64) {
	this.partitions = append(this.partitions[:index], this.partitions[index+1:]...)
}

func (this *IPartitionGroup) Iter() []IPartition {
	return this.partitions
}

func (this *IPartitionGroup) Size() int64 {
	return int64(len(this.partitions))
}

func (this *IPartitionGroup) Empty() bool {
	return len(this.partitions) == 0
}

func (this *IPartitionGroup) Add(part IPartition) {
	this.partitions = append(this.partitions, part)
}

func (this *IPartitionGroup) Clear() {
	this.partitions = make([]IPartition, 0, 10)
}

func (this *IPartitionGroup) Clone() (*IPartitionGroup, error) {
	copy := NewIPartitionGroup()
	for _, p := range this.partitions {
		other, err := p.Clone()
		if err != nil {
			return nil, err
		}
		copy.Add(other)
	}
	return copy, nil
}

func (this *IPartitionGroup) ShadowCopy() (*IPartitionGroup, error) {
	copy := NewIPartitionGroup()
	for _, p := range this.partitions {
		copy.Add(p)
	}
	return copy, nil
}

func (this *IPartitionGroup) Cache() bool {
	return this._cache
}

func (this *IPartitionGroup) setCache(e bool) {
	this._cache = e
}

func Copy(rit api.IReadIterator, wit api.IWriteIterator) error {
	for elem, err := rit.Next(); elem != nil; elem, err = rit.Next() {
		if err != nil {
			return err
		}
		if err = wit.Write(elem); err != nil {
			return err
		}
	}
	return nil
}
