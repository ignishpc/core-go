package storage

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api"
	"ignis/executor/core/iarray"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"reflect"
)

const IMemoryPartitionType = "IMemoryPartition"

type IMemoryPartition struct {
	init_sz int64
	elems   iarray.IArray
	factory iarray.NewIArray
	native  bool
}

func NewIMemoryPartition(sz int64) (*IMemoryPartition, error) {
	return NewIMemoryPartitionNative(sz, false)
}

func NewIMemoryPartitionNative(sz int64, native bool) (*IMemoryPartition, error) {
	return &IMemoryPartition{
		sz,
		nil,
		nil,
		native,
	}, nil
}

func NewIMemoryPartitionWrap(a iarray.IArray, native bool) (*IMemoryPartition, error) {
	return &IMemoryPartition{
		a.Len(),
		a,
		nil,
		native,
	}, nil
}

func (this *IMemoryPartition) ReadIterator() (api.IReadIterator, error) {
	return &iMemoryReadIterator{0, this.elems}, nil
}

func (this *IMemoryPartition) WriteIterator() (api.IWriteIterator, error) {
	return &iMemoryWriteIterator{this.init_sz, &this.elems}, nil
}

func (this *IMemoryPartition) Read(transport thrift.TTransport) error {
	zlibTrans, err := itransport.NewTZlibTransport(transport, 0)
	if err != nil {
		return ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlibTrans)
	if this.elems != nil {
		err = proto.ReadInObject(this.elems.Array())
	} else {
		this.elems, err = proto.ReadObjectAsArray()
	}
	if err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IMemoryPartition) Write(transport thrift.TTransport, compression int8, native *bool) error {
	if native == nil {
		native = &this.native
	}
	zlibTrans, err := itransport.NewTZlibTransport(transport, int(compression))
	if err != nil {
		return ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlibTrans)
	if this.elems != nil {
		err = proto.WriteObjectNative(this.elems.Array(), *native)
	} else {
		err = proto.WriteObjectNative(make([]interface{}, 0, 0), *native)
	}
	if err != nil {
		return ierror.Raise(err)
	}
	err2 := zlibTrans.Flush(context.Background())
	if err2 != nil {
		return ierror.Raise(err2)
	}
	return nil
}

func (this *IMemoryPartition) Clone() (IPartition, error) {
	other, err := NewIMemoryPartition(this.init_sz)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return other, this.CopyTo(other)
}

func (this *IMemoryPartition) CopyFrom(source IPartition) error {
	if men, ok := source.(*IMemoryPartition); ok {
		first := this.Size()
		this.elems.Resize(this.Size()+source.Size(), false)
		for i := int64(0); i < men.Size(); i++ {
			this.elems.Set(first+i, men.elems.Get(i))
		}
	} else {
		it, err := source.ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		first := this.Size()
		this.elems.Resize(this.Size()+source.Size(), false)
		for i := int64(0); i < men.Size(); i++ {
			obj, err2 := it.Next()
			this.elems.Set(first+i, obj)
			if err2 != nil {
				return ierror.Raise(err2)
			}
		}
	}
	return nil
}

func (this *IMemoryPartition) CopyTo(target IPartition) error {
	return target.CopyFrom(this)
}

func (this *IMemoryPartition) MoveFrom(source IPartition) error {
	if men, ok := source.(*IMemoryPartition); ok {
		if this.Empty() {
			this.elems = men.elems
			men.elems = nil
			return nil
		}
	}
	if err := source.CopyFrom(source); err!= nil{
		return ierror.Raise(err)
	}
	return source.Clear()
}

func (this *IMemoryPartition) MoveTo(target IPartition) error {
	return target.MoveFrom(this)
}

func (this *IMemoryPartition) Size() int64 {
	if this.elems == nil {
		return 0
	}
	return this.elems.Len()
}

func (this *IMemoryPartition) Empty() bool {
	return this.Size() == 0
}

func (this *IMemoryPartition) Bytes() int64 {
	if this.elems == nil || this.elems.Len() == 0 {
		return 0
	}
	tp := reflect.TypeOf(this.elems.Get(0))
	return this.elems.Len() * int64(tp.Size())
}

func (this *IMemoryPartition) Clear() error {
	this.elems.Resize(this.elems.Len(), false)
	return nil
}

func (this *IMemoryPartition) Fit() error {
	this.elems.Resize(this.elems.Len(), true)
	return nil
}

func (this *IMemoryPartition) Type() string {
	return IMemoryPartitionType
}

type iMemoryReadIterator struct {
	pos   int64
	elems iarray.IArray
}

func (this *iMemoryReadIterator) HasNext() bool {
	return this.elems.Len() > this.pos
}
func (this *iMemoryReadIterator) Next() (interface{}, error) {
	pos := this.pos
	this.pos += 1
	return this.elems.Get(pos), nil
}

type iMemoryWriteIterator struct {
	init_sz int64
	elems   *iarray.IArray
}

func (this *iMemoryWriteIterator) Write(v interface{}) error {
	if *this.elems == nil {
		*this.elems = iarray.Get(reflect.TypeOf(v))(0, this.init_sz, nil)
	}
	(*this.elems).Append(v)
	return nil
}
