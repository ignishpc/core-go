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
	elems   iarray.IArray
	factory iarray.NewIArray
	native  bool
}

func NewIMemoryPartition(sz int64) (*IMemoryPartition, error) {
	return NewIMemoryPartitionNative(sz, false)
}

func NewIMemoryPartitionNative(sz int64, native bool) (*IMemoryPartition, error) {
	return &IMemoryPartition{
		iarray.NewIPointerArray(0, sz, nil),
		iarray.NewIPointerArray,
		native,
	}, nil
}

func NewIMemoryPartitionWrap(elem iarray.IArray, native bool) (*IMemoryPartition, error) {
	return &IMemoryPartition{
		iarray.NewIPointerArray(0, 0, elem),
		iarray.NewIPointerArray,
		native,
	}, nil
}

func (this *IMemoryPartition) ReadIterator() (api.IReadIterator, error) {
	return nil, nil //TODO
}

func (this *IMemoryPartition) WriteIterator() (api.IWriteIterator, error) {
	return nil, nil //TODO
}

func (this *IMemoryPartition) Read(transport thrift.TTransport) error {
	zlibTrans, err := itransport.NewTZlibTransport(transport, 0)
	if err != nil {
		return ierror.RaiseMsg(err.Error())
	}
	proto := iprotocol.NewIObjectProtocol(zlibTrans)
	err2 := proto.ReadInObject(this.elems.Array())
	if err2 != nil {
		return ierror.RaiseMsg(err2.Error())
	}
	return nil
}

func (this *IMemoryPartition) Write(transport thrift.TTransport, compression int8, native *bool) error {
	if native == nil {
		native = &this.native
	}
	zlibTrans, err := itransport.NewTZlibTransport(transport, int(compression))
	if err != nil {
		return ierror.RaiseMsg(err.Error())
	}
	proto := iprotocol.NewIObjectProtocol(zlibTrans)
	err2 := proto.WriteObjectNative(this.elems.Array(), *native)
	if err2 != nil {
		return ierror.RaiseMsg(err2.Error())
	}
	err3 := zlibTrans.Flush(context.Background())
	if err3 != nil {
		return ierror.RaiseMsg(err3.Error())
	}
	return nil
}

func (this *IMemoryPartition) Clone() (IPartition, error) {
	return nil, nil //TODO
}

func (this *IMemoryPartition) CopyFrom(source IPartition) error {
	return nil //TODO
}

func (this *IMemoryPartition) CopyTo(target IPartition) error {
	return nil //TODO
}

func (this *IMemoryPartition) MoveFrom(source IPartition) error {
	return nil //TODO
}

func (this *IMemoryPartition) MoveTo(target IPartition) error {
	return nil //TODO
}

func (this *IMemoryPartition) Size() int64 {
	return this.elems.Len()
}

func (this *IMemoryPartition) Empty() bool {
	return this.Size() == 0
}

func (this *IMemoryPartition) Bytes() int64 {
	tp := reflect.TypeOf(this.elems.Array()).Elem()
	tsize := tp.Size()
	if tp.Kind() == reflect.Ptr {
		tsize += 100
	}
	return this.elems.Len() * int64(tsize)
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
