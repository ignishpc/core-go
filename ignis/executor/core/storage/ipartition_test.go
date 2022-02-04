package storage

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"reflect"
	"testing"
)

type IPartitionTestAbs interface {
	GetName() string
	TestItWriteItRead(t *testing.T)
	TestItWriteTransRead(t *testing.T)
}

func addPartitionTest(test IPartitionTestAbs) {
	partitionTestSuit = append(partitionTestSuit, test)
}

var partitionTestSuit = make([]IPartitionTestAbs, 0)

func executeTest(t *testing.T, f func(IPartitionTestAbs, *testing.T)) {
	if len(partitionTestSuit) == 0 {
		t.Skip()
	}
	for _, test := range partitionTestSuit {
		t.Log(test.GetName())
		f(test, t)
	}
}

func TestItWriteItRead(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestItWriteItRead)
}

func TestItWriteTransRead(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestItWriteTransRead)
}

type IPartitionTest[T any] struct {
	name    string
	create  func() IPartition[T]
	elemens func(n int) []T
}

//from ... to ...
func (this *IPartitionTest[T]) writeIterator(elems []T, part IPartition[T]) {
	it, err := part.WriteIterator()
	if err != nil {
		panic(err)
	}
	for _, e := range elems {
		if err := it.Write(e); err != nil {
			panic(err)
		}
	}
}

func (this *IPartitionTest[T]) readIterator(part IPartition[T]) []T {
	it, err := part.ReadIterator()
	if err != nil {
		panic(err)
	}
	elems := make([]T, part.Size())

	for i := 0; i < int(part.Size()); i++ {
		elem, err := it.Next()
		if err != nil {
			panic(err)
		}
		elems[i] = elem
	}
	return elems
}

func (this *IPartitionTest[T]) read(elems []T, part IPartition[T], native bool) {
	buffer := thrift.NewTMemoryBuffer()
	zlib, err := itransport.NewIZlibTransport(buffer)
	if err != nil {
		panic(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlib)
	if err := proto.WriteObjectWithNative(elems, native); err != nil {
		panic(err)
	}
	zlib.Flush(context.Background())
	if err := part.Read(buffer); err != nil {
		panic(err)
	}
}

func (this *IPartitionTest[T]) write(part IPartition[T], native bool) []T {
	buffer := thrift.NewTMemoryBuffer()
	part.WriteWithNative(buffer, 0, native)
	zlib, err := itransport.NewIZlibTransport(buffer)
	if err != nil {
		panic(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlib)
	elems, err := proto.ReadObject()
	if err != nil {
		panic(err)
	}
	if elems2, ok := elems.([]T); ok {
		return elems2
	} else {
		value := reflect.ValueOf(elems)
		elems2 := make([]T, value.Len())
		for i := 0; i < len(elems2); i++ {
			elems2[i] = value.Index(i).Interface().(T)
		}
		return elems2
	}

	return elems.([]T)
}

func (this *IPartitionTest[T]) GetName() string {
	return this.name
}

func (this *IPartitionTest[T]) TestItWriteItRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100)
	this.writeIterator(elems, part)
	assert.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(part)
	assert.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestItWriteTransRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100)
	this.writeIterator(elems, part)
	assert.Equal(t, len(elems), int(part.Size()))
	result := this.write(part, false)
	assert.Equal(t, elems, result)
}
