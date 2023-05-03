package storage

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"reflect"
	"testing"
)

type IPartitionTestAbs interface {
	GetName() string
	TestItWriteItRead(t *testing.T)
	TestItWriteTransRead(t *testing.T)
	TestItWriteTransReadNative(t *testing.T)
	TestTransWriteItRead(t *testing.T)
	TestTransWriteNativeItRead(t *testing.T)
	TestTransWriteTransRead(t *testing.T)
	TestTransWriteNativeTransReadNative(t *testing.T)
	TestClear(t *testing.T)
	TestAppendItWrite(t *testing.T)
	TestAppendTransRead(t *testing.T)
	TestCopy(t *testing.T)
	TestMove(t *testing.T)
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

func TestItWriteTransReadNative(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestItWriteTransReadNative)
}

func TestTransWriteItRead(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestTransWriteItRead)
}

func TestTransWriteNativeItRead(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestTransWriteNativeItRead)
}

func TestTransWriteTransRead(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestTransWriteTransRead)
}

func TestTransWriteNativeTransReadNative(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestTransWriteNativeTransReadNative)
}

func TestClear(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestClear)
}

func TestAppendItWrite(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestAppendItWrite)
}

func TestAppendTransRead(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestAppendTransRead)
}

func TestCopy(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestCopy)
}

func TestMove(t *testing.T) {
	executeTest(t, (IPartitionTestAbs).TestMove)
}

type IPartitionTest[T any] struct {
	name    string
	create  func() IPartition[T]
	elemens func(n int, seed int) []T
}

// from ... to ...
func (this *IPartitionTest[T]) writeIterator(t *testing.T, elems []T, part IPartition[T]) {
	it, err := part.WriteIterator()
	require.Nil(t, err)
	for _, e := range elems {
		require.Nil(t, it.Write(e))
	}
}

func (this *IPartitionTest[T]) readIterator(t *testing.T, part IPartition[T]) []T {
	it, err := part.ReadIterator()
	require.Nil(t, err)
	elems := make([]T, part.Size())

	for i := 0; i < int(part.Size()); i++ {
		elem, err := it.Next()
		require.Nil(t, err)
		elems[i] = elem
	}
	return elems
}

func (this *IPartitionTest[T]) read(t *testing.T, elems []T, part IPartition[T], native bool) {
	buffer := thrift.NewTMemoryBuffer()
	zlib, err := itransport.NewIZlibTransport(buffer)
	require.Nil(t, err)
	proto := iprotocol.NewIObjectProtocol(zlib)
	require.Nil(t, proto.WriteObjectWithNative(elems, native))
	require.Nil(t, zlib.Flush(context.Background()))
	require.Nil(t, part.Read(buffer))
}

func (this *IPartitionTest[T]) write(t *testing.T, part IPartition[T], native bool) []T {
	buffer := thrift.NewTMemoryBuffer()
	require.Nil(t, part.WriteWithNative(buffer, 0, native))
	zlib, err := itransport.NewIZlibTransport(buffer)
	require.Nil(t, err)
	proto := iprotocol.NewIObjectProtocol(zlib)
	elems, err := proto.ReadObject()
	require.Nil(t, err)
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
}

func (this *IPartitionTest[T]) GetName() string {
	return this.name
}

func (this *IPartitionTest[T]) TestItWriteItRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.writeIterator(t, elems, part)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestItWriteTransRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.writeIterator(t, elems, part)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.write(t, part, false)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestItWriteTransReadNative(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.writeIterator(t, elems, part)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.write(t, part, true)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestTransWriteItRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.read(t, elems, part, false)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestTransWriteNativeItRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.read(t, elems, part, true)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestTransWriteTransRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.read(t, elems, part, false)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.write(t, part, false)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestTransWriteNativeTransReadNative(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.read(t, elems, part, true)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.write(t, part, true)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestClear(t *testing.T) {
	part := this.create()
	elems := this.elemens(100, 0)
	this.writeIterator(t, elems, part)
	require.Nil(t, part.Clear())
	require.True(t, part.Empty())
}

func (this *IPartitionTest[T]) TestAppendItWrite(t *testing.T) {
	part := this.create()
	elems := this.elemens(200, 0)
	this.writeIterator(t, elems[:100], part)
	this.writeIterator(t, elems[100:], part)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestAppendTransRead(t *testing.T) {
	part := this.create()
	elems := this.elemens(200, 0)
	this.read(t, elems[:100], part, false)
	this.read(t, elems[100:], part, false)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestCopy(t *testing.T) {
	part := this.create()
	part2 := this.create()
	elems := this.elemens(200, 0)
	this.writeIterator(t, elems[:100], part)
	this.writeIterator(t, elems[100:], part2)
	require.Nil(t, part2.CopyTo(part))

	require.Equal(t, len(elems), int(part.Size()))
	require.Equal(t, len(elems)/2, int(part2.Size()))
	result := this.readIterator(t, part)
	result2 := this.readIterator(t, part2)
	require.Equal(t, elems, result)
	require.Equal(t, elems[100:], result2)
}

func (this *IPartitionTest[T]) TestMove(t *testing.T) {
	part := this.create()
	part2 := this.create()
	elems := this.elemens(200, 0)
	this.writeIterator(t, elems[:100], part)
	this.writeIterator(t, elems[100:], part2)
	require.Nil(t, part2.MoveTo(part))

	require.Equal(t, len(elems), int(part.Size()))
	require.Equal(t, 0, int(part2.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}
