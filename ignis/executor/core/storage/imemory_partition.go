package storage

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api"
	"ignis/executor/core/ierror"
)

type ArrayHolder interface {
	Get(index int64) interface{}
	Set(index int64, value interface{})
	Resize(sz int64) error
	Size() int64
	TSize() int64
	Factory() ArrayHolderFactory
}

type ArrayHolderFactory interface {
	New(sz int64) (ArrayHolder, error)
	Tp() string
}

var holders map[string]ArrayHolderFactory

func init() {
	//addHolder(BasicArrayHolderFactory{})
	//addHolder(Int64ArrayHolderFactory{})
}

func holder(tp string) (ArrayHolderFactory, error) {
	factory, found := holders[tp]
	if !found {
		return nil, ierror.RaiseMsg(fmt.Sprintf("%s type is not valid for IMemoryParition", tp))
	}
	return factory, nil
}

const IMemoryPartitionType = "IMemoryPartition"

type IMemoryPartition struct {
	elems   int64
	holder  ArrayHolder
	native  bool
	factory ArrayHolderFactory
}

func NewIMemoryPartition(sz int64) (*IMemoryPartition, error) {
	return NewIMemoryPartition3(sz, false, "interface")
}

func NewIMemoryPartition2(sz int64, native bool) (*IMemoryPartition, error) {
	return NewIMemoryPartition3(sz, native, "interface")
}

func NewIMemoryPartition3(sz int64, native bool, cls string) (*IMemoryPartition, error) {
	holder, err := holder(cls)
	if err != nil {
		return nil, err
	}
	array, err := holder.New(sz)
	if err != nil {
		return nil, err
	}
	return &IMemoryPartition{
		0,
		array,
		native,
		holder,
	}, nil
}

func NewIMemoryPartitionWrap(elem ArrayHolder, native bool) (*IMemoryPartition, error) {
	return &IMemoryPartition{
		elem.Size(),
		elem,
		native,
		elem.Factory(),
	}, nil
}

func (this *IMemoryPartition) ReadIterator() (api.IReadIterator, error) {
	return nil, nil //TODO
}

func (this *IMemoryPartition) WriteIterator() (api.IWriteIterator, error) {
	return nil, nil //TODO
}

func (this *IMemoryPartition) Read(transport thrift.TTransport) error {
	return nil //TODO
}

func (this *IMemoryPartition) Write(transport thrift.TTransport, compression int8, native *bool) error {
	return nil //TODO
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
	return this.elems
}

func (this *IMemoryPartition) Empty() bool {
	return this.Size() == 0
}

func (this *IMemoryPartition) Bytes() int64 {
	return this.holder.TSize() * this.holder.Size()
}

func (this *IMemoryPartition) Clear() error {
	this.elems = 0
	return nil
}

func (this *IMemoryPartition) Fit() error {
	return this.holder.Resize(this.Size())
}

func (this *IMemoryPartition) Type() string {
	return IMemoryPartitionType
}

/*      Holders        */
/*Basic*/
func addHolder(holder ArrayHolderFactory) {
	holders[holder.Tp()] = holder
}

type BasicArrayHolderFactory struct {
}

func (this BasicArrayHolderFactory) New(sz int64) (ArrayHolder, error) {
	return &BasicArrayHolder{
		elems: make([]interface{}, sz),
	}, nil
}

func (this BasicArrayHolderFactory) Tp() string {
	return "interface"
}

type BasicArrayHolder struct {
	elems []interface{}
}

func (this *BasicArrayHolder) Get(index int64) interface{} {
	return this.elems[index]
}

func (this *BasicArrayHolder) Set(index int64, value interface{}) {
	this.elems[index] = value
}

func (this *BasicArrayHolder) Resize(sz int64) error {
	return nil//TODO
}

func (this *BasicArrayHolder) Size() int64 {
	return int64(len(this.elems))
}

func (this *BasicArrayHolder) TSize() int64 {
	return 500
}

func (this *BasicArrayHolder) Factory() ArrayHolderFactory {
	return &BasicArrayHolderFactory{}
}

/*int64*/
type Int64ArrayHolderFactory struct {
}

func (this Int64ArrayHolderFactory) New(sz int64) (ArrayHolder, error) {
	return &BasicArrayHolder{
		elems: make([]interface{}, sz),
	}, nil
}

func (this Int64ArrayHolderFactory) Tp() string {
	return "int64"
}

type Int64ArrayHolder struct {
	elems []int64
}

func (this *Int64ArrayHolder) Get(index int64) interface{} {
	return this.elems[index]
}

func (this *Int64ArrayHolder) Set(index int64, value interface{}) {
	this.elems[index] = value.(int64)
}

func (this *Int64ArrayHolder) Resize(sz int64) error {
	return nil//TODO
}

func (this *Int64ArrayHolder) Size() int64 {
	return int64(len(this.elems))
}

func (this *Int64ArrayHolder) TSize() int64 {
	return -1
}

func (this *Int64ArrayHolder) Factory() ArrayHolderFactory {
	return &Int64ArrayHolderFactory{}
}
