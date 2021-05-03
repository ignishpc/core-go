package io

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"reflect"
)

type IReaderType interface {
	Read(protocol thrift.TProtocol) (interface{}, error)
	getStruct() reflect.Type
}

type ReadObj func(protocol thrift.TProtocol) (interface{}, error)

type _IReader struct {
}

var readers = make([]IReaderType, 256)
var IReader _IReader

func (this _IReader) Set(key int8, value IReaderType) {
	readers[key] = value
}

func (this _IReader) Get(key int8) IReaderType {
	return readers[key]
}

func (this _IReader) Remove(key int8) {
	readers[key] = nil
}

func (this _IReader) Read(protocol thrift.TProtocol) (interface{}, error) {
	id, err := ReadTypeAux(protocol)
	if err != nil {
		return nil, err
	}
	reader, err := this.GetReaderType(id)
	if err != nil {
		return nil, err
	}
	return reader.Read(protocol)
}

func ReadTypeAux(protocol thrift.TProtocol) (int8, error) {
	return protocol.ReadByte(ctx)
}

func ReadSizeAux(protocol thrift.TProtocol) (int64, error) {
	return protocol.ReadI64(ctx)
}

func (this _IReader) GetReaderType(id int8) (IReaderType, error) {
	reader := readers[id]
	if reader == nil {
		return nil, ierror.RaiseMsg(fmt.Sprintf("IReaderType not implemented for id %d", id))
	}
	return reader, nil
}

type IReaderTypeImpl struct {
	st   reflect.Type
	read ReadObj
}

func (this *IReaderTypeImpl) Read(protocol thrift.TProtocol) (interface{}, error) {
	return this.read(protocol)
}

func (this *IReaderTypeImpl) getStruct() reflect.Type {
	return this.st
}

func NewIReaderType(tp reflect.Type, read ReadObj) IReaderType {
	return &IReaderTypeImpl{
		tp,
		read,
	}
}

func init() {
	IReader.Set(I_VOID, NewIReaderType(reflect.TypeOf(nil), func(protocol thrift.TProtocol) (interface{}, error) {
		return nil, nil
	}))
	IReader.Set(I_BOOL, NewIReaderType(reflect.TypeOf(nil), func(protocol thrift.TProtocol) (interface{}, error) {
		return protocol.ReadBool(ctx)
	}))
	//TODO add readers
}

