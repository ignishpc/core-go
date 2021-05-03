package io

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"reflect"
)

type IWriterType interface {
	GetWriter(obj interface{}) IWriterType
	Write(protocol thrift.TProtocol, obj interface{}) error
	WriteType(protocol thrift.TProtocol) error
	GetType() int8
	Set(key reflect.Type, value IWriterType)
	Get(key reflect.Type) IWriterType
	Remove(key reflect.Type)
}

type WriteObj func(protocol thrift.TProtocol, obj interface{}) error

type _IWriter struct {
}

var writers map[reflect.Type]IWriterType
var IWriter _IWriter

func (this _IWriter) Set(key reflect.Type, value IWriterType) {
	writers[key] = value
}

func (this _IWriter) Get(key reflect.Type) IWriterType {
	return writers[key]
}

func (this _IWriter) Remove(key reflect.Type) {
	delete(writers, key)
}

func (this _IWriter) Write(protocol thrift.TProtocol, obj interface{}) error {
	writer, err := this.GetWriterType(obj)
	if err != nil {
		return err
	}
	if err := writer.WriteType(protocol); err != nil {
		return nil
	}
	return writer.Write(protocol, obj)

}

func WriteTypeAux(protocol thrift.TProtocol, tp int8) error {
	return protocol.WriteByte(ctx, tp)
}

func WriteSizeAux(protocol thrift.TProtocol, sz int64) error {
	return protocol.WriteI64(ctx, sz)
}

func (this _IWriter) GetWriterType(obj interface{}) (IWriterType, error) {
	tp := reflect.TypeOf(obj)
	writer, found := writers[tp]
	if !found {
		return nil, ierror.RaiseMsg(fmt.Sprintf("IWriterType not implemented for id %s", tp.Name()))
	}
	return writer.GetWriter(obj), nil
}

func (this _IWriter) GetWriterTypeStruct(st reflect.Type) (IWriterType, error) {
	writer, found := writers[st]
	if !found {
		return nil, ierror.RaiseMsg(fmt.Sprintf("IWriterType not implemented for id %s", st.Name()))
	}
	return writer, nil
}

type IWriterTypeImpl struct {
	writers map[reflect.Type]IWriterType
	tp      int8
	write   WriteObj
}

func NewIWriteType(tp int8, write WriteObj) IWriterType {
	return &IWriterTypeImpl{
		map[reflect.Type]IWriterType{},
		tp,
		write,
	}
}

func (this *IWriterTypeImpl) GetWriter(obj interface{}) IWriterType {
	tp := reflect.TypeOf(obj)
	writer, found := writers[tp]
	if !found {
		return this
	}
	return writer.GetWriter(obj)
}

func (this *IWriterTypeImpl) Write(protocol thrift.TProtocol, obj interface{}) error {
	return this.write(protocol, obj)
}

func (this *IWriterTypeImpl) WriteType(protocol thrift.TProtocol) error {
	return WriteTypeAux(protocol, this.tp)
}

func (this *IWriterTypeImpl) GetType() int8 {
	return this.tp
}

func (this *IWriterTypeImpl) Set(key reflect.Type, value IWriterType) {
	this.writers[key] = value
}

func (this *IWriterTypeImpl) Get(key reflect.Type) IWriterType {
	return this.writers[key]
}

func (this *IWriterTypeImpl) Remove(key reflect.Type) {
	delete(this.writers, key)
}

func init() {
	IWriter.Set(reflect.TypeOf(nil), NewIWriteType(I_VOID, func(protocol thrift.TProtocol, obj interface{}) error {
		return nil
	}))
	IWriter.Set(reflect.TypeOf(nil), NewIWriteType(I_BOOL, func(protocol thrift.TProtocol, obj interface{}) error {
		return protocol.WriteBool(ctx, obj.(bool))
	}))
	//TODO add writers
}
