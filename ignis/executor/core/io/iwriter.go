package io

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"reflect"
	"strings"
)

type IWriterType interface {
	Write(protocol thrift.TProtocol, obj any) error
	WriteType(protocol thrift.TProtocol) error
	Type() int8
	SetWriter(id any, value IWriterType)
	GetWriter(rt reflect.Type) (IWriterType, error)
	RemoveWriter(id any)
}

type IWriterTypeF func(protocol thrift.TProtocol, obj any) error

var writers = map[string]IWriterType{}
var gen_writers [256]IWriterType

func SetWriter[T any](value IWriterType) {
	writers[typeName[T]()] = value
}

func SetKindWriter(id reflect.Kind, value IWriterType) {
	gen_writers[id] = value
}

func SetGenericWriter[T any](value IWriterType) {
	any_name := typeName[T]()
	gen_name := any_name[0:strings.IndexByte(any_name, '[')]
	writers[gen_name] = value
}

func GetWriter(rt reflect.Type) (IWriterType, error) {
	if writer := writers[rt.String()]; writer != nil {
		return writer.GetWriter(rt)
	}
	if writer := gen_writers[rt.Kind()]; writer != nil {
		return writer.GetWriter(rt)
	}
	return nil, ierror.RaiseMsg(fmt.Sprintf("IWriterType not implemented for %s", rt.String()))
}

func RemoveWriter[T any](key int8) {
	delete(writers, typeName[T]())
}

func WriteTypeAux(protocol thrift.TProtocol, tp int8) error {
	return protocol.WriteByte(ctx, tp)
}

func WriteSizeAux(protocol thrift.TProtocol, sz int64) error {
	return protocol.WriteI64(ctx, sz)
}

func Write(protocol thrift.TProtocol, obj any) error {
	writer, err := GetWriter(reflect.TypeOf(obj))
	if err != nil {
		return ierror.Raise(err)
	}
	if err = writer.WriteType(protocol); err != nil {
		return nil
	}
	return writer.Write(protocol, obj)

}

type IWriteTypeImpl struct {
	tp      int8
	write   IWriterTypeF
	writers map[any]IWriterType
}

func NewIWriteType(tp int8, f IWriterTypeF) IWriterType {
	return &IWriteTypeImpl{tp, f, map[any]IWriterType{}}
}

func (this *IWriteTypeImpl) Write(protocol thrift.TProtocol, obj any) error {
	return this.write(protocol, obj)
}

func (this *IWriteTypeImpl) WriteType(protocol thrift.TProtocol) error {
	return WriteTypeAux(protocol, this.tp)
}

func (this *IWriteTypeImpl) Type() int8 {
	return this.tp
}

func (this *IWriteTypeImpl) SetWriter(id any, value IWriterType) {
	this.writers[id] = value
}

func (this *IWriteTypeImpl) GetWriter(rt reflect.Type) (IWriterType, error) {
	if len(this.writers) == 0 {
		return this, nil
	}
	if writer := this.writers[rt.String()]; writer != nil {
		return writer, nil
	}
	if writer := this.writers[rt.Kind()]; writer != nil {
		return writer, nil
	}
	return this, nil
}

func (this *IWriteTypeImpl) RemoveWriter(id any) {
	delete(this.writers, id)
}

type IWriteTypePointer struct {
	writer IWriterType
}

func (this *IWriteTypePointer) Write(protocol thrift.TProtocol, obj any) error {
	value := reflect.ValueOf(obj)
	return this.writer.Write(protocol, value.Elem().Interface())
}

func (this *IWriteTypePointer) WriteType(protocol thrift.TProtocol) error {
	return this.writer.WriteType(protocol)
}

func (this *IWriteTypePointer) Type() int8 {
	return this.writer.Type()
}

func (this *IWriteTypePointer) SetWriter(id any, value IWriterType) {
}

func (this *IWriteTypePointer) GetWriter(rt reflect.Type) (IWriterType, error) {
	writer, err := GetWriter(rt.Elem())
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return &IWriteTypePointer{writer}, nil
}

func (this *IWriteTypePointer) RemoveWriter(id any) {
}

type IWriteTypeGeneric struct {
}

func (this *IWriteTypeGeneric) Write(protocol thrift.TProtocol, obj any) error {
	return nil
}

func (this *IWriteTypeGeneric) WriteType(protocol thrift.TProtocol) error {
	return nil
}

func (this *IWriteTypeGeneric) Type() int8 {
	return 0
}

func (this *IWriteTypeGeneric) SetWriter(id any, value IWriterType) {
}

func (this *IWriteTypeGeneric) GetWriter(rt reflect.Type) (IWriterType, error) {
	any_name := rt.String()
	gen_name := any_name[0:strings.IndexByte(any_name, '[')]
	if writer := writers[gen_name]; writer != nil {
		return writer.GetWriter(rt)
	}
	return nil, ierror.RaiseMsg(fmt.Sprintf("IWriterType not implemented for %s", rt.String()))
}

func (this *IWriteTypeGeneric) RemoveWriter(id any) {
}

func init() {
	SetWriter[any](NewIWriteType(I_VOID, func(protocol thrift.TProtocol, obj any) error {
		return ierror.RaiseMsg("interface{} cannot be written")
	}))
	SetWriter[bool](NewIWriteType(I_BOOL, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteBool(ctx, obj.(bool))
	}))
	SetWriter[int8](NewIWriteType(I_I08, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteByte(ctx, obj.(int8))
	}))
	SetWriter[uint8](NewIWriteType(I_I16, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI16(ctx, int16(obj.(uint8)))
	}))
	SetWriter[int16](NewIWriteType(I_I16, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI16(ctx, obj.(int16))
	}))
	SetWriter[uint16](NewIWriteType(I_I32, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI32(ctx, int32(obj.(uint16)))
	}))
	SetWriter[int32](NewIWriteType(I_I32, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI32(ctx, obj.(int32))
	}))
	SetWriter[uint32](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI64(ctx, int64(obj.(uint32)))
	}))
	SetWriter[int64](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI64(ctx, obj.(int64))
	}))
	SetWriter[uint64](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI64(ctx, int64(obj.(uint64)))
	}))
	if reflect.TypeOf(int(0)).Size() == 4 {
		SetWriter[int](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI32(ctx, int32(obj.(int)))
		}))
		SetWriter[uint](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI64(ctx, int64(obj.(uint)))
		}))
	} else {
		SetWriter[int](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI64(ctx, int64(obj.(int)))
		}))
		SetWriter[uint](NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI64(ctx, int64(obj.(uint)))
		}))
	}
	SetWriter[float32](NewIWriteType(I_DOUBLE, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteDouble(ctx, float64(obj.(float32)))
	}))
	SetWriter[float64](NewIWriteType(I_DOUBLE, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteDouble(ctx, obj.(float64))
	}))
	SetWriter[string](NewIWriteType(I_STRING, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteString(ctx, obj.(string))
	}))
	SetKindWriter(reflect.Array, NewIWriteType(I_LIST, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())
		WriteSizeAux(protocol, sz)
		tp_elem := rt.Elem()
		if sz > 0 && tp_elem.Kind() == reflect.Interface {
			tp_elem = value.Index(0).Elem().Type()
		}
		writer, err := GetWriter(tp_elem)
		if err != nil {
			return ierror.Raise(err)
		}
		err = writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		for i := 0; i < int(sz); i++ {
			err = writer.Write(protocol, value.Index(i).Interface())
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetKindWriter(reflect.Slice, gen_writers[reflect.Array])
	gen_writers[reflect.Array].SetWriter(I_PAIR_LIST, NewIWriteType(I_LIST, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type().Elem()
		sz := int64(value.Len())
		WriteSizeAux(protocol, sz)

		first_rt := rt.Field(0).Type
		second_rt := rt.Field(1).Type
		if sz > 0 {
			if first_rt.Kind() == reflect.Interface {
				first_rt = value.Index(0).Field(0).Elem().Type()
			}
			if second_rt.Kind() == reflect.Interface {
				second_rt = value.Index(0).Field(1).Elem().Type()
			}
		}

		first_writer, err := GetWriter(first_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		second_writer, err := GetWriter(second_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		first_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		second_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		for i := 0; i < int(sz); i++ {
			err = first_writer.Write(protocol, value.Index(i).Field(0).Interface())
			if err != nil {
				return ierror.Raise(err)
			}
			err = second_writer.Write(protocol, value.Index(i).Field(1).Interface())
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))

	SetKindWriter(reflect.Map, NewIWriteType(I_MAP, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())
		WriteSizeAux(protocol, sz)

		key_rt := rt.Key()
		value_rt := rt.Elem()

		if sz > 0 {
			if key_rt.Kind() == reflect.Interface {
				it := value.MapRange()
				it.Next()
				key_rt = it.Key().Elem().Type()
			}
			if value_rt.Kind() == reflect.Interface {
				it := value.MapRange()
				it.Next()
				value_rt = it.Key().Elem().Type()
			}
		}

		key_writer, err := GetWriter(key_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		value_writer, err := GetWriter(value_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		key_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		value_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}

		it := value.MapRange()
		for it.Next() {
			err = key_writer.Write(protocol, it.Key().Interface())
			if err != nil {
				return ierror.Raise(err)
			}
			err = value_writer.Write(protocol, it.Value().Interface())
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	gen_writers[reflect.Map].SetWriter(I_SET, NewIWriteType(I_LIST, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())
		WriteSizeAux(protocol, sz)

		key_rt := rt.Key()
		if sz > 0 && key_rt.Kind() == reflect.Interface {
			it := value.MapRange()
			it.Next()
			key_rt = it.Key().Elem().Type()
		}
		key_writer, err := GetWriter(key_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		key_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		it := value.MapRange()
		for it.Next() {
			err = key_writer.Write(protocol, it.Key().Interface())
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	gen_writers[reflect.Pointer] = &IWriteTypePointer{nil}
	SetGenericWriter[ipair.IPair[any, any]](NewIWriteType(I_PAIR, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()

		first_rt := rt.Field(0).Type
		second_rt := rt.Field(1).Type
		if first_rt.Kind() == reflect.Interface {
			first_rt = value.Field(0).Elem().Type()
		}
		if second_rt.Kind() == reflect.Interface {
			second_rt = value.Field(1).Elem().Type()
		}
		first_writer, err := GetWriter(first_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		second_writer, err := GetWriter(second_rt)
		if err != nil {
			return ierror.Raise(err)
		}
		first_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		second_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}

		err = first_writer.Write(protocol, value.Field(0).Interface())
		if err != nil {
			return ierror.Raise(err)
		}
		err = second_writer.Write(protocol, value.Field(1).Interface())
		if err != nil {
			return ierror.Raise(err)
		}

		return nil
	}))

}
