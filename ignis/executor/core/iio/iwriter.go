package iio

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/utils"
	"reflect"
	"strings"
)

func WriteTypeAux(protocol thrift.TProtocol, tp int8) error {
	return protocol.WriteByte(ctx, tp)
}

func WriteSizeAux(protocol thrift.TProtocol, sz int64) error {
	return protocol.WriteI64(ctx, sz)
}

type IWriter interface {
	Write(protocol thrift.TProtocol, obj any) error
	WriteType(protocol thrift.TProtocol) error
	Type() int8
	SetWriter(id string, value IWriter)
	GetWriter(id string) IWriter
}

type IWriterF func(protocol thrift.TProtocol, obj any) error

type IWriterType struct {
	tp      int8
	write   IWriterF
	writers map[string]IWriter
	ptr     IWriter
	array   IWriter
}

func (this *IWriterType) Write(protocol thrift.TProtocol, obj any) error {
	return this.write(protocol, obj)
}

func (this *IWriterType) WriteType(protocol thrift.TProtocol) error {
	return WriteTypeAux(protocol, this.tp)
}

func (this *IWriterType) Type() int8 {
	return this.tp
}

func (this *IWriterType) SetWriter(id string, value IWriter) {
	if value != nil {
		switch id {
		case "*":
			this.ptr = value
		case "[]":
			this.array = value
		default:
			this.writers[id] = value
		}
	} else {
		delete(this.writers, id)
	}
}

func (this *IWriterType) GetWriter(id string) (writer IWriter) {
	writer = this
	switch id[0] {
	case '*':
		if this.ptr != nil {
			writer = this.ptr
		}
	case '[':
		if this.array != nil {
			writer = this.array
		}
	}
	if len(this.writers) > 0 {
		if w2, present := this.writers[id]; present {
			return w2
		}
		i := strings.IndexByte(id, '[')
		if i > 0 {
			w2, present := this.writers[id[0:i]]
			if present {
				return w2
			}
		}
	}
	return
}

var globalWriter IWriter

func Write(protocol thrift.TProtocol, obj any) error {
	writer, err := GetWriter(GetName(obj))
	if err != nil {
		return ierror.Raise(err)
	}
	if err = writer.WriteType(protocol); err != nil {
		return ierror.Raise(err)
	}
	return writer.Write(protocol, obj)
}

func SetWriter(id string, value IWriter) {
	globalWriter.SetWriter(id, value)
}

func GetWriter(id string) (IWriter, error) {
	if writer := globalWriter.GetWriter(id); writer != globalWriter {
		return writer.GetWriter(id), nil
	}
	return nil, ierror.RaiseMsg(fmt.Sprintf("IWriter not implemented for %s", id))
}

func NewIWriteType(tp int8, f IWriterF) IWriter {
	return &IWriterType{tp: tp, write: f, writers: map[string]IWriter{}}
}

type IWriterPrinterType[T any] struct {
	IWriterType
}

func (this *IWriterPrinterType[T]) Write(protocol thrift.TProtocol, obj any) error {
	return this.write(protocol, *(obj.(*T)))
}

type IArrayWriterType[T any] struct {
	IWriterType
	valWriter IWriterType
	err       error
}

func (this *IArrayWriterType[T]) Write(protocol thrift.TProtocol, obj any) error {
	if this.err != nil {
		return this.err
	}
	array := obj.([]T)
	sz := len(array)
	if err := WriteSizeAux(protocol, int64(sz)); err != nil {
		return ierror.Raise(err)
	}

	err := this.valWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	for _, e := range array {
		err = this.valWriter.Write(protocol, e)
		if err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

type IMapWriterType[K comparable, V any] struct {
	IWriterType
	keyWriter IWriterType
	valWriter IWriterType
	err       error
}

func (this *IMapWriterType[K, V]) Write(protocol thrift.TProtocol, obj any) error {
	if this.err != nil {
		return this.err
	}
	m := obj.(map[K]V)
	sz := len(m)
	if err := WriteSizeAux(protocol, int64(sz)); err != nil {
		return ierror.Raise(err)
	}

	err := this.valWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	err = this.valWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	for k, v := range m {
		err = this.valWriter.Write(protocol, k)
		if err != nil {
			return ierror.Raise(err)
		}
		err = this.valWriter.Write(protocol, v)
		if err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

type IPairArrayWriterType[T any] struct {
	IWriterType
	firstWriter  IWriterType
	secondWriter IWriterType
	err          error
}

func (this *IPairArrayWriterType[T]) Write(protocol thrift.TProtocol, obj any) error {
	if this.err != nil {
		return this.err
	}
	array := obj.([]T)
	sz := len(array)
	if err := WriteSizeAux(protocol, int64(sz)); err != nil {
		return ierror.Raise(err)
	}

	err := this.firstWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	err = this.secondWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	for _, e := range array {
		var pa any = &e
		pp := pa.(ipair.IAbstractPair)
		if err = this.firstWriter.Write(protocol, pp.GetFirst()); err != nil {
			return ierror.Raise(err)
		}
		if err = this.secondWriter.Write(protocol, pp.GetSecond()); err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

type IAbsPointerWriterType struct {
	IWriterType
}

func (this *IAbsPointerWriterType) GetWriter(id string) IWriter {
	valWriter, err := GetWriter(id[1:])

	return NewIWriteType(utils.Ternary(err != nil, valWriter.Type(), I_VOID), func(protocol thrift.TProtocol, obj any) error {
		if err != nil {
			return ierror.Raise(err)
		}
		elem := reflect.ValueOf(obj).Elem()
		return valWriter.Write(protocol, elem.Interface())
	})
}

type IPointerWriterType[T any] struct {
	IWriterType
	err error
}

func (this *IPointerWriterType[T]) Write(protocol thrift.TProtocol, obj any) error {
	if this.err != nil {
		return this.err
	}
	return this.write(protocol, *(obj.(*T)))
}

func init() {
	globalWriter = NewIWriteType(I_VOID, nil)
	SetWriter(TypeName[any](), NewIWriteType(I_VOID, func(protocol thrift.TProtocol, obj any) error {
		return ierror.RaiseMsg("interface{} cannot be written")
	}))
	SetWriter(TypeName[bool](), NewIWriteType(I_BOOL, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteBool(ctx, obj.(bool))
	}))
	SetWriter(TypeName[int8](), NewIWriteType(I_I08, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteByte(ctx, obj.(int8))
	}))
	SetWriter(TypeName[uint8](), NewIWriteType(I_I16, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI16(ctx, int16(obj.(uint8)))
	}))
	SetWriter(TypeName[int16](), NewIWriteType(I_I16, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI16(ctx, obj.(int16))
	}))
	SetWriter(TypeName[uint16](), NewIWriteType(I_I32, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI32(ctx, int32(obj.(uint16)))
	}))
	SetWriter(TypeName[int32](), NewIWriteType(I_I32, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI32(ctx, obj.(int32))
	}))
	SetWriter(TypeName[uint32](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI64(ctx, int64(obj.(uint32)))
	}))
	SetWriter(TypeName[int64](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI64(ctx, obj.(int64))
	}))
	SetWriter(TypeName[uint64](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteI64(ctx, int64(obj.(uint64)))
	}))
	if reflect.TypeOf(int(0)).Size() == 4 {
		SetWriter(TypeName[int](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI32(ctx, int32(obj.(int)))
		}))
		SetWriter(TypeName[uint](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI64(ctx, int64(obj.(uint)))
		}))
	} else {
		SetWriter(TypeName[int](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI64(ctx, int64(obj.(int)))
		}))
		SetWriter(TypeName[uint](), NewIWriteType(I_I64, func(protocol thrift.TProtocol, obj any) error {
			return protocol.WriteI64(ctx, int64(obj.(uint)))
		}))
	}
	SetWriter(TypeName[float32](), NewIWriteType(I_DOUBLE, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteDouble(ctx, float64(obj.(float32)))
	}))
	SetWriter(TypeName[float64](), NewIWriteType(I_DOUBLE, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteDouble(ctx, obj.(float64))
	}))
	SetWriter(TypeName[string](), NewIWriteType(I_STRING, func(protocol thrift.TProtocol, obj any) error {
		return protocol.WriteString(ctx, obj.(string))
	}))
	SetWriter(TypeGenericName[*any](), &IAbsPointerWriterType{})
	array := NewIWriteType(I_LIST, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())
		if err := WriteSizeAux(protocol, int64(sz)); err != nil {
			return ierror.Raise(err)
		}
		tpElem := rt.Elem()
		if sz > 0 && tpElem.Kind() == reflect.Interface {
			tpElem = value.Index(0).Elem().Type()
		}
		writer, err := GetWriter(tpElem.String())
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
	})
	SetWriter(TypeGenericName[[]any](), array)
	array.SetWriter(TypeGenericName[ipair.IPair[any, any]](), NewIWriteType(I_PAIR_LIST, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type().Elem()
		sz := int64(value.Len())
		if err := WriteSizeAux(protocol, int64(sz)); err != nil {
			return ierror.Raise(err)
		}

		firstRt := rt.Field(0).Type
		secondRt := rt.Field(1).Type
		if sz > 0 {
			if firstRt.Kind() == reflect.Interface {
				firstRt = value.Index(0).Field(0).Elem().Type()
			}
			if secondRt.Kind() == reflect.Interface {
				secondRt = value.Index(0).Field(1).Elem().Type()
			}
		}

		firstWriter, err := GetWriter(firstRt.String())
		if err != nil {
			return ierror.Raise(err)
		}
		secondWriter, err := GetWriter(secondRt.String())
		if err != nil {
			return ierror.Raise(err)
		}
		err = firstWriter.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		err = secondWriter.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		for i := 0; i < int(sz); i++ {
			err = firstWriter.Write(protocol, value.Index(i).Field(0).Interface())
			if err != nil {
				return ierror.Raise(err)
			}
			err = secondWriter.Write(protocol, value.Index(i).Field(1).Interface())
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetWriter(TypeName[[]byte](), NewIWriteType(I_BINARY, func(protocol thrift.TProtocol, obj any) error {
		array := obj.([]byte)
		sz := len(array)
		if err := WriteSizeAux(protocol, int64(sz)); err != nil {
			return ierror.Raise(err)
		}

		for _, e := range array {
			if err := protocol.WriteByte(ctx, int8(e)); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetWriter(TypeGenericName[map[any]any](), NewIWriteType(I_MAP, func(protocol thrift.TProtocol, obj any) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())
		if err := WriteSizeAux(protocol, int64(sz)); err != nil {
			return ierror.Raise(err)
		}

		keyRt := rt.Key()
		valueRt := rt.Elem()

		if sz > 0 {
			if keyRt.Kind() == reflect.Interface {
				it := value.MapRange()
				it.Next()
				keyRt = it.Key().Elem().Type()
			}
			if valueRt.Kind() == reflect.Interface {
				it := value.MapRange()
				it.Next()
				valueRt = it.Key().Elem().Type()
			}
		}

		key_writer, err := GetWriter(keyRt.String())
		if err != nil {
			return ierror.Raise(err)
		}
		value_writer, err := GetWriter(valueRt.String())
		if err != nil {
			return ierror.Raise(err)
		}
		err = key_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		err = value_writer.WriteType(protocol)
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
	SetWriter(TypeGenericName[ipair.IPair[any, any]](), NewIWriteType(I_PAIR, func(protocol thrift.TProtocol, obj any) error {
		p := obj.(ipair.IAbstractPair)

		first_writer, err := GetWriter(GetName(p.GetFirst()))
		if err != nil {
			return ierror.Raise(err)
		}
		second_writer, err := GetWriter(GetName(p.GetSecond()))
		if err != nil {
			return ierror.Raise(err)
		}
		err = first_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		err = second_writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}

		err = first_writer.Write(protocol, p.GetFirst())
		if err != nil {
			return ierror.Raise(err)
		}
		err = second_writer.Write(protocol, p.GetSecond())
		if err != nil {
			return ierror.Raise(err)
		}

		return nil
	}))

}
