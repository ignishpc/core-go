package iio

import (
	"context"
	"encoding/gob"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"ignis/executor/core/utils"
	"reflect"
	"unsafe"
)

var native_arrays = make(map[string]func(n int64) any)
var native_writers = make(map[string]func() IWriter)
var native_readers = make(map[string]func() IReader)

func init() {
	nameAny := NameFix(utils.TypeName[any]())
	native_arrays[nameAny] = func(n int64) any {
		return make([]any, n)
	}
	native_writers[nameAny] = func() IWriter {
		return &IAnyNativeWriter{}
	}
	native_readers[nameAny] = func() IReader {
		return &IAnyNativeReader{}
	}
}

func GetNativeReader(name string) (IReader, error) {
	name = NameFix(name)
	if reader, ok := native_readers[name]; ok {
		return reader(), nil
	} else {
		return nil, ierror.RaiseMsg("Native serialization requires that the type has been previously used in the executor.")
	}
}

func GetNativeReaderTp[T any]() IReader {
	return native_readers[newNativeType[T]()]()
}

func GetNativeWriter(name string) (IWriter, error) {
	name = NameFix(name)
	if writer, ok := native_writers[name]; ok {
		return writer(), nil
	} else {
		return &INativeWriterReflect{nil, nil}, nil
	}
}

func GetNativeWriterTp[T any]() IWriter {
	return native_writers[newNativeType[T]()]()
}

func newNativeType[T any]() string {
	name := NameFix(utils.TypeName[T]())
	if name == utils.TypeName[any]() {
		return name
	}
	f := func(decoder *gob.Decoder) (any, error) {
		v := new(T)
		return *v, decoder.Decode(v)
	}
	native_arrays[name] = func(n int64) any {
		return make([]T, n)
	}
	native_writers[name] = func() IWriter {
		return &INativeWriter[T]{nil, nil}
	}
	native_readers[name] = func() IReader {
		return &INativeReader{f, nil, nil}
	}
	return name
}

type INativeWriter[T any] struct {
	encoder  *gob.Encoder
	protocol thrift.TProtocol
}

func (this *INativeWriter[T]) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	if this.protocol != protocol {
		this.protocol = protocol
		this.encoder = gob.NewEncoder(protocol.Transport())
	}
	return ierror.Raise(this.encoder.Encode((*T)(obj)))
}

func (this *INativeWriter[T]) WriteType(protocol thrift.TProtocol) error {
	return nil
}

func (this *INativeWriter[T]) Type() int8 {
	return I_VOID
}

type IAnyNativeWriter struct {
	encoder  *gob.Encoder
	protocol thrift.TProtocol
}

func (this *IAnyNativeWriter) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	if this.protocol != protocol {
		this.protocol = protocol
		this.encoder = gob.NewEncoder(protocol.Transport())
	}
	val := reflect.NewAt(rtp, obj).Elem().Elem()
	if err := this.encoder.Encode(val.Type().String()); err != nil {
		return ierror.Raise(err)
	}
	return ierror.Raise(this.encoder.EncodeValue(val))
}

func (this *IAnyNativeWriter) WriteType(protocol thrift.TProtocol) error {
	return nil
}

func (this *IAnyNativeWriter) Type() int8 {
	return I_VOID
}

type INativeWriterReflect struct {
	encoder  *gob.Encoder
	protocol thrift.TProtocol
}

func (this *INativeWriterReflect) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	if this.protocol != protocol {
		this.protocol = protocol
		this.encoder = gob.NewEncoder(protocol.Transport())
	}
	return ierror.Raise(this.encoder.EncodeValue(reflect.NewAt(rtp, obj)))
}

func (this *INativeWriterReflect) WriteType(protocol thrift.TProtocol) error {
	return nil
}

func (this *INativeWriterReflect) Type() int8 {
	return I_VOID
}

type INativeReader struct {
	f        func(decoder *gob.Decoder) (any, error)
	decoder  *gob.Decoder
	protocol thrift.TProtocol
}

func (this *INativeReader) Read(protocol thrift.TProtocol) (any, error) {
	if this.protocol != protocol {
		this.protocol = protocol
		this.decoder = gob.NewDecoder(protocol.Transport())
	}
	return this.f(this.decoder)
}

func (this *INativeReader) Empty() any {
	return nil
}

type IAnyNativeReader struct {
	decoder *gob.Decoder
	reader  *INativeReader
	tp      string
}

func (this *IAnyNativeReader) Read(protocol thrift.TProtocol) (any, error) {
	var tp string
	if this.decoder == nil {
		this.decoder = gob.NewDecoder(protocol.Transport())
		if err := this.decoder.Decode(&tp); err != nil {
			return nil, ierror.Raise(err)
		}
		reader, err := GetNativeReader(tp)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		this.reader = reader.(*INativeReader)
		this.reader.decoder = this.decoder
		this.reader.protocol = protocol
		this.tp = tp
	} else {
		if err := this.decoder.Decode(&tp); err != nil {
			return nil, ierror.Raise(err)
		}
		if this.tp != tp {
			reader, err := GetNativeReader(tp)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			this.reader.f = reader.(*INativeReader).f
			this.tp = tp
		}
	}
	return this.reader.Read(protocol)
}

func (this *IAnyNativeReader) Empty() any {
	return nil
}

func WriteNative(protocol thrift.TProtocol, obj any) error {
	val := reflect.ValueOf(obj)
	k := reflect.TypeOf(obj).Kind()
	isArray := k == reflect.Array || k == reflect.Slice
	if err := protocol.WriteBool(context.Background(), isArray); err != nil {
		return ierror.Raise(err)
	}
	if isArray {
		n := int64(val.Len())
		var tp reflect.Type
		if n == 0 {
			tp = val.Type().Elem()
		} else {
			tp = val.Index(0).Type()
		}
		id := tp.String()

		if err := protocol.WriteString(context.Background(), id); err != nil {
			return ierror.Raise(err)
		}
		if err := WriteSizeAux(protocol, n); err != nil {
			return ierror.Raise(err)
		}
		if n == 0 {
			return nil
		}
		writer, err := GetNativeWriter(id)
		if err != nil {
			return ierror.Raise(err)
		}
		for i := int64(0); i < n; i++ {
			if err := writer.Write(protocol, tp, val.Index(int(i)).Addr().UnsafePointer()); err != nil {
				return ierror.Raise(err)
			}
		}
	} else {
		tp := val.Type()
		id := tp.String()

		if err := protocol.WriteString(context.Background(), id); err != nil {
			return ierror.Raise(err)
		}
		writer, err := GetNativeWriter(val.String())
		if err != nil {
			return ierror.Raise(err)
		}
		if err := writer.Write(protocol, tp, val.Addr().UnsafePointer()); err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

func ReadNative(protocol thrift.TProtocol) (any, error) {
	isArray, err := protocol.ReadBool(context.Background())
	if err != nil {
		return nil, ierror.Raise(err)
	}
	name, err := protocol.ReadString(context.Background())
	if err != nil {
		return nil, ierror.Raise(err)
	}
	reader, err := GetNativeReader(name)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	if isArray {
		n, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		array := native_arrays[name](n)
		rarray := reflect.ValueOf(array)
		for i := int64(0); i < n; i++ {
			elem, err := reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			rarray.Index(int(i)).Set(reflect.ValueOf(elem))
		}
		return array, nil
	} else {
		return reader.Read(protocol)
	}
}
