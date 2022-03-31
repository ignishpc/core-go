package iio

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/utils"
	"reflect"
	"strings"
	"unsafe"
)

func WriteTypeAux(protocol thrift.TProtocol, tp int8) error {
	return protocol.WriteByte(ctx, tp)
}

func WriteSizeAux(protocol thrift.TProtocol, sz int64) error {
	return protocol.WriteI64(ctx, sz)
}

type IWriter interface {
	Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error
	WriteType(protocol thrift.TProtocol) error
	Type() int8
}

type IWriterF func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error

var writers = make(map[string]IWriter)

func Write[T any](protocol thrift.TProtocol, obj T) error {
	writer, tp, err := GetWriterObj(obj)
	if err != nil {
		return ierror.Raise(err)
	}
	if err = writer.WriteType(protocol); err != nil {
		return ierror.Raise(err)
	}
	return writer.Write(protocol, tp, unsafe.Pointer(&obj))
}

func SetWriter(id string, value IWriter) {
	id = NameFix(id)
	if value != nil {
		writers[id] = value
	} else {
		delete(writers, id)
	}
}

func GetWriter(id string) IWriter {
	id = NameFix(id)
	if writer, present := writers[id]; present {
		return writer
	}
	return nil
}

func getWriterAux(id string) (IWriter, error) {
	id = NameFix(id)
	if writer, present := writers[id]; present {
		return writer, nil
	}
	name := id
	for true {
		i := strings.LastIndexByte(name, '[')
		if i < 0 {
			break
		}
		name = name[:i]
		writer, present := writers[name]
		if present {
			return writer, nil
		}
	}
	return nil, ierror.RaiseMsg(fmt.Sprintf("IWriter not implemented for %s", id))
}

var anyType = utils.TypeObj[any]()

func GetWriterObj[T any](obj T) (IWriter, reflect.Type, error) {
	tp := reflect.TypeOf(&obj).Elem()
	if tp.Kind() == reflect.Pointer {
		tpe := tp.Elem()
		if tpe.Kind() == reflect.Pointer || tpe == anyType { // Pointer to pointer or pointer to any
			writer, atp, aerr := GetWriterObj(reflect.ValueOf(obj).Elem().Interface())
			if tpe == anyType {
				return &IPointerWriterType{atp, writer, aerr}, tp, nil
			}
			aWriter := writer.(*IAnyWriterType)
			return &IPointerWriterType{aWriter.rtp, aWriter.valWriter, aWriter.err}, tp, nil
		}
		writer, err := getWriterAux(tpe.String())
		return &IAnyWriterType{tpe, writer, err}, tp, nil

	} else if tp == anyType {
		realTp := reflect.TypeOf(obj)
		if realTp == nil {
			//Collection created with any
		} else if realTp.Kind() == reflect.Pointer { //any is a pointer
			val := reflect.ValueOf(obj).Elem()
			ival := val.Interface()
			if ival != nil { //any stores something
				writer, atp, aerr := GetWriterObj(ival)
				aWriter := writer.(*IAnyWriterType)
				var pWriter IWriter
				if val.Kind() != reflect.Interface { //any stores a pointer to any
					pWriter = &IPointerWriterType{aWriter.rtp, aWriter.valWriter, aWriter.err}
				} else {
					pWriter = &IPointerWriterType{atp, aWriter, aerr}
				}
				return &IAnyWriterType{realTp, pWriter, aerr}, tp, nil
			}
		} else {
			writer, err := getWriterAux(realTp.String())
			return &IAnyWriterType{realTp, writer, err}, tp, nil
		}
	}
	writer, err := getWriterAux(tp.String())
	return writer, tp, err
}

type IWriterType struct {
	tp    int8
	write IWriterF
}

func (this *IWriterType) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	return this.write(protocol, rtp, obj)
}

func (this *IWriterType) WriteType(protocol thrift.TProtocol) error {
	return WriteTypeAux(protocol, this.tp)
}

func (this *IWriterType) Type() int8 {
	return this.tp
}

type IAnyWriterType struct {
	rtp       reflect.Type
	valWriter IWriter
	err       error
}

func (this *IAnyWriterType) Write(protocol thrift.TProtocol, tp reflect.Type, obj unsafe.Pointer) error {
	return this.valWriter.Write(protocol, this.rtp, utils.GetAnyData((*any)(obj)))
}

func (this *IAnyWriterType) WriteType(protocol thrift.TProtocol) error {
	if this.err != nil {
		return this.err
	}
	return WriteTypeAux(protocol, this.valWriter.Type())
}

func (this *IAnyWriterType) Type() int8 {
	if this.err != nil {
		return I_VOID
	}
	return this.valWriter.Type()
}

type IPointerWriterType struct {
	rtp       reflect.Type
	valWriter IWriter
	err       error
}

func (this *IPointerWriterType) Write(protocol thrift.TProtocol, tp reflect.Type, obj unsafe.Pointer) error {
	return this.valWriter.Write(protocol, this.rtp, unsafe.Pointer(*(**any)(obj)))
}

func (this *IPointerWriterType) WriteType(protocol thrift.TProtocol) error {
	if this.err != nil {
		return this.err
	}
	return WriteTypeAux(protocol, this.valWriter.Type())
}

func (this *IPointerWriterType) Type() int8 {
	if this.err != nil {
		return I_VOID
	}
	return this.valWriter.Type()
}

func NewIWrite(tp int8, f IWriterF) IWriter {
	return &IWriterType{tp: tp, write: f}
}

type IArrayWriterType[T any] struct {
	IWriterType
	rtp       reflect.Type
	valWriter IWriter
	err       error
}

func (this *IArrayWriterType[T]) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	if this.err != nil {
		return this.err
	}
	array := (*[]T)(obj)
	sz := len(*array)
	if err := WriteSizeAux(protocol, int64(sz)); err != nil {
		return ierror.Raise(err)
	}

	err := this.valWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	for _, e := range *array {
		err = this.valWriter.Write(protocol, this.rtp, unsafe.Pointer(&e))
		if err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

type IMapWriterType[K comparable, V any] struct {
	IWriterType
	keyWriter IWriter
	keyRtp    reflect.Type
	valWriter IWriter
	valRtp    reflect.Type
	err       error
}

func (this *IMapWriterType[K, V]) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	if this.err != nil {
		return this.err
	}
	m := (*map[K]V)(obj)
	sz := len(*m)
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

	for k, v := range *m {
		err = this.valWriter.Write(protocol, this.keyRtp, unsafe.Pointer(&k))
		if err != nil {
			return ierror.Raise(err)
		}
		err = this.valWriter.Write(protocol, this.valRtp, unsafe.Pointer(&v))
		if err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

type IPairWriterType[T any] struct {
	IWriterType
	firstWriter  IWriter
	firstRpt     reflect.Type
	secondWriter IWriter
	secondRpt    reflect.Type
	err          error
}

func (this *IPairWriterType[T]) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	p := any((*T)(obj)).(ipair.IAbstractPair)

	err := this.firstWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}
	err = this.secondWriter.WriteType(protocol)
	if err != nil {
		return ierror.Raise(err)
	}

	err = this.firstWriter.Write(protocol, this.firstRpt, p.GetFirstPointer())
	if err != nil {
		return ierror.Raise(err)
	}
	err = this.secondWriter.Write(protocol, this.secondRpt, p.GetSecondPointer())
	if err != nil {
		return ierror.Raise(err)
	}

	return nil
}

type IPairArrayWriterType[T any] struct {
	IWriterType
	firstWriter  IWriter
	firstRpt     reflect.Type
	secondWriter IWriter
	secondRpt    reflect.Type
	err          error
}

func (this *IPairArrayWriterType[T]) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	if this.err != nil {
		return this.err
	}
	array := (*[]T)(obj)
	sz := int64(len(*array))
	if err := WriteSizeAux(protocol, sz); err != nil {
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

	for _, e := range *array {
		pp := any(&e).(ipair.IAbstractPair)
		if err = this.firstWriter.Write(protocol, this.firstRpt, pp.GetFirstPointer()); err != nil {
			return ierror.Raise(err)
		}
		if err = this.secondWriter.Write(protocol, this.secondRpt, pp.GetSecondPointer()); err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

func init() {
	SetWriter(utils.TypeName[any](), NewIWrite(I_VOID, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return nil
	}))
	SetWriter(utils.TypeName[bool](), NewIWrite(I_BOOL, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteBool(ctx, *(*bool)(obj))
	}))
	SetWriter(utils.TypeName[int8](), NewIWrite(I_I08, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteByte(ctx, *(*int8)(obj))
	}))
	SetWriter(utils.TypeName[uint8](), NewIWrite(I_I16, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteByte(ctx, *(*int8)(obj))
	}))
	SetWriter(utils.TypeName[int16](), NewIWrite(I_I16, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteI16(ctx, *(*int16)(obj))
	}))
	SetWriter(utils.TypeName[uint16](), NewIWrite(I_I32, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteI16(ctx, *(*int16)(obj))
	}))
	SetWriter(utils.TypeName[int32](), NewIWrite(I_I32, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteI32(ctx, *(*int32)(obj))
	}))
	SetWriter(utils.TypeName[uint32](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteI32(ctx, *(*int32)(obj))
	}))
	SetWriter(utils.TypeName[int64](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteI64(ctx, *(*int64)(obj))
	}))
	SetWriter(utils.TypeName[uint64](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteI64(ctx, *(*int64)(obj))
	}))
	if reflect.TypeOf(int(0)).Size() == 4 {
		SetWriter(utils.TypeName[int](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
			return protocol.WriteI32(ctx, *(*int32)(obj))
		}))
		SetWriter(utils.TypeName[uint](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
			return protocol.WriteI32(ctx, *(*int32)(obj))
		}))
	} else {
		SetWriter(utils.TypeName[int](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
			return protocol.WriteI64(ctx, *(*int64)(obj))
		}))
		SetWriter(utils.TypeName[uint](), NewIWrite(I_I64, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
			return protocol.WriteI64(ctx, *(*int64)(obj))
		}))
	}
	SetWriter(utils.TypeName[float32](), NewIWrite(I_DOUBLE, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteDouble(ctx, float64(*(*float32)(obj)))
	}))
	SetWriter(utils.TypeName[float64](), NewIWrite(I_DOUBLE, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteDouble(ctx, *(*float64)(obj))
	}))
	SetWriter(utils.TypeName[string](), NewIWrite(I_STRING, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		return protocol.WriteString(ctx, *(*string)(obj))
	}))
	SetWriter(TypeGenericName[[]any](), NewIWrite(I_LIST, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		value := reflect.NewAt(rtp, obj).Elem()
		rt := value.Type()
		sz := int64(value.Len())
		if err := WriteSizeAux(protocol, sz); err != nil {
			return ierror.Raise(err)
		}
		var writer IWriter
		var vtp reflect.Type
		var err error
		if sz == 0 {
			writer, vtp, err = GetWriterObj(reflect.New(rt.Elem()).Elem().Interface())
		} else {
			writer, vtp, err = GetWriterObj(value.Index(0).Interface())
		}

		if err != nil {
			return ierror.Raise(err)
		}
		err = writer.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		for i := 0; i < int(sz); i++ {
			e := value.Index(i).Interface()
			err = writer.Write(protocol, vtp, unsafe.Pointer(&e))
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetWriter(TypeGenericName[[]ipair.IPair[any, any]](), NewIWrite(I_PAIR_LIST, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		value := reflect.NewAt(rtp, obj).Elem()
		rt := value.Type().Elem()
		sz := int64(value.Len())
		if err := WriteSizeAux(protocol, sz); err != nil {
			return ierror.Raise(err)
		}

		var firstWriter IWriter
		var ftp reflect.Type
		var err error
		var secondWriter IWriter
		var stp reflect.Type
		var err2 error

		if sz == 0 {
			firstWriter, ftp, err = GetWriterObj(reflect.New(rt.Field(0).Type).Elem().Interface())
			secondWriter, stp, err2 = GetWriterObj(reflect.New(rt.Field(1).Type).Elem().Interface())
		} else {
			firstWriter, ftp, err = GetWriterObj(value.Index(0).Field(0).Interface())
			secondWriter, stp, err2 = GetWriterObj(value.Index(0).Field(1).Interface())
		}
		if err != nil {
			return ierror.Raise(err)
		}
		if err2 != nil {
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
			e := value.Index(i).Field(0).Interface()
			err = firstWriter.Write(protocol, ftp, unsafe.Pointer(&e))
			if err != nil {
				return ierror.Raise(err)
			}
			e = value.Index(i).Field(1).Interface()
			err = secondWriter.Write(protocol, stp, unsafe.Pointer(&e))
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetWriter(utils.TypeName[[]byte](), NewIWrite(I_BINARY, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		array := (*[]byte)(obj)
		sz := int64(len(*array))
		if err := WriteSizeAux(protocol, sz); err != nil {
			return ierror.Raise(err)
		}

		for _, e := range *array {
			if err := protocol.WriteByte(ctx, int8(e)); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetWriter(TypeGenericName[map[any]any](), NewIWrite(I_MAP, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		value := reflect.NewAt(rtp, obj).Elem()
		rt := value.Type()
		sz := int64(value.Len())
		if err := WriteSizeAux(protocol, sz); err != nil {
			return ierror.Raise(err)
		}

		var keyWriter IWriter
		var ktp reflect.Type
		var err error
		var valueWriter IWriter
		var vtp reflect.Type
		var err2 error

		if sz == 0 {
			keyWriter, ktp, err = GetWriterObj(reflect.New(rt.Key()).Elem().Interface())
			valueWriter, vtp, err2 = GetWriterObj(reflect.New(rt.Elem()).Elem().Interface())
		} else {
			it := value.MapRange()
			it.Next()
			keyWriter, ktp, err = GetWriterObj(it.Key().Interface())
			valueWriter, vtp, err2 = GetWriterObj(it.Value().Interface())
		}

		if err != nil {
			return ierror.Raise(err)
		}
		if err2 != nil {
			return ierror.Raise(err)
		}
		err = keyWriter.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}
		err = valueWriter.WriteType(protocol)
		if err != nil {
			return ierror.Raise(err)
		}

		it := value.MapRange()
		for it.Next() {
			e := it.Key().Interface()
			err = keyWriter.Write(protocol, ktp, unsafe.Pointer(&e))
			if err != nil {
				return ierror.Raise(err)
			}
			e = it.Value().Interface()
			err = valueWriter.Write(protocol, vtp, unsafe.Pointer(&e))
			if err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetWriter(TypeGenericName[ipair.IPair[any, any]](), NewIWrite(I_PAIR, func(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
		p := reflect.NewAt(rtp, obj).Elem()

		firstWriter, ftp, err := GetWriterObj(p.Field(0).Interface())
		if err != nil {
			return ierror.Raise(err)
		}
		secondWriter, stp, err := GetWriterObj(p.Field(1).Interface())
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

		e := p.Field(0).Interface()
		err = firstWriter.Write(protocol, ftp, unsafe.Pointer(&e))
		if err != nil {
			return ierror.Raise(err)
		}
		e = p.Field(1).Interface()
		err = secondWriter.Write(protocol, stp, unsafe.Pointer(&e))
		if err != nil {
			return ierror.Raise(err)
		}

		return nil
	}))
}
