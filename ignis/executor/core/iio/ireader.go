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

func ReadTypeAux(protocol thrift.TProtocol) (int8, error) {
	return protocol.ReadByte(ctx)
}

func ReadSizeAux(protocol thrift.TProtocol) (int64, error) {
	return protocol.ReadI64(ctx)
}

var pairName = strings.Replace(utils.TypeName[ipair.IPair[any, any]](), utils.TypeName[any](), "%s", -1)

func GetNamePair(first any, second any) string {
	return fmt.Sprintf(pairName, GetName(first), GetName(second))
}

var mapName = strings.Replace(utils.TypeName[map[any]any](), "any", "%s", -1)

func GetNameMap(key any, value any) string {
	return fmt.Sprintf(mapName, GetName(key), GetName(value))
}

func GetName(obj any) string {
	return reflect.TypeOf(obj).String()
}

type IReader interface {
	Read(protocol thrift.TProtocol) (any, error)
	Empty() any
}

type IGenericReader interface {
	Read(protocol thrift.TProtocol, info any) (any, error)
}

type IReaderF func(protocol thrift.TProtocol) (any, error)
type IGenericReaderF func(protocol thrift.TProtocol, info any) (any, error)

var readers = make([]IReader, 256)

func SetReader(key int8, value IReader) {
	readers[key] = value
}

func GetReader(key int8) (IReader, error) {
	reader := readers[key]
	if reader == nil {
		return nil, ierror.RaiseMsg(fmt.Sprintf("IReader not implemented for id %d", key))
	}
	return reader, nil
}

func GetGenericReader(key int8, id string) IGenericReader {
	id = NameFix(id)
	reader, present := readers[key].(*IReaderType).readers[id]
	if !present {
		return readers[key].(*IReaderType).def
	}
	return reader
}

func SetGenericReader(key int8, id string, gr IGenericReader) {
	id = NameFix(id)
	if gr == nil {
		delete(readers[key].(*IReaderType).readers, id)
	} else {
		readers[key].(*IReaderType).readers[id] = gr
	}
}

func Read[T any](protocol thrift.TProtocol) (r T, errr error) {
	id, err := ReadTypeAux(protocol)
	if err != nil {
		errr = ierror.Raise(err)
		return
	}
	reader, err := GetReader(id)
	if err != nil {
		errr = ierror.Raise(err)
		return
	}
	value, errr := reader.Read(protocol)
	if err != nil {
		errr = ierror.Raise(err)
		return
	}
	r = value.(T)
	return
}

type IReaderType struct {
	readers map[string]IGenericReader
	def     IGenericReader
	read    IReaderF
	empty   any
}

func (this *IReaderType) Read(protocol thrift.TProtocol) (any, error) {
	return this.read(protocol)
}

func (this *IReaderType) Empty() any {
	return this.empty
}

func NewIReader(f IReaderF, empty any) IReader {
	return &IReaderType{
		map[string]IGenericReader{},
		nil,
		f,
		empty,
	}
}

type IGenericReaderImpl struct {
	f IGenericReaderF
}

func (this *IGenericReaderImpl) Read(protocol thrift.TProtocol, info any) (any, error) {
	return this.f(protocol, info)
}

func NewIGenericReaderImpl(f IGenericReaderF) IGenericReader {
	return &IGenericReaderImpl{
		f,
	}
}

type IArrayReaderInfo struct {
	sz     int64
	reader IReader
	elem   any
}

type IArrayGenericReader[T any] struct {
}

func (this *IArrayGenericReader[T]) Read(protocol thrift.TProtocol, info any) (any, error) {
	arrayInfo := info.(*IArrayReaderInfo)
	array := make([]T, arrayInfo.sz)
	if arrayInfo.sz > 0 {
		array[0] = arrayInfo.elem.(T)
	}
	for i := int64(1); i < arrayInfo.sz; i++ {
		elem, err := arrayInfo.reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		array[i] = elem.(T)
	}
	return array, nil
}

type IPairArrayReaderInfo struct {
	sz           int64
	firstReader  IReader
	secondReader IReader
	first        any
	second       any
}

type IPairArrayGenericReader[T any] struct {
}

func (this *IPairArrayGenericReader[T]) Read(protocol thrift.TProtocol, info any) (any, error) {
	arrayInfo := info.(*IPairArrayReaderInfo)
	array := make([]T, arrayInfo.sz)
	var pp any
	if arrayInfo.sz > 0 {
		pp = &array[0]
		p := pp.(ipair.IAbstractPair)
		p.SetFirst(arrayInfo.first)
		p.SetSecond(arrayInfo.second)
	}
	for i := int64(1); i < arrayInfo.sz; i++ {
		pp = &array[i]
		first, err := arrayInfo.firstReader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second, err := arrayInfo.secondReader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		p := pp.(ipair.IAbstractPair)
		p.SetFirst(first)
		p.SetSecond(second)
	}
	return array, nil
}

type IMapReaderInfo struct {
	sz        int64
	keyReader IReader
	valReader IReader
	key       any
	val       any
}

type IMapGenericReader[K comparable, V any] struct {
}

func (this *IMapGenericReader[K, V]) Read(protocol thrift.TProtocol, info any) (any, error) {
	mapInfo := info.(*IMapReaderInfo)
	m := make(map[K]V, mapInfo.sz)
	if mapInfo.sz > 0 {
		m[mapInfo.key.(K)] = mapInfo.val.(V)
	}
	for i := int64(1); i < mapInfo.sz; i++ {
		first, err := mapInfo.keyReader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second, err := mapInfo.valReader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		m[first.(K)] = second.(V)
	}
	return m, nil
}

type ISetReaderInfo struct {
	sz     int64
	reader IReader
	elem   any
}

type ISetGenericReader[T comparable] struct {
}

func (this *ISetGenericReader[T]) Read(protocol thrift.TProtocol, info any) (any, error) {
	setInfo := info.(*ISetReaderInfo)
	set := make(map[T]bool, setInfo.sz)
	if setInfo.sz > 0 {
		set[setInfo.elem.(T)] = true
	}
	for i := int64(1); i < setInfo.sz; i++ {
		elem, err := setInfo.reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		set[elem.(T)] = true
	}
	return setInfo, nil
}

type IPairReaderInfo struct {
	first  any
	second any
}

type IPairGenericReader[T any] struct {
}

func (this *IPairGenericReader[T]) Read(protocol thrift.TProtocol, info any) (any, error) {
	pairInfo := info.(*IPairReaderInfo)
	p := new(T)
	var pa any = p
	pp := pa.(ipair.IAbstractPair)
	pp.SetFirst(pairInfo.first)
	pp.SetSecond(pairInfo.second)
	return *p, nil
}

func setReaderR(key int8, value IReader) *IReaderType {
	readers[key] = value
	return value.(*IReaderType)
}

func SetIntAsDefault(flag bool) {
	if reflect.TypeOf(int(0)).Size() == 4 {
		if flag {
			SetReader(I_I32, NewIReader(func(protocol thrift.TProtocol) (any, error) {
				val, err := protocol.ReadI32(ctx)
				return int(val), err
			}, 0))
		} else {
			SetReader(I_I32, NewIReader(func(protocol thrift.TProtocol) (any, error) {
				return protocol.ReadI32(ctx)
			}, 0))
		}
	} else {
		if flag {
			SetReader(I_I64, NewIReader(func(protocol thrift.TProtocol) (any, error) {
				val, err := protocol.ReadI64(ctx)
				return int(val), err
			}, 0))
		} else {
			SetReader(I_I64, NewIReader(func(protocol thrift.TProtocol) (any, error) {
				return protocol.ReadI64(ctx)
			}, 0))
		}
	}
}

func init() {
	SetReader(I_VOID, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return nil, nil
	}, nil))
	SetReader(I_BOOL, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadBool(ctx)
	}, true))
	SetReader(I_I08, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadByte(ctx)
	}, 0))
	SetReader(I_I16, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadI16(ctx)
	}, 0))
	SetReader(I_I32, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadI32(ctx)
	}, 0))
	SetReader(I_I64, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadI64(ctx)
	}, 0))
	SetReader(I_DOUBLE, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadDouble(ctx)
	}, 0))
	SetReader(I_STRING, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadString(ctx)
	}, 0))
	setReaderR(I_LIST, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		var err error
		info := new(IArrayReaderInfo)

		if info.sz, err = ReadSizeAux(protocol); err != nil {
			return nil, ierror.Raise(err)
		}
		id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if info.reader, err = GetReader(id); err != nil {
			return nil, ierror.Raise(err)
		}
		if info.sz == 0 {
			if id == I_VOID {
				return make([]any, 0), nil
			}
			info.elem = info.reader.Empty()
		} else {
			if info.elem, err = info.reader.Read(protocol); err != nil {
				return nil, ierror.Raise(err)
			}
		}

		return GetGenericReader(I_LIST, GetName(info.elem)).Read(protocol, info)
	}, []any{})).def = NewIGenericReaderImpl(func(protocol thrift.TProtocol, info any) (any, error) {
		arrayInfo := info.(*IArrayReaderInfo)
		l := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(arrayInfo.elem)), int(arrayInfo.sz), int(arrayInfo.sz))
		if arrayInfo.sz > 0 {
			l.Index(0).Set(reflect.ValueOf(arrayInfo.elem))
		}
		for i := int64(1); i < arrayInfo.sz; i++ {
			elem, err := arrayInfo.reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			l.Index(int(i)).Set(reflect.ValueOf(elem))
		}

		return l.Interface(), nil
	})
	setReaderR(I_PAIR_LIST, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		var err error
		info := new(IPairArrayReaderInfo)

		if info.sz, err = ReadSizeAux(protocol); err != nil {
			return nil, ierror.Raise(err)
		}
		firstId, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		secondId, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if info.firstReader, err = GetReader(firstId); err != nil {
			return nil, ierror.Raise(err)
		}
		if info.secondReader, err = GetReader(secondId); err != nil {
			return nil, ierror.Raise(err)
		}
		if info.sz == 0 {
			if firstId == I_VOID || secondId == I_VOID {
				return make([]any, 0), nil
			}
			info.first = info.firstReader.Empty()
			info.second = info.secondReader.Empty()
		} else {
			if info.first, err = info.firstReader.Read(protocol); err != nil {
				return nil, ierror.Raise(err)
			}
			if info.second, err = info.secondReader.Read(protocol); err != nil {
				return nil, ierror.Raise(err)
			}
		}

		return GetGenericReader(I_PAIR_LIST, GetNamePair(info.first, info.second)).Read(protocol, info)
	}, []ipair.IPair[any, any]{})).def = NewIGenericReaderImpl(func(protocol thrift.TProtocol, info any) (any, error) {
		arrayInfo := info.(*IPairArrayReaderInfo)

		l := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(ipair.IPair[any, any]{arrayInfo.first, arrayInfo.second})), int(arrayInfo.sz), int(arrayInfo.sz))
		if arrayInfo.sz > 0 {
			l.Index(0).Set(reflect.ValueOf(ipair.IPair[any, any]{arrayInfo.first, arrayInfo.second}))
		}
		for i := int64(1); i < arrayInfo.sz; i++ {
			first, err := arrayInfo.firstReader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			second, err := arrayInfo.secondReader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			l.Index(int(i)).Set(reflect.ValueOf(ipair.IPair[any, any]{first, second}))
		}

		return l.Interface(), nil
	})
	setReaderR(I_BINARY, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		sz, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		array := make([]byte, int(sz))

		for i := 0; i < int(sz); i++ {
			if v, err := protocol.ReadByte(ctx); err != nil {
				return nil, ierror.Raise(err)
			} else {
				array[i] = byte(v)
			}
		}

		return array, nil
	}, []byte{}))
	setReaderR(I_MAP, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		var err error
		info := new(IMapReaderInfo)

		if info.sz, err = ReadSizeAux(protocol); err != nil {
			return nil, ierror.Raise(err)
		}
		keyId, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		valueId, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if info.keyReader, err = GetReader(keyId); err != nil {
			return nil, ierror.Raise(err)
		}
		if info.valReader, err = GetReader(valueId); err != nil {
			return nil, ierror.Raise(err)
		}

		if info.sz == 0 {
			if keyId == I_VOID || valueId == I_VOID {
				return make(map[any]any), nil
			}
			info.key = info.keyReader.Empty()
			info.val = info.valReader.Empty()
		} else {
			if info.key, err = info.keyReader.Read(protocol); err != nil {
				return nil, ierror.Raise(err)
			}
			if info.val, err = info.valReader.Read(protocol); err != nil {
				return nil, ierror.Raise(err)
			}
		}

		return GetGenericReader(I_MAP, GetNameMap(info.key, info.val)).Read(protocol, info)

	}, map[any]any{})).def = NewIGenericReaderImpl(func(protocol thrift.TProtocol, info any) (any, error) {
		mapInfo := info.(*IMapReaderInfo)

		m := reflect.MakeMapWithSize(reflect.MapOf(reflect.TypeOf(mapInfo.key), reflect.TypeOf(mapInfo.val)), int(mapInfo.sz))
		if mapInfo.sz > 0 {
			m.SetMapIndex(reflect.ValueOf(mapInfo.key), reflect.ValueOf(mapInfo.val))
		}
		for i := int64(1); i < mapInfo.sz; i++ {
			key, err := mapInfo.keyReader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			value, err := mapInfo.valReader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			m.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))

		}
		return m.Interface(), nil
	})
	setReaderR(I_SET, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		var err error
		info := new(ISetReaderInfo)

		if info.sz, err = ReadSizeAux(protocol); err != nil {
			return nil, ierror.Raise(err)
		}
		id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if info.reader, err = GetReader(id); err != nil {
			return nil, ierror.Raise(err)
		}
		if info.sz == 0 {
			if id == I_VOID {
				return make(map[any]bool), nil
			}
			info.elem = info.reader.Empty()
		} else {
			if info.elem, err = info.reader.Read(protocol); err != nil {
				return nil, ierror.Raise(err)
			}
		}

		return GetGenericReader(I_SET, GetName(info.elem)).Read(protocol, info)

	}, map[any]bool{})).def = NewIGenericReaderImpl(func(protocol thrift.TProtocol, info any) (any, error) {
		mapInfo := info.(*ISetReaderInfo)

		m := reflect.MakeChan(reflect.MapOf(reflect.TypeOf(mapInfo.elem), reflect.TypeOf(true)), int(mapInfo.sz))
		present := reflect.ValueOf(true)
		if mapInfo.sz > 0 {
			m.SetMapIndex(reflect.ValueOf(mapInfo.elem), present)
		}
		for i := int64(1); i < mapInfo.sz; i++ {
			key, err := mapInfo.reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			m.SetMapIndex(reflect.ValueOf(key), present)

		}
		return m.Interface(), nil
	})
	setReaderR(I_PAIR, NewIReader(func(protocol thrift.TProtocol) (any, error) {
		var err error
		info := new(IPairReaderInfo)

		firstId, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		secondId, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		firstReader, err := GetReader(firstId)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		secondReader, err := GetReader(secondId)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if info.first, err = firstReader.Read(protocol); err != nil {
			return nil, ierror.Raise(err)
		}
		if info.second, err = secondReader.Read(protocol); err != nil {
			return nil, ierror.Raise(err)
		}
		return GetGenericReader(I_PAIR, GetNamePair(info.first, info.second)).Read(protocol, info)
	}, ipair.IPair[any, any]{})).def = NewIGenericReaderImpl(func(protocol thrift.TProtocol, info any) (any, error) {
		pair := info.(*IPairReaderInfo)
		return ipair.IPair[any, any]{pair.first, pair.second}, nil
	})
}
