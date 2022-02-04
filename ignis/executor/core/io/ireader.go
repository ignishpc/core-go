package io

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"reflect"
)

func typeName[T any]() string {
	return reflect.TypeOf((*T)(nil)).Elem().String()
}

func typePairName[T1 any, T2 any]() string {
	return typeName[T1]() + ":" + typeName[T2]()
}

type IReaderType interface {
	Read(protocol thrift.TProtocol) (any, error)
}

type IReaderTypeF func(protocol thrift.TProtocol) (any, error)

var readers = make([]IReaderType, 256)

func SetReader(key int8, value IReaderType) {
	readers[key] = value
}

func GetReader(key int8) IReaderType {
	return readers[key]
}

func GetReaderCheck(key int8) (IReaderType, error) {
	reader := readers[key]
	if reader == nil {
		return nil, ierror.RaiseMsg(fmt.Sprintf("IReaderType not implemented for id %d", key))
	}
	return reader, nil
}

func RemoveReader(key int8) {
	readers[key] = nil
}

func Read[T any](protocol thrift.TProtocol) (r T, errr error) {
	id, err := ReadTypeAux(protocol)
	if err != nil {
		errr = ierror.Raise(err)
		return
	}
	reader, err := GetReaderCheck(id)
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

func ReadTypeAux(protocol thrift.TProtocol) (int8, error) {
	return protocol.ReadByte(ctx)
}

func ReadSizeAux(protocol thrift.TProtocol) (int64, error) {
	return protocol.ReadI64(ctx)
}

type IReaderTypeHelper interface {
	Set(args ...any) (any, error)
}

func getHelper(key int8, obj any) IReaderTypeHelper {
	return readers[key].(*IReaderTypeImpl).helpers[reflect.TypeOf(obj).String()]
}

func getHelperPair(key int8, first any, second any) IReaderTypeHelper {
	return readers[key].(*IReaderTypeImpl).helpers[reflect.TypeOf(first).String()+":"+reflect.TypeOf(second).String()]
}

type iReaderTypeHelperArray[T any] struct {
}

func (this *iReaderTypeHelperArray[T]) Set(args ...any) (any, error) {
	protocol := args[0].(thrift.TProtocol)
	sz := args[1].(int64)
	reader := args[2].(IReaderType)
	first := args[3].(T)

	array := make([]T, sz)
	array[0] = first
	for i := int64(1); i < sz; i++ {
		elem, err := reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		array[i] = elem.(T)
	}
	return array, nil
}

type iReaderTypeHelperSet[T comparable] struct {
}

func (this *iReaderTypeHelperSet[T]) Set(args ...any) (any, error) {
	protocol := args[0].(thrift.TProtocol)
	sz := args[1].(int64)
	reader := args[2].(IReaderType)
	first := args[3].(T)

	s := map[T]bool{}
	s[first] = true
	for i := int64(1); i < sz; i++ {
		elem, err := reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		s[elem.(T)] = true
	}
	return s, nil
}

type iReaderTypeHelperPair[T1 any, T2 any] struct {
}

func (this *iReaderTypeHelperPair[T1, T2]) Set(args ...any) (any, error) {
	first := args[0].(T1)
	second := args[1].(T2)
	return ipair.IPair[T1, T2]{first, second}, nil
}

type iReaderTypeHelperArrayPair[T1 any, T2 any] struct {
}

func (this *iReaderTypeHelperArrayPair[T1, T2]) Set(args ...any) (any, error) {
	protocol := args[0].(thrift.TProtocol)
	sz := args[1].(int64)
	reader_first := args[2].(IReaderType)
	reader_second := args[3].(IReaderType)
	first := args[4].(T1)
	second := args[5].(T2)

	array := make([]ipair.IPair[T1, T2], sz)
	array[0] = ipair.IPair[T1, T2]{first, second}
	for i := int64(1); i < sz; i++ {
		first, err := reader_first.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second, err := reader_second.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		array[i] = ipair.IPair[T1, T2]{first.(T1), second.(T2)}
	}
	return array, nil
}

type iReaderTypeHelperMap[T1 comparable, T2 any] struct {
}

func (this *iReaderTypeHelperMap[T1, T2]) Set(args ...any) (any, error) {
	protocol := args[0].(thrift.TProtocol)
	sz := args[1].(int64)
	reader_first := args[2].(IReaderType)
	reader_second := args[3].(IReaderType)
	first := args[4].(T1)
	second := args[5].(T2)

	m := map[T1]T2{}
	m[first] = second

	for i := int64(1); i < sz; i++ {
		key, err := reader_first.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		value, err := reader_second.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		m[key.(T1)] = value.(T2)
	}
	return m, nil
}

func createArrayReaderHelper[T any]() error {
	reader_list, err := GetReaderCheck(I_LIST)
	if err != nil {
		return ierror.Raise(err)
	}
	reader_list.(*IReaderTypeImpl).helpers[typeName[T]()] = &iReaderTypeHelperArray[T]{}
	return nil
}

func createPairReaderHelper[T1 comparable, T2 any]() error {
	reader_pair, err := GetReaderCheck(I_PAIR)
	if err != nil {
		return ierror.Raise(err)
	}
	reader_list, err := GetReaderCheck(I_PAIR_LIST)
	if err != nil {
		return ierror.Raise(err)
	}
	reader_map, err := GetReaderCheck(I_MAP)
	if err != nil {
		return ierror.Raise(err)
	}
	reader_set, err := GetReaderCheck(I_SET)
	if err != nil {
		return ierror.Raise(err)
	}

	reader_pair.(*IReaderTypeImpl).helpers[typePairName[T1, T2]()] = &iReaderTypeHelperPair[T1, T2]{}
	reader_list.(*IReaderTypeImpl).helpers[typePairName[T1, T2]()] = &iReaderTypeHelperArrayPair[T1, T2]{}
	reader_map.(*IReaderTypeImpl).helpers[typePairName[T1, T2]()] = &iReaderTypeHelperMap[T1, T2]{}
	reader_set.(*IReaderTypeImpl).helpers[typeName[T1]()] = &iReaderTypeHelperSet[T1]{}
	return nil
}

type IReaderTypeImpl struct {
	helpers map[string]IReaderTypeHelper
	read    IReaderTypeF
}

func (this *IReaderTypeImpl) Read(protocol thrift.TProtocol) (any, error) {
	return this.read(protocol)
}

func NewIReaderType(f IReaderTypeF) IReaderType {
	return &IReaderTypeImpl{
		map[string]IReaderTypeHelper{},
		f,
	}
}

func init() {
	SetReader(I_VOID, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return nil, nil
	}))
	SetReader(I_BOOL, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadBool(ctx)
	}))
	SetReader(I_I08, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadByte(ctx)
	}))
	SetReader(I_I16, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadI16(ctx)
	}))
	SetReader(I_I32, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadI32(ctx)
	}))
	SetReader(I_I64, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadI64(ctx)
	}))
	SetReader(I_DOUBLE, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadDouble(ctx)
	}))
	SetReader(I_STRING, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		return protocol.ReadString(ctx)
	}))
	SetReader(I_LIST, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		sz, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if sz == 0 {
			return make([]any, 0), nil
		}

		reader, err := GetReaderCheck(id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		elem, err := reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if helper := getHelper(I_LIST, elem); helper != nil {
			return helper.Set(protocol, sz, reader, elem)
		}

		l := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(elem)), int(sz), int(sz))
		l.Index(0).Set(reflect.ValueOf(elem))
		for i := int64(1); i < sz; i++ {
			elem, err = reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			l.Index(int(i)).Set(reflect.ValueOf(elem))
		}

		return l.Interface(), nil
	}))
	SetReader(I_SET, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		sz, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if sz == 0 {
			return make([]any, 0), nil
		}

		reader, err := GetReaderCheck(id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		key, err := reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if helper := getHelper(I_SET, key); helper != nil {
			return helper.Set(protocol, sz, reader, key)
		}

		s := reflect.MakeChan(reflect.MapOf(reflect.TypeOf(key), reflect.TypeOf(true)), int(sz))
		value := reflect.ValueOf(true)
		s.SetMapIndex(reflect.ValueOf(key), value)
		for i := int64(1); i < sz; i++ {
			key, err = reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			s.SetMapIndex(reflect.ValueOf(key), value)

		}
		return s.Interface(), nil
	}))
	SetReader(I_MAP, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		sz, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		key_id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		value_id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if sz == 0 {
			return make([]any, 0), nil
		}

		key_reader, err := GetReaderCheck(key_id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		value_reader, err := GetReaderCheck(value_id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		key, err := key_reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		value, err := value_reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if helper := getHelperPair(I_MAP, key, value); helper != nil {
			return helper.Set(protocol, sz, key_reader, key_reader, key, value)
		}

		m := reflect.MakeChan(reflect.MapOf(reflect.TypeOf(key), reflect.TypeOf(value)), int(sz))
		m.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
		for i := int64(1); i < sz; i++ {
			key, err := key_reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			value, err := value_reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			m.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))

		}
		return m.Interface(), nil
	}))
	SetReader(I_PAIR, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		first_id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second_id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		first_reader, err := GetReaderCheck(first_id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second_reader, err := GetReaderCheck(second_id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		first, err := first_reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second, err := second_reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if helper := getHelperPair(I_MAP, first, second); helper != nil {
			return helper.Set(first, second)
		}

		return ipair.IPair[any, any]{first, second}, nil
	}))
	SetReader(I_BINARY, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		sz, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		bs := make([]byte, int(sz))

		for i := int64(0); i < sz; i++ {
			elem, err := protocol.ReadByte(ctx)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			bs[i] = byte(elem)
		}
		return bs, nil
	}))
	SetReader(I_PAIR_LIST, NewIReaderType(func(protocol thrift.TProtocol) (any, error) {
		sz, err := ReadSizeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		first_id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second_id, err := ReadTypeAux(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if sz == 0 {
			return make([]ipair.IPair[any, any], 0), nil
		}

		first_reader, err := GetReaderCheck(first_id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second_reader, err := GetReaderCheck(second_id)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		first, err := first_reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		second, err := second_reader.Read(protocol)
		if err != nil {
			return nil, ierror.Raise(err)
		}

		if helper := getHelperPair(I_PAIR_LIST, first, second); helper != nil {
			return helper.Set(protocol, sz, first_reader, second_reader, first, second)
		}

		l := reflect.MakeSlice(reflect.TypeOf(ipair.IPair[any, any]{first, second}), int(sz), int(sz))
		l.Index(0).Set(reflect.ValueOf(ipair.IPair[any, any]{first, second}))
		for i := int64(1); i < sz; i++ {
			first, err := first_reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			second, err := second_reader.Read(protocol)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			l.Index(int(i)).Set(reflect.ValueOf(ipair.IPair[any, any]{first, second}))
		}

		return l.Interface(), nil
	}))
}

/*
func initArray() {
	createArrayReaderHelper[bool]()
	createArrayReaderHelper[int8]()
	createArrayReaderHelper[int16]()
	createArrayReaderHelper[int32]()
	createArrayReaderHelper[int64]()
	createArrayReaderHelper[float64]()
	createArrayReaderHelper[string]()

	createArrayReaderHelper[[]bool]()
	createArrayReaderHelper[[]int8]()
	createArrayReaderHelper[[]int16]()
	createArrayReaderHelper[[]int32]()
	createArrayReaderHelper[[]int64]()
	createArrayReaderHelper[[]float64]()
	createArrayReaderHelper[[]string]()
}

func initPair() {
	createPairReaderHelper[bool, bool]()
	createPairReaderHelper[bool, int8]()
	createPairReaderHelper[bool, int16]()
	createPairReaderHelper[bool, int32]()
	createPairReaderHelper[bool, int64]()
	createPairReaderHelper[bool, float64]()
	createPairReaderHelper[bool, string]()

	createPairReaderHelper[int8, bool]()
	createPairReaderHelper[int8, int8]()
	createPairReaderHelper[int8, int16]()
	createPairReaderHelper[int8, int32]()
	createPairReaderHelper[int8, int64]()
	createPairReaderHelper[int8, float64]()
	createPairReaderHelper[int8, string]()

	createPairReaderHelper[int16, bool]()
	createPairReaderHelper[int16, int8]()
	createPairReaderHelper[int16, int16]()
	createPairReaderHelper[int16, int32]()
	createPairReaderHelper[int16, int64]()
	createPairReaderHelper[int16, float64]()
	createPairReaderHelper[int16, string]()

	createPairReaderHelper[int32, bool]()
	createPairReaderHelper[int32, int8]()
	createPairReaderHelper[int32, int16]()
	createPairReaderHelper[int32, int32]()
	createPairReaderHelper[int32, int64]()
	createPairReaderHelper[int32, float64]()
	createPairReaderHelper[int32, string]()

	createPairReaderHelper[int64, bool]()
	createPairReaderHelper[int64, int8]()
	createPairReaderHelper[int64, int16]()
	createPairReaderHelper[int64, int32]()
	createPairReaderHelper[int64, int64]()
	createPairReaderHelper[int64, float64]()
	createPairReaderHelper[int64, string]()

	createPairReaderHelper[float64, bool]()
	createPairReaderHelper[float64, int8]()
	createPairReaderHelper[float64, int16]()
	createPairReaderHelper[float64, int32]()
	createPairReaderHelper[float64, int64]()
	createPairReaderHelper[float64, float64]()
	createPairReaderHelper[float64, string]()

	createPairReaderHelper[string, bool]()
	createPairReaderHelper[string, int8]()
	createPairReaderHelper[string, int16]()
	createPairReaderHelper[string, int32]()
	createPairReaderHelper[string, int64]()
	createPairReaderHelper[string, float64]()
	createPairReaderHelper[string, string]()
}*/
