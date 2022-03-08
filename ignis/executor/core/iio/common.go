package iio

import (
	"fmt"
	"ignis/executor/api/ipair"
	"ignis/executor/core/utils"
	"reflect"
	"strings"
)

func TypeGenericName[T any]() string {
	rt := reflect.TypeOf((*T)(nil)).Elem()
	switch rt.Kind() {
	case reflect.Pointer:
		return "*"
	case reflect.Slice:
		fallthrough
	case reflect.Array:
		return "[]"
	default:
		id := rt.String()
		i := strings.IndexByte(id, '[')
		if i > 0 {
			return id[0:i]
		}
		return id
	}
}

func TypeObj[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

func GetName(obj any) string {
	return reflect.TypeOf(obj).String()
}

func TypeName[T any]() string {
	return TypeObj[T]().String()
}

var pairName = strings.Replace(TypeName[ipair.IPair[any, any]](), "any", "%s", -1)

func GetNamePair(first any, second any) string {
	return fmt.Sprintf(pairName, GetName(first), GetName(second))
}

var mapName = strings.Replace(TypeName[map[any]any](), "any", "%s", -1)

func GetNameMap(key any, value any) string {
	return fmt.Sprintf(mapName, GetName(key), GetName(value))
}

var kindContiguous = [32]bool{}

func IsContiguous[T any]() bool {
	return IsContiguousType(TypeObj[T]())
}

func IsContiguousType(tp reflect.Type) bool {
	if tp.Kind() == reflect.Struct {
		for i := 0; i < tp.NumField(); i++ {
			if !IsContiguousType(tp.Field(i).Type) {
				return false
			}
		}
		return true
	} else {
		return kindContiguous[tp.Kind()]
	}
}

func init() {
	kindContiguous[reflect.Invalid] = false
	kindContiguous[reflect.Bool] = true
	kindContiguous[reflect.Int] = true
	kindContiguous[reflect.Int8] = true
	kindContiguous[reflect.Int16] = true
	kindContiguous[reflect.Int32] = true
	kindContiguous[reflect.Int64] = true
	kindContiguous[reflect.Uint] = true
	kindContiguous[reflect.Uint8] = true
	kindContiguous[reflect.Uint16] = true
	kindContiguous[reflect.Uint32] = true
	kindContiguous[reflect.Uint64] = true
	kindContiguous[reflect.Uintptr] = false
	kindContiguous[reflect.Float32] = true
	kindContiguous[reflect.Float64] = true
	kindContiguous[reflect.Complex64] = true
	kindContiguous[reflect.Complex128] = true
	kindContiguous[reflect.Array] = false
	kindContiguous[reflect.Chan] = false
	kindContiguous[reflect.Func] = false
	kindContiguous[reflect.Interface] = false
	kindContiguous[reflect.Map] = false
	kindContiguous[reflect.Pointer] = false
	kindContiguous[reflect.Slice] = false
	kindContiguous[reflect.String] = false
	kindContiguous[reflect.Struct] = false
	kindContiguous[reflect.UnsafePointer] = false
}

func AddBasicType[T any]() {
	tp := TypeObj[T]()
	writer, werr := GetWriter(GetName(tp))
	printer := GetPrinter(GetName(tp))

	//Pointers
	SetWriter(TypeName[*T](), &IPointerWriterType[T]{*writer.(*IWriterType), werr})
	SetPrinter(TypeName[*T](), &IPointerPrinterType[T]{*printer.(*IPrinterType)})

	//Array
	SetWriter(TypeName[[]T](), &IArrayWriterType[T]{
		*NewIWriteType(I_LIST, nil).(*IWriterType),
		*writer.(*IWriterType),
		werr,
	})
	SetPrinter(TypeName[[]T](), &IArrayPrinterType[T]{
		*NewIPrinterType(nil).(*IPrinterType),
		*printer.(*IPrinterType),
	})
	SetGenericReader(I_LIST, tp.String(), &IArrayGenericReader[T]{})

	// Only if TP is Pair
	if strings.HasPrefix(tp.String(), TypeGenericName[ipair.IPair[any, any]]()) {
		firstTp := tp.Field(0).Type
		secondTp := tp.Field(1).Type

		firstWriter, ferr := GetWriter(GetName(firstTp))
		secondWriter, serr := GetWriter(GetName(secondTp))

		//Pair
		SetGenericReader(I_PAIR, tp.String(), &IPairGenericReader[T]{})

		//Array of pairs
		SetWriter(TypeName[[]T](), &IPairArrayWriterType[T]{
			*NewIWriteType(I_LIST, nil).(*IWriterType),
			*firstWriter.(*IWriterType),
			*secondWriter.(*IWriterType),
			utils.Ternary(ferr != nil, ferr, serr),
		})
		SetGenericReader(I_PAIR_LIST, tp.String(), &IPairArrayGenericReader[T]{})
	}
}

func AddKeyType[K comparable, V any]() {
	name := TypeName[map[K]V]()
	keyTp := TypeObj[K]()
	valTp := TypeObj[V]()

	keyWriter, kerr := GetWriter(GetName(keyTp))
	vaWriter, verr := GetWriter(GetName(valTp))

	keyPrinter := GetPrinter(GetName(keyTp))
	vaPrinter := GetPrinter(GetName(valTp))

	//Map
	SetWriter(name, &IMapWriterType[K, V]{
		*NewIWriteType(I_MAP, nil).(*IWriterType),
		*keyWriter.(*IWriterType),
		*vaWriter.(*IWriterType),
		utils.Ternary(kerr != nil, kerr, verr),
	})
	SetPrinter(name, &IMapPrinterType[K, V]{
		*NewIPrinterType(nil).(*IPrinterType),
		*keyPrinter.(*IPrinterType),
		*vaPrinter.(*IPrinterType),
	})
	SetGenericReader(I_MAP, name, &IMapGenericReader[K, V]{})

	//Set
	SetGenericReader(I_SET, TypeName[K](), &ISetGenericReader[K]{})
}
