package iio

import (
	"ignis/executor/api/ipair"
	"ignis/executor/core/utils"
	"reflect"
	"regexp"
	"strings"
)

var fixer = regexp.MustCompile(`([a-zA-Z0-9\/]*\.)`)

func NameFix(id string) string { //type name wrong issues
	return fixer.ReplaceAllString(id, "")
}

func TypeGenericName[T any]() string {
	rt := utils.TypeObj[T]()
	id := rt.String()
	i := strings.LastIndexByte(id, '[')
	if i < 0 {
		return id
	}
	return id[0:i]
}

var kindContiguous = [32]bool{}

func IsContiguous[T any]() bool {
	return IsContiguousType(utils.TypeObj[T]())
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
	tp := utils.TypeObj[T]()

	//Native
	newNativeType[T]()

	//Basic
	SetPrinter(utils.TypeName[T](), &IPrinterFmt[T]{})

	if ipair.IsPairType(utils.TypeObj[T]()) { // Only if TP is Pair
		firstTp := tp.Field(0).Type
		secondTp := tp.Field(1).Type

		afirstWriter, _, _ := GetWriterObj(reflect.New(firstTp).Elem().Interface())
		firstWriter := afirstWriter.(*IAnyWriterType)
		asecondWriter, _, _ := GetWriterObj(reflect.New(secondTp).Elem().Interface())
		secondWriter := asecondWriter.(*IAnyWriterType)

		afirstPrinter, _ := GetPrinterObj(reflect.New(firstTp).Elem().Interface())
		firstPrinter := afirstPrinter.(*IAnyPrinterType)
		asecondPrinter, _ := GetPrinterObj(reflect.New(firstTp).Elem().Interface())
		secondPrinter := asecondPrinter.(*IAnyPrinterType)

		//Pair
		SetGenericReader(I_PAIR, tp.String(), &IPairGenericReader[T]{})
		SetWriter(utils.TypeName[T](), &IPairWriterType[T]{
			*NewIWrite(I_PAIR, nil).(*IWriterType),
			firstWriter.valWriter,
			firstWriter.rtp,
			secondWriter.valWriter,
			secondWriter.rtp,
			utils.Ternary(firstWriter.err != nil, firstWriter.err, secondWriter.err),
		})
		SetPrinter(utils.TypeName[T](), &IPairPrinterType[T]{
			firstPrinter.valPrinter,
			firstPrinter.rtp,
			secondPrinter.valPrinter,
			secondPrinter.rtp,
		})

		//Array of pairs
		SetWriter(utils.TypeName[[]T](), &IPairArrayWriterType[T]{
			*NewIWrite(I_PAIR_LIST, nil).(*IWriterType),
			firstWriter.valWriter,
			firstWriter.rtp,
			secondWriter.valWriter,
			secondWriter.rtp,
			utils.Ternary(firstWriter.err != nil, firstWriter.err, secondWriter.err),
		})
		SetGenericReader(I_PAIR_LIST, tp.String(), &IPairArrayGenericReader[T]{})
	}

	//Array
	if !ipair.IsPairType(utils.TypeObj[T]()) {
		awriter, _, _ := GetWriterObj(reflect.New(tp).Elem().Interface())
		writer := awriter.(*IAnyWriterType)

		SetWriter(utils.TypeName[[]T](), &IArrayWriterType[T]{
			*NewIWrite(I_LIST, nil).(*IWriterType),
			writer.rtp,
			writer.valWriter,
			writer.err,
		})

		aprinter, _ := GetPrinterObj(reflect.New(tp).Elem().Interface())
		printer := aprinter.(*IAnyPrinterType)

		SetPrinter(utils.TypeName[[]T](), &IArrayPrinterType[T]{
			printer.rtp,
			printer.valPrinter,
		})
	}
	SetGenericReader(I_LIST, tp.String(), &IArrayGenericReader[T]{})
}

func AddKeyType[K comparable, V any]() {
	name := utils.TypeName[map[K]V]()
	keyTp := utils.TypeObj[K]()
	valTp := utils.TypeObj[V]()

	akeyWriter, _, _ := GetWriterObj(reflect.New(keyTp).Elem().Interface())
	keyWriter := akeyWriter.(*IAnyWriterType)
	avalWriter, _, _ := GetWriterObj(reflect.New(valTp).Elem().Interface())
	valWriter := avalWriter.(*IAnyWriterType)

	akeyPrinter, _ := GetPrinterObj(reflect.New(keyTp).Elem().Interface())
	keyPrinter := akeyPrinter.(*IAnyPrinterType)
	avalPrinter, _ := GetPrinterObj(reflect.New(valTp).Elem().Interface())
	valPrinter := avalPrinter.(*IAnyPrinterType)

	//Native
	newNativeType[map[K]V]()
	newNativeType[ipair.IPair[K, V]]()

	//Map
	SetWriter(name, &IMapWriterType[K, V]{
		*NewIWrite(I_MAP, nil).(*IWriterType),
		keyWriter.valWriter,
		keyWriter.rtp,
		valWriter.valWriter,
		valWriter.rtp,
		utils.Ternary(keyWriter.err != nil, keyWriter.err, valWriter.err),
	})
	SetPrinter(name, &IMapPrinterType[K, V]{
		keyPrinter.valPrinter,
		keyPrinter.rtp,
		valPrinter.valPrinter,
		valPrinter.rtp,
	})
	SetGenericReader(I_MAP, name, &IMapGenericReader[K, V]{})

	//Set
	SetGenericReader(I_SET, utils.TypeName[K](), &ISetGenericReader[K]{})
}
