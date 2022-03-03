package iio

import (
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

func TypeNamePair[T1 any, T2 any]() string {
	return "(" + TypeName[T1]() + ":" + TypeName[T2]() + ")"
}

func GetNamePair(first any, second any) string {
	return "(" + GetName(first) + ":" + GetName(second) + ")"
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
