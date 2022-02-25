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
