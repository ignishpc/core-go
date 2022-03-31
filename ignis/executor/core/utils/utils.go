package utils

import (
	"reflect"
	"unsafe"
)

func Min[T Ordered](a, b T) T {
	if a > b {
		return b
	}
	return a
}

func Max[T Ordered](a, b T) T {
	if a < b {
		return b
	}
	return a
}

func Ternary[T any](cond bool, v1 T, v2 T) T {
	if cond {
		return v1
	}
	return v2
}

func TypeObj[T any]() reflect.Type {
	return reflect.TypeOf((*T)(nil)).Elem()
}

func TypeName[T any]() string {
	return TypeObj[T]().String()
}

type iface struct {
	tab  *_type
	data unsafe.Pointer
}
type _type struct {
	size       uintptr
	ptrdata    uintptr
	hash       uint32
	tflag      uint8
	align      uint8
	fieldalign uint8
	kind       uint8
	/*...*/
}

func GetAnyData(obj *any) unsafe.Pointer {
	var kindDirectIface uint8 = 1 << 5
	tmp := (*iface)(unsafe.Pointer(obj))
	if tmp.tab.kind&kindDirectIface != 0 {
		return unsafe.Pointer(&tmp.data)
	}
	return tmp.data
}
