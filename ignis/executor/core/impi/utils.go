package impi

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"strings"
	"unsafe"
)

type C_int64 = C.int64_t
type C_int32 = C.int16_t
type C_int16 = C.int32_t
type C_int8 = C.int8_t

func C_ArrayToString(array []C_char) string {
	str := C.GoString(&array[0])
	return strings.Clone(str)
}

func C_ArrayFromString(str string) []C_char {
	array := make([]C_char, len(str)+1)
	for i, c := range str {
		array[i] = C_char(c)
	}
	array[len(str)] = '\x00'
	return array
}

func P[T any](ptr *T) unsafe.Pointer {
	return unsafe.Pointer(ptr)
}

var none C_int

func PS[T any](ptr *[]T) unsafe.Pointer {
	if len(*ptr) > 0 {
		return unsafe.Pointer(&((*ptr)[0]))
	}
	return unsafe.Pointer(&none)
}

func P_NIL() unsafe.Pointer { return unsafe.Pointer(nil) }

func Memcpy(dest unsafe.Pointer, src unsafe.Pointer, size int) {
	C.memcpy(dest, src, C.size_t(size))
}
