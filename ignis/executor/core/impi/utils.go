package impi

/*
#include <stdlib.h>
*/
import "C"
import (
	"strings"
	"unsafe"
)

type C_int64 = C.int64_t

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
