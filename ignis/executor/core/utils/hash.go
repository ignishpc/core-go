package utils

import (
	"C"
	"constraints"
	"github.com/pierrec/xxHash/xxHash64"
	"reflect"
	"unsafe"
)

func memhash(p unsafe.Pointer, sz int) uint64 {
	return xxHash64.Checksum(C.GoBytes(p, C.int(sz)), 0)
}

type Hasher interface {
	Hash(p unsafe.Pointer) uint64
}

type fastHasher[T constraints.Integer] struct {
}

func (this *fastHasher[T]) Hash(p unsafe.Pointer) uint64 {
	return uint64(*(*T)(p))
}

type boolHasher struct {
}

func (this *boolHasher) Hash(p unsafe.Pointer) uint64 {
	b := (*bool)(p)
	if *b {
		return uint64(1)
	} else {
		return uint64(0)
	}
}

type basicHasher struct {
	sz int
}

func (this *basicHasher) Hash(p unsafe.Pointer) uint64 {
	return uint64(memhash(p, this.sz))
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

type stringHasher struct {
}

func (this *stringHasher) Hash(p unsafe.Pointer) uint64 {
	s := (*stringStruct)(p)
	return uint64(memhash(s.str, s.len))
}

type arrayHasher struct {
	sz int
}

func (this *arrayHasher) Hash(p unsafe.Pointer) uint64 {
	s := (*[1]int)(p)
	return uint64(memhash(unsafe.Pointer(&(*s)[0]), this.sz))
}

type errorHasher struct {
}

func (this *errorHasher) Hash(p unsafe.Pointer) uint64 {
	panic("type is not hashable")
}

func GetHasher(p reflect.Type) Hasher {
	switch p.Kind() {
	case reflect.Bool:
		return &boolHasher{}
	case reflect.Int:
		return &fastHasher[int]{}
	case reflect.Int8:
		return &fastHasher[int8]{}
	case reflect.Int16:
		return &fastHasher[int16]{}
	case reflect.Int32:
		return &fastHasher[uint32]{}
	case reflect.Int64:
		return &fastHasher[uint64]{}
	case reflect.Uint:
		return &fastHasher[uint]{}
	case reflect.Uint8:
		return &fastHasher[uint8]{}
	case reflect.Uint16:
		return &fastHasher[uint16]{}
	case reflect.Uint32:
		return &fastHasher[uint32]{}
	case reflect.Uint64:
		return &fastHasher[uint64]{}
	case reflect.Uintptr:
		return &fastHasher[uintptr]{}
	case reflect.Float32:
		return &fastHasher[int32]{} //Has same number of bytes
	case reflect.Float64:
		return &fastHasher[int64]{} //Has same number of bytes
	case reflect.Complex64:
		fallthrough
	case reflect.Complex128:
		fallthrough
	case reflect.Pointer:
		fallthrough
	case reflect.UnsafePointer:
		fallthrough
	case reflect.Struct:
		return &basicHasher{int(p.Size())}
	case reflect.String:
		return &stringHasher{}
	case reflect.Array:
		return &arrayHasher{int(p.Size() * p.Elem().Size())}
	case reflect.Chan:
		fallthrough
	case reflect.Func:
		fallthrough
	case reflect.Interface:
		fallthrough
	case reflect.Map:
		fallthrough
	case reflect.Slice:
		fallthrough
	default:
		return &errorHasher{}
	}
}

func Hash[T comparable](e T, h Hasher) uint64 {
	return h.Hash(unsafe.Pointer(&e))
}
