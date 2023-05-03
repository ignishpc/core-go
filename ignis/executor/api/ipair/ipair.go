package ipair

import (
	"reflect"
	"unsafe"
)

type IConvertPair interface {
	To() IAbstractPair
}

type IAbstractPair interface {
	SetFirst(f any)
	GetFirst() any
	GetFirstType() reflect.Type
	GetFirstPointer() unsafe.Pointer
	SetSecond(f any)
	GetSecond() any
	GetSecondType() reflect.Type
	GetSecondPointer() unsafe.Pointer
	Copy(first any, second any) IAbstractPair
	New(first any, second any) IAbstractPair
	NewEmpty() IAbstractPair
	Get() any
}

type IPair[T1 any, T2 any] struct {
	First  T1
	Second T2
}

func IsPairType(p reflect.Type) bool {
	return reflect.PointerTo(p).Implements(reflect.TypeOf((*IAbstractPair)(nil)).Elem())
}

func (this *IPair[T1, T2]) SetFirst(f any) {
	this.First = f.(T1)
}

func (this *IPair[T1, T2]) GetFirst() any {
	return this.First
}

func (this *IPair[T1, T2]) GetFirstPointer() unsafe.Pointer {
	return unsafe.Pointer(&this.First)
}

func (this *IPair[T1, T2]) GetFirstType() reflect.Type {
	return reflect.TypeOf(this.First)
}

func (this *IPair[T1, T2]) SetSecond(f any) {
	this.Second = f.(T2)
}

func (this *IPair[T1, T2]) GetSecond() any {
	return this.Second
}

func (this *IPair[T1, T2]) GetSecondPointer() unsafe.Pointer {
	return unsafe.Pointer(&this.Second)
}

func (this *IPair[T1, T2]) GetSecondType() reflect.Type {
	return reflect.TypeOf(this.Second)
}

func (p IPair[T1, T2]) To() IAbstractPair {
	return &p
}

func (this *IPair[T1, T2]) Copy(first any, second any) IAbstractPair {
	return &IPair[T1, T2]{this.First, this.Second}
}

func (this *IPair[T1, T2]) New(first any, second any) IAbstractPair {
	return &IPair[T1, T2]{first.(T1), second.(T2)}
}
func (this *IPair[T1, T2]) NewEmpty() IAbstractPair {
	return &IPair[T1, T2]{}
}

func (this *IPair[T1, T2]) Get() any {
	return *this
}

func New[T1 any, T2 any](first T1, second T2) *IPair[T1, T2] {
	return &IPair[T1, T2]{first, second}
}

func Compare[T1 comparable, T2 comparable](a *IPair[T1, T2], b *IPair[T1, T2]) bool {
	return a.First == b.First && a.Second == b.Second
}
