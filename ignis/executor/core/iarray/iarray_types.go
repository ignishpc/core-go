package iarray

import "reflect"

// @formatter:off
// registry[reflect.TypeOf(new([]<Type>))] = NewI<Type>Array
/* template
// []<Type> //

type I<Type>Array struct{ a []<Type> }

func NewI<Type>Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &I<Type>Array{other.([]<Type>)}}
	return &I<Type>Array{make([]<Type>, sz, cap)}
}
func (this *I<Type>Array) Array() interface{} {
	return this.a
}
func (this *I<Type>Array) Resize(sz int64, shrink bool) {
    l := int64(len(this.a))
	c := int64(cap(this.a))
    if c > sz && shrink { this.a = append([]<Type>(nil), this.a[:sz]...) } else
    if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
    if l == 0 {	this.a = make([]<Type>, sz) } else
    { this.a = append(this.a, make([]<Type>, l-sz)...)	}
}
func (this *I<Type>Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *I<Type>Array) Set(i int64, value interface{}) { this.a[i] = *value.(*<Type>) }
func (this *I<Type>Array) Append(value interface{})       { this.a = append(this.a, *value.(*<Type>)) }
func (this *I<Type>Array) Len() int64                     { return int64(len(this.a)) }
*/
func init() {
	registry[reflect.TypeOf(new([]bool))] = NewIboolArray
	registry[reflect.TypeOf(new([]int))] = NewIintArray
	registry[reflect.TypeOf(new([]int8))] = NewIint8Array
	registry[reflect.TypeOf(new([]int16))] = NewIint16Array
	registry[reflect.TypeOf(new([]int32))] = NewIint32Array
	registry[reflect.TypeOf(new([]int64))] = NewIint64Array
	registry[reflect.TypeOf(new([]uint))] = NewIuintArray
	registry[reflect.TypeOf(new([]uint8))] = NewIuint8Array
	registry[reflect.TypeOf(new([]uint16))] = NewIuint16Array
	registry[reflect.TypeOf(new([]uint32))] = NewIuint32Array
	registry[reflect.TypeOf(new([]uint64))] = NewIuint64Array
	registry[reflect.TypeOf(new([]float32))] = NewIfloat32Array
	registry[reflect.TypeOf(new([]float64))] = NewIfloat64Array
	registry[reflect.TypeOf(new([]complex64))] = NewIcomplex64Array
	registry[reflect.TypeOf(new([]complex128))] = NewIcomplex128Array
}

// []bool //

type IboolArray struct{ a []bool }

func NewIboolArray(sz, cap int64, other interface{}) IArray {
	if other != nil { return &IboolArray{other.([]bool)}}
	return &IboolArray{make([]bool, sz, cap)}
}
func (this *IboolArray) Array() interface{} {
	return this.a
}
func (this *IboolArray) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]bool(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]bool, sz) } else
	{ this.a = append(this.a, make([]bool, l-sz)...)	}
}
func (this *IboolArray) Get(i int64) interface{}        { return &this.a[i] }
func (this *IboolArray) Set(i int64, value interface{}) { this.a[i] = *value.(*bool) }
func (this *IboolArray) Append(value interface{})       { this.a = append(this.a, *value.(*bool)) }
func (this *IboolArray) Len() int64                     { return int64(len(this.a)) }


// []int //

type IintArray struct{ a []int }

func NewIintArray(sz, cap int64, other interface{}) IArray {
	if other != nil { return &IintArray{other.([]int)}}
	return &IintArray{make([]int, sz, cap)}
}
func (this *IintArray) Array() interface{} {
	return this.a
}
func (this *IintArray) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]int(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]int, sz) } else
	{ this.a = append(this.a, make([]int, l-sz)...)	}
}
func (this *IintArray) Get(i int64) interface{}        { return &this.a[i] }
func (this *IintArray) Set(i int64, value interface{}) { this.a[i] = *value.(*int) }
func (this *IintArray) Append(value interface{})       { this.a = append(this.a, *value.(*int)) }
func (this *IintArray) Len() int64                     { return int64(len(this.a)) }


// []int8 //

type Iint8Array struct{ a []int8 }

func NewIint8Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iint8Array{other.([]int8)}}
	return &Iint8Array{make([]int8, sz, cap)}
}
func (this *Iint8Array) Array() interface{} {
	return this.a
}
func (this *Iint8Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]int8(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]int8, sz) } else
	{ this.a = append(this.a, make([]int8, l-sz)...)	}
}
func (this *Iint8Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iint8Array) Set(i int64, value interface{}) { this.a[i] = *value.(*int8) }
func (this *Iint8Array) Append(value interface{})       { this.a = append(this.a, *value.(*int8)) }
func (this *Iint8Array) Len() int64                     { return int64(len(this.a)) }


// []int16 //

type Iint16Array struct{ a []int16 }

func NewIint16Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iint16Array{other.([]int16)}}
	return &Iint16Array{make([]int16, sz, cap)}
}
func (this *Iint16Array) Array() interface{} {
	return this.a
}
func (this *Iint16Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]int16(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]int16, sz) } else
	{ this.a = append(this.a, make([]int16, l-sz)...)	}
}
func (this *Iint16Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iint16Array) Set(i int64, value interface{}) { this.a[i] = *value.(*int16) }
func (this *Iint16Array) Append(value interface{})       { this.a = append(this.a, *value.(*int16)) }
func (this *Iint16Array) Len() int64                     { return int64(len(this.a)) }


// []int32 //

type Iint32Array struct{ a []int32 }

func NewIint32Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iint32Array{other.([]int32)}}
	return &Iint32Array{make([]int32, sz, cap)}
}
func (this *Iint32Array) Array() interface{} {
	return this.a
}
func (this *Iint32Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]int32(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]int32, sz) } else
	{ this.a = append(this.a, make([]int32, l-sz)...)	}
}
func (this *Iint32Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iint32Array) Set(i int64, value interface{}) { this.a[i] = *value.(*int32) }
func (this *Iint32Array) Append(value interface{})       { this.a = append(this.a, *value.(*int32)) }
func (this *Iint32Array) Len() int64                     { return int64(len(this.a)) }


// []int64 //

type Iint64Array struct{ a []int64 }

func NewIint64Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iint64Array{other.([]int64)}}
	return &Iint64Array{make([]int64, sz, cap)}
}
func (this *Iint64Array) Array() interface{} {
	return this.a
}
func (this *Iint64Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]int64(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]int64, sz) } else
	{ this.a = append(this.a, make([]int64, l-sz)...)	}
}
func (this *Iint64Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iint64Array) Set(i int64, value interface{}) { this.a[i] = *value.(*int64) }
func (this *Iint64Array) Append(value interface{})       { this.a = append(this.a, *value.(*int64)) }
func (this *Iint64Array) Len() int64                     { return int64(len(this.a)) }


// []uint //

type IuintArray struct{ a []uint }

func NewIuintArray(sz, cap int64, other interface{}) IArray {
	if other != nil { return &IuintArray{other.([]uint)}}
	return &IuintArray{make([]uint, sz, cap)}
}
func (this *IuintArray) Array() interface{} {
	return this.a
}
func (this *IuintArray) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]uint(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]uint, sz) } else
	{ this.a = append(this.a, make([]uint, l-sz)...)	}
}
func (this *IuintArray) Get(i int64) interface{}        { return &this.a[i] }
func (this *IuintArray) Set(i int64, value interface{}) { this.a[i] = *value.(*uint) }
func (this *IuintArray) Append(value interface{})       { this.a = append(this.a, *value.(*uint)) }
func (this *IuintArray) Len() int64                     { return int64(len(this.a)) }


// []uint8 //

type Iuint8Array struct{ a []uint8 }

func NewIuint8Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iuint8Array{other.([]uint8)}}
	return &Iuint8Array{make([]uint8, sz, cap)}
}
func (this *Iuint8Array) Array() interface{} {
	return this.a
}
func (this *Iuint8Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]uint8(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]uint8, sz) } else
	{ this.a = append(this.a, make([]uint8, l-sz)...)	}
}
func (this *Iuint8Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iuint8Array) Set(i int64, value interface{}) { this.a[i] = *value.(*uint8) }
func (this *Iuint8Array) Append(value interface{})       { this.a = append(this.a, *value.(*uint8)) }
func (this *Iuint8Array) Len() int64                     { return int64(len(this.a)) }


// []uint16 //

type Iuint16Array struct{ a []uint16 }

func NewIuint16Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iuint16Array{other.([]uint16)}}
	return &Iuint16Array{make([]uint16, sz, cap)}
}
func (this *Iuint16Array) Array() interface{} {
	return this.a
}
func (this *Iuint16Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]uint16(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]uint16, sz) } else
	{ this.a = append(this.a, make([]uint16, l-sz)...)	}
}
func (this *Iuint16Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iuint16Array) Set(i int64, value interface{}) { this.a[i] = *value.(*uint16) }
func (this *Iuint16Array) Append(value interface{})       { this.a = append(this.a, *value.(*uint16)) }
func (this *Iuint16Array) Len() int64                     { return int64(len(this.a)) }


// []uint32 //

type Iuint32Array struct{ a []uint32 }

func NewIuint32Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iuint32Array{other.([]uint32)}}
	return &Iuint32Array{make([]uint32, sz, cap)}
}
func (this *Iuint32Array) Array() interface{} {
	return this.a
}
func (this *Iuint32Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]uint32(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]uint32, sz) } else
	{ this.a = append(this.a, make([]uint32, l-sz)...)	}
}
func (this *Iuint32Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iuint32Array) Set(i int64, value interface{}) { this.a[i] = *value.(*uint32) }
func (this *Iuint32Array) Append(value interface{})       { this.a = append(this.a, *value.(*uint32)) }
func (this *Iuint32Array) Len() int64                     { return int64(len(this.a)) }


// []uint64 //

type Iuint64Array struct{ a []uint64 }

func NewIuint64Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Iuint64Array{other.([]uint64)}}
	return &Iuint64Array{make([]uint64, sz, cap)}
}
func (this *Iuint64Array) Array() interface{} {
	return this.a
}
func (this *Iuint64Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]uint64(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]uint64, sz) } else
	{ this.a = append(this.a, make([]uint64, l-sz)...)	}
}
func (this *Iuint64Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Iuint64Array) Set(i int64, value interface{}) { this.a[i] = *value.(*uint64) }
func (this *Iuint64Array) Append(value interface{})       { this.a = append(this.a, *value.(*uint64)) }
func (this *Iuint64Array) Len() int64                     { return int64(len(this.a)) }


// []float32 //

type Ifloat32Array struct{ a []float32 }

func NewIfloat32Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Ifloat32Array{other.([]float32)}}
	return &Ifloat32Array{make([]float32, sz, cap)}
}
func (this *Ifloat32Array) Array() interface{} {
	return this.a
}
func (this *Ifloat32Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]float32(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]float32, sz) } else
	{ this.a = append(this.a, make([]float32, l-sz)...)	}
}
func (this *Ifloat32Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Ifloat32Array) Set(i int64, value interface{}) { this.a[i] = *value.(*float32) }
func (this *Ifloat32Array) Append(value interface{})       { this.a = append(this.a, *value.(*float32)) }
func (this *Ifloat32Array) Len() int64                     { return int64(len(this.a)) }


// []float64 //

type Ifloat64Array struct{ a []float64 }

func NewIfloat64Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Ifloat64Array{other.([]float64)}}
	return &Ifloat64Array{make([]float64, sz, cap)}
}
func (this *Ifloat64Array) Array() interface{} {
	return this.a
}
func (this *Ifloat64Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]float64(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]float64, sz) } else
	{ this.a = append(this.a, make([]float64, l-sz)...)	}
}
func (this *Ifloat64Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Ifloat64Array) Set(i int64, value interface{}) { this.a[i] = *value.(*float64) }
func (this *Ifloat64Array) Append(value interface{})       { this.a = append(this.a, *value.(*float64)) }
func (this *Ifloat64Array) Len() int64                     { return int64(len(this.a)) }


// []complex64 //

type Icomplex64Array struct{ a []complex64 }

func NewIcomplex64Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Icomplex64Array{other.([]complex64)}}
	return &Icomplex64Array{make([]complex64, sz, cap)}
}
func (this *Icomplex64Array) Array() interface{} {
	return this.a
}
func (this *Icomplex64Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]complex64(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]complex64, sz) } else
	{ this.a = append(this.a, make([]complex64, l-sz)...)	}
}
func (this *Icomplex64Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Icomplex64Array) Set(i int64, value interface{}) { this.a[i] = *value.(*complex64) }
func (this *Icomplex64Array) Append(value interface{})       { this.a = append(this.a, *value.(*complex64)) }
func (this *Icomplex64Array) Len() int64                     { return int64(len(this.a)) }


// []complex128 //

type Icomplex128Array struct{ a []complex128 }

func NewIcomplex128Array(sz, cap int64, other interface{}) IArray {
	if other != nil { return &Icomplex128Array{other.([]complex128)}}
	return &Icomplex128Array{make([]complex128, sz, cap)}
}
func (this *Icomplex128Array) Array() interface{} {
	return this.a
}
func (this *Icomplex128Array) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink { this.a = append([]complex128(nil), this.a[:sz]...) } else
	if int64(cap(this.a)) > sz {this.a = this.a[:sz] } else
	if l == 0 {	this.a = make([]complex128, sz) } else
	{ this.a = append(this.a, make([]complex128, l-sz)...)	}
}
func (this *Icomplex128Array) Get(i int64) interface{}        { return &this.a[i] }
func (this *Icomplex128Array) Set(i int64, value interface{}) { this.a[i] = *value.(*complex128) }
func (this *Icomplex128Array) Append(value interface{})       { this.a = append(this.a, *value.(*complex128)) }
func (this *Icomplex128Array) Len() int64                     { return int64(len(this.a)) }

