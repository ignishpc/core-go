package iarray

import "reflect"

var registry = map[reflect.Type]NewIArray{}

func Registry(tp reflect.Type, newf NewIArray) {
	registry[tp] = newf
}

func Get(tp reflect.Type) NewIArray {
	if tp.Kind() == reflect.Ptr {
		return NewIPointerArray
	}
	a, found := registry[tp]
	if found {
		return a
	}
	return func(sz, cap int64, other interface{}) IArray {
		return NewIDefaultArray(sz, cap, other, tp)
	}
}

type NewIArray func(sz, cap int64, other interface{}) IArray

type IArray interface {
	Array() interface{}
	Resize(sz int64, shrink bool)
	Get(i int64) interface{}
	Set(i int64, value interface{})
	Append(value interface{})
	Len() int64
}

type IPointerArray struct {
	a []interface{}
}

func NewIPointerArray(sz, cap int64, other interface{}) IArray {
	if other != nil {
		return &IPointerArray{other.([]interface{})}
	}
	return &IPointerArray{make([]interface{}, sz, cap)}
}

func (this *IPointerArray) Array() interface{} {
	return this.a
}

func (this *IPointerArray) Resize(sz int64, shrink bool) {
	l := int64(len(this.a))
	c := int64(cap(this.a))
	if c > sz && shrink {
		this.a = append([]interface{}(nil), this.a[:sz]...)
	} else if int64(cap(this.a)) > sz {
		this.a = this.a[:sz]
	} else if len(this.a) == 0 {
		this.a = make([]interface{}, sz)
	} else {
		this.a = append(this.a, make([]interface{}, l-sz)...)
	}
}
func (this *IPointerArray) Get(i int64) interface{}        { return &this.a[i] }
func (this *IPointerArray) Set(i int64, value interface{}) { this.a[i] = value }
func (this *IPointerArray) Append(value interface{})       { this.a = append(this.a, value) }
func (this *IPointerArray) Len() int64                     { return int64(len(this.a)) }

type IDefaultArray struct {
	a reflect.Value
}

func NewIDefaultArray(sz, cap int64, other interface{}, tp reflect.Type) IArray {
	if other == nil {
		other = reflect.MakeSlice(tp, int(sz), int(cap))
	}
	return &IDefaultArray{reflect.ValueOf(other)}
}
func (this *IDefaultArray) Array() interface{} {
	return this.a
}
func (this *IDefaultArray) Resize(sz int64, shrink bool) {
	l := int64(this.a.Len())
	c := int64(this.a.Cap())
	if c > sz && shrink {
		other := reflect.MakeSlice(this.a.Type(), 0, int(sz))
		this.a = reflect.Append(other, this.a.Slice(0, int(sz)))
	} else if int64(this.a.Cap()) > sz {
		this.a = this.a.Slice(0, int(sz))
	} else if l == 0 {
		other := reflect.MakeSlice(this.a.Type(), int(sz), int(sz))
		this.a = reflect.ValueOf(other)
	} else {
		this.a = reflect.AppendSlice(this.a, reflect.MakeSlice(this.a.Type(), int(sz-l), int(sz-l)))
	}
}
func (this *IDefaultArray) Get(i int64) interface{} {
	return this.a.Index(int(i)).Interface()
}
func (this *IDefaultArray) Set(i int64, value interface{}) {
	this.a.Index(int(i)).Set(reflect.ValueOf(value))
}
func (this *IDefaultArray) Append(value interface{}) {
	this.a = reflect.Append(this.a, reflect.ValueOf(value))
}
func (this *IDefaultArray) Len() int64 { return this.Len() }