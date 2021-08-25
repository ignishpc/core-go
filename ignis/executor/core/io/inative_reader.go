package io

import (
	"encoding/gob"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"reflect"
)

type _INativeReader struct {
}

type _name struct {
	id   string
	kind reflect.Kind
	sz   int
	sub  [2]*_name
}

var types = map[string]reflect.Type{}
var INativeReader = new(_INativeReader)

func (this _INativeReader) Read(protocol thrift.TProtocol) (interface{}, error) {
	dec := gob.NewDecoder(protocol.Transport())
	var id _name
	if err := dec.Decode(&id); err != nil {
		return nil, err
	}
	tp, err := getType(id)
	if err != nil {
		return nil, err
	}
	obj := reflect.New(tp)
	err = dec.Decode(&obj)
	return obj, err
}

func geId(tp reflect.Type) string {
	return fmt.Sprintf("%d:%s.%s", tp.Kind(), tp.PkgPath(), tp.Name())
}

func getName(tp reflect.Type) _name {
	sz := 0
	switch tp.Kind() {
	case reflect.Array:
		sz = tp.Len()
	case reflect.Slice:
		etp := getName(tp.Elem())
		return _name{
			id:   "",
			kind: tp.Kind(),
			sz:   sz,
			sub: [2]*_name{
				&etp,
				nil},
		}
	case reflect.Map:
		key := getName(tp.Key())
		etp := getName(tp.Elem())
		return _name{
			id:   "",
			kind: tp.Kind(),
			sub: [2]*_name{
				&key,
				&etp},
		}
	}
	return _name{
		id:   geId(tp),
		kind: tp.Kind(),
		sub:  [2]*_name{},
	}

}

func getType(id _name) (reflect.Type, error) {
	switch id.kind {
	case reflect.Ptr:
		etp, err := getType(*id.sub[0])
		if err != nil {
			return nil, err
		}
		return reflect.PtrTo(etp), nil
	case reflect.Array:
		etp, err := getType(*id.sub[0])
		if err != nil {
			return nil, err
		}
		return reflect.ArrayOf(id.sz, etp), nil
	case reflect.Slice:
		etp, err := getType(*id.sub[0])
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(etp), nil
	case reflect.Map:
		ekey, err := getType(*id.sub[0])
		if err != nil {
			return nil, err
		}
		evalue, err2 := getType(*id.sub[0])
		if err2 != nil {
			return nil, err2
		}
		return reflect.MapOf(ekey, evalue), nil
	}
	tp, found := types[id.id]
	if !found {
		return nil, ierror.RaiseMsg("Go Decoder require a type, use INativeReader.Register with " + id.id)
	}
	return tp, nil
}

func (this _INativeReader) Register(tp reflect.Type) {
	types[geId(tp)] = tp
}

func init() {
	INativeReader.Register(reflect.TypeOf((*bool)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*int)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*int8)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*int16)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*int32)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*int64)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*uint)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*uint8)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*uint16)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*uint32)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*uint64)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*uintptr)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*float32)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*float64)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*complex64)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*complex128)(nil)).Elem())
	INativeReader.Register(reflect.TypeOf((*string)(nil)).Elem())
}
