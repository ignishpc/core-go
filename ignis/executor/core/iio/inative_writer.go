package iio

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
)

func WriteNative(protocol thrift.TProtocol, obj any) error {
	/*enc := gob.NewEncoder(protocol.Transport())
	tp := reflect.TypeOf(obj)
	if err := enc.Encode(tp.String()); err != nil {
		return err
	}
	return enc.Encode(obj)*/
	return ierror.RaiseMsg("Not implemented yet")
}

func ReadNative(protocol thrift.TProtocol) (any, error) {
	/*dec := gob.NewDecoder(protocol.Transport())
	var tps string
	if err := dec.Decode(tps); err != nil {
		return nil, err
	}*/
	return nil, ierror.RaiseMsg("Not implemented yet")
}
