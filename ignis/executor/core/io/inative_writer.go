package io

import (
	"encoding/gob"
	"github.com/apache/thrift/lib/go/thrift"
	"reflect"
)

type _INativeWriter struct {
}

var INativeWriter = new(_INativeWriter)

func (this _INativeWriter) Write(protocol thrift.TProtocol, obj interface{}) error {
	enc := gob.NewEncoder(protocol.Transport())
	tp := reflect.TypeOf(obj)
	INativeReader.Register(tp)
	if err := enc.Encode(getName(tp)); err != nil {
		return err
	}
	return enc.Encode(obj)
}
