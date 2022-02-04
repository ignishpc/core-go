package itransport

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
)

type IZlibTransport struct {
	thrift.TZlibTransport
}

func NewIZlibTransport(trans thrift.TTransport) (*IZlibTransport, error) {
	return NewIZlibTransportWithLevel(trans, 6)
}

func NewIZlibTransportWithLevel(trans thrift.TTransport, level int) (*IZlibTransport, error) {
	parent, err := thrift.NewTZlibTransport(trans, 6)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return &IZlibTransport{*parent}, nil
}
