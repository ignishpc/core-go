package itransport

import "github.com/apache/thrift/lib/go/thrift"

type IZlibTransport struct {
	thrift.TZlibTransport
}

func NewTZlibTransport(trans thrift.TTransport, level int) (*IZlibTransport, error) {
	return nil, nil
}
