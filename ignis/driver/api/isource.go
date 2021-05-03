package api

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/iprotocol"
	"ignis/rpc"
)

type ISource struct {
	native bool
	inner  *rpc.ISource
}

func NewISource(name string) *ISource {
	return NewISourceNative(name, false)
}

func NewISourceNative(name string, native bool) *ISource {
	inner := rpc.NewISource()
	inner.Obj = rpc.NewIEncoded()
	inner.Obj.Name = &name
	return &ISource{
		native,
		inner,
	}
}

func (this *ISource) addParam(key string, value interface{}) *ISource {
	buffer :=thrift.NewTMemoryBuffer()
	proto := iprotocol.NewIObjectProtocol(buffer)
	proto.WriteObjectNative(value, this.native)
	this.inner.Params[key] = buffer.Bytes()
	return this
}

func (this *ISource) rpc() *rpc.ISource {
	return this.inner
}
