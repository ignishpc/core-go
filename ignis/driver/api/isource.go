package api

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/driver/api/derror"
	"ignis/executor/core/iio"
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

func AddParam[T any](this *ISource, key string, value T) (*ISource, error) {
	iio.AddBasicType[T]()
	buffer := thrift.NewTMemoryBuffer()
	proto := iprotocol.NewIObjectProtocol(buffer)
	if err := proto.WriteObjectWithNative(value, this.native); err != nil {
		return this, derror.NewGenericIDriverError(err)
	}
	this.inner.Params[key] = buffer.Bytes()
	return this, nil
}

func (this *ISource) rpc() *rpc.ISource {
	return this.inner
}
