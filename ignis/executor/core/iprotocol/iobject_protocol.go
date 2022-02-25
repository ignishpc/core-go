package iprotocol

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
)

var ctx = context.Background()

const (
	IGNIS_PROTOCOL = 0
	GO_PROTOCOL    = 3
)

type IObjectProtocol struct {
	thrift.TCompactProtocol
}

func NewIObjectProtocol(trans thrift.TTransport) *IObjectProtocol {
	return &IObjectProtocol{
		*thrift.NewTCompactProtocolConf(trans, &thrift.TConfiguration{}),
	}
}

func (this *IObjectProtocol) WriteObject(obj any) error {
	return this.WriteObjectWithNative(obj, false)
}

func (this *IObjectProtocol) WriteObjectWithNative(obj any, native bool) error {
	if err := this.WriteSerialization(native); err != nil {
		return ierror.Raise(err)
	}
	if native {
		return iio.WriteNative(this, obj)
	} else {
		return iio.Write(this, obj)
	}
}

func (this *IObjectProtocol) ReadObject() (any, error) {
	native, err := this.ReadSerialization()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	if native {
		return iio.ReadNative(this)
	} else {
		return iio.Read[any](this)
	}
}

func (this *IObjectProtocol) ReadSerialization() (bool, error) {
	id, err := this.ReadByte(ctx)
	if err != nil {
		return false, nil
	}
	if id == IGNIS_PROTOCOL {
		return false, nil
	} else if id == GO_PROTOCOL {
		return true, nil
	} else {
		return false, ierror.RaiseMsg("Serialization is not compatible with Go")
	}
}

func (this *IObjectProtocol) WriteSerialization(native bool) error {
	if native {
		return this.WriteByte(ctx, GO_PROTOCOL)
	} else {
		return this.WriteByte(ctx, IGNIS_PROTOCOL)
	}
}
