package iprotocol

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"ignis/executor/core/io"
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

func (this *IObjectProtocol) WriteObject(obj interface{}) error {
	return this.WriteObjectNative(obj, false)
}

func (this *IObjectProtocol) ReadObject() (interface{}, error) {
	native, err := this.ReadSerialization()
	if err != nil {
		return nil, err
	}
	if native {
		return io.INativeReader.Read(this)
	} else {
		return io.IReader.Read(this)
	}

}

func (this *IObjectProtocol) WriteObjectNative(obj interface{}, native bool) error {
	if err := this.WriteSerialization(native); err != nil {
		return err
	}
	if native {
		return io.INativeWriter.Write(this, obj)
	} else {
		return io.IWriter.Write(this, obj)
	}
}

func (this *IObjectProtocol) ReadSerialization() (bool, error) {
	id, err := this.ReadByte(ctx)
	if err != nil {
		return false, nil
	}
	if id == IGNIS_PROTOCOL {
		return true, nil
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
