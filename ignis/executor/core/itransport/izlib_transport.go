package itransport

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
)

type IZlibTransport struct {
	thrift.TZlibTransport
	trans          thrift.TTransport
	winit          bool
	rinit          bool
	inCompression  int
	outCompression int
}

func NewIZlibTransport(trans thrift.TTransport) (*IZlibTransport, error) {
	return NewIZlibTransportWithLevel(trans, 6)
}

func NewIZlibTransportWithLevel(trans thrift.TTransport, level int) (*IZlibTransport, error) {
	parent, err := thrift.NewTZlibTransport(trans, 6)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return &IZlibTransport{*parent, trans, false, false, 0, level}, nil
}

func (this *IZlibTransport) Read(p []byte) (int, error) {
	if !this.rinit {
		aux := []byte{0}
		this.trans.Read(aux)
		this.inCompression = int(aux[0])
		this.rinit = true
	}
	if this.inCompression > 0 {
		return this.TZlibTransport.Read(p)
	} else {
		return this.trans.Read(p)
	}
}

func (this *IZlibTransport) Write(p []byte) (int, error) {
	if !this.winit {
		aux := []byte{byte(this.outCompression)}
		if _, err := this.trans.Write(aux); err != nil {
			return 0, err
		}
		this.winit = true
	}
	if this.outCompression > 0 {
		return this.TZlibTransport.Write(p)
	} else {
		return this.trans.Write(p)
	}
}

func (this *IZlibTransport) Flush(ctx context.Context) error {
	if !this.winit {
		aux := []byte{0}
		if _, err := this.trans.Write(aux); err != nil {
			return err
		}
	} else if this.outCompression == 0 {
		return this.trans.Flush(ctx)
	}
	return this.TZlibTransport.Flush(ctx)
}

func (this *IZlibTransport) GetTransport() thrift.TTransport {
	return this.trans
}

func (this *IZlibTransport) GetInCompression() (int, error) {
	if !this.rinit {
		aux := []byte{}
		this.Read(aux)
	}
	return this.inCompression, nil
}

func (this *IZlibTransport) Reset() error {
	other, err := NewIZlibTransportWithLevel(this.trans, this.outCompression)
	if err != nil {
		return ierror.Raise(err)
	}
	*this = *other
	return nil
}
