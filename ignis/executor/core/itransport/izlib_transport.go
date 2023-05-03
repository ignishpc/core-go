package itransport

import (
	"compress/flate"
	"compress/zlib"
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"hash"
	"ignis/executor/core/ierror"
	"io"
	"unsafe"
)

type innerZlib struct {
	reader    io.ReadCloser
	transport thrift.TTransport
	writer    *zlib.Writer
}

func newInnerZlib(trans thrift.TTransport, level int) (*innerZlib, error) {
	w, err := zlib.NewWriterLevel(trans, level)
	if err != nil {
		return nil, err
	}

	return &innerZlib{
		writer:    w,
		transport: trans,
	}, nil
}

func (z *innerZlib) Close() error {
	if z.reader != nil {
		if err := z.reader.Close(); err != nil {
			return err
		}
	}
	if err := z.writer.Close(); err != nil {
		return err
	}
	return z.transport.Close()
}

type hide struct {
	w           io.Writer
	level       int
	dict        []byte
	compressor  *flate.Writer
	digest      hash.Hash32
	err         error
	scratch     [4]byte
	wroteHeader bool
}

func (z *innerZlib) Flush(ctx context.Context) error {
	if err := z.writer.Flush(); err != nil {
		return err
	}
	z.writer.Reset(z.transport)
	(*hide)(unsafe.Pointer(z.writer)).wroteHeader = true
	return z.transport.Flush(ctx)
}

func (z *innerZlib) IsOpen() bool {
	return z.transport.IsOpen()
}

func (z *innerZlib) Open() error {
	return z.transport.Open()
}

func (z *innerZlib) Read(p []byte) (int, error) {
	if z.reader == nil {
		r, err := zlib.NewReader(z.transport)
		if err != nil {
			return 0, err
		}
		z.reader = r
	}
	n, err := z.reader.Read(p)
	if err != nil {
		if err == io.ErrNoProgress || err == io.ErrUnexpectedEOF {
			return n, nil
		}
	}

	return n, err
}

func (z *innerZlib) RemainingBytes() uint64 {
	return z.transport.RemainingBytes()
}

func (z *innerZlib) Write(p []byte) (int, error) {
	return z.writer.Write(p)
}

type IZlibTransport struct {
	*innerZlib
	winit          bool
	rinit          bool
	flushed        bool
	inCompression  int
	outCompression int
}

func NewIZlibTransport(trans thrift.TTransport) (*IZlibTransport, error) {
	return NewIZlibTransportWithLevel(trans, 6)
}

func NewIZlibTransportWithLevel(trans thrift.TTransport, level int) (*IZlibTransport, error) {
	inner, err := newInnerZlib(trans, level)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return &IZlibTransport{inner, false, false, false, 0, level}, nil
}

func (this *IZlibTransport) Read(p []byte) (int, error) {
	if !this.rinit {
		aux := []byte{0}
		if _, err := this.transport.Read(aux); err != nil {
			return 0, err
		}
		this.inCompression = int(aux[0])
		this.rinit = true
	}
	if this.inCompression > 0 {
		return this.innerZlib.Read(p)
	} else {
		return this.transport.Read(p)
	}
}

func (this *IZlibTransport) Write(p []byte) (int, error) {
	if !this.winit {
		aux := []byte{byte(this.outCompression)}
		if _, err := this.transport.Write(aux); err != nil {
			return 0, err
		}
		this.winit = true
	}
	if this.outCompression > 0 {
		this.flushed = false
		return this.innerZlib.Write(p)
	} else {
		return this.transport.Write(p)
	}
}

func (this *IZlibTransport) Flush(ctx context.Context) error {
	if !this.winit {
		aux := []byte{}
		if _, err := this.transport.Write(aux); err != nil {
			return err
		}
	} else if this.outCompression == 0 {
		return this.transport.Flush(ctx)
	}
	if this.flushed {
		return nil
	}
	this.flushed = true
	return this.innerZlib.Flush(ctx)
}

func (this *IZlibTransport) GetTransport() thrift.TTransport {
	return this.transport
}

func (this *IZlibTransport) GetInCompression() (int, error) {
	if !this.rinit {
		aux := []byte{}
		if _, err := this.Read(aux); err != nil {
			return 0, err
		}
	}
	return this.inCompression, nil
}

func (this *IZlibTransport) Reset() error {
	inner, err := newInnerZlib(this.transport, this.outCompression)
	if err != nil {
		return ierror.Raise(err)
	}
	this.innerZlib = inner
	this.rinit = false
	this.winit = false
	this.flushed = false
	return nil
}
