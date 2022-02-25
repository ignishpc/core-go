package itransport

import (
	"bytes"
	"context"
	"github.com/apache/thrift/lib/go/thrift"
)

type IMemoryBuffer struct {
	*bytes.Buffer
	size int
}

type IMemoryBufferTransportFactory struct {
	size int
}

func (p *IMemoryBufferTransportFactory) GetTransport(trans thrift.TTransport) (thrift.TTransport, error) {
	if trans != nil {
		t, ok := trans.(*IMemoryBuffer)
		if ok && t.size > 0 {
			return NewIMemoryBufferLen(t.size), nil
		}
	}
	return NewIMemoryBufferLen(p.size), nil
}

func NewIMemoryBufferTransportFactory(size int) *IMemoryBufferTransportFactory {
	return &IMemoryBufferTransportFactory{size: size}
}

func NewIMemoryBuffer() *IMemoryBuffer {
	return &IMemoryBuffer{Buffer: &bytes.Buffer{}, size: 0}
}

func NewIMemoryBufferLen(size int) *IMemoryBuffer {
	buf := make([]byte, 0, size)
	return &IMemoryBuffer{Buffer: bytes.NewBuffer(buf), size: size}
}

func NewIMemoryBufferBytes(buf []byte) *IMemoryBuffer {
	return &IMemoryBuffer{Buffer: bytes.NewBuffer(buf), size: len(buf)}
}

func (p *IMemoryBuffer) IsOpen() bool {
	return true
}

func (p *IMemoryBuffer) Open() error {
	return nil
}

func (p *IMemoryBuffer) Close() error {
	return nil
}

func (p *IMemoryBuffer) Flush(ctx context.Context) error {
	return nil
}

func (p *IMemoryBuffer) RemainingBytes() (num_bytes uint64) {
	return uint64(p.Buffer.Len())
}
