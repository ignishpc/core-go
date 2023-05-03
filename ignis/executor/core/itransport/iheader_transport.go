package itransport

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
)

type IHeaderTransport struct {
	trans  thrift.TTransport
	header []byte
	pos    int
}

func NewIHeaderTransport(trans thrift.TTransport, header []byte) *IHeaderTransport {
	return &IHeaderTransport{trans, header, 0}
}

func (this *IHeaderTransport) Read(p []byte) (n int, err error) {
	if len(this.header) == this.pos {
		return this.trans.Read(p)
	}
	if len(p) > (len(this.header) - this.pos) {
		sz := len(this.header) - this.pos
		copy(p, this.header[this.pos:])
		this.pos = len(this.header)
		n, err = this.trans.Read(p[sz:])
		n += sz
	} else {
		copy(p, this.header[this.pos:this.pos+len(p)])
		this.pos += len(p)
		n = len(p)
	}
	return
}

func (this *IHeaderTransport) Write(p []byte) (n int, err error) {
	return this.trans.Write(p)
}

func (this *IHeaderTransport) Close() error {
	return this.trans.Close()
}

func (this *IHeaderTransport) Flush(ctx context.Context) (err error) {
	return this.trans.Flush(ctx)
}

func (this *IHeaderTransport) RemainingBytes() (num_bytes uint64) {
	return this.trans.RemainingBytes() + uint64(len(this.header)-this.pos)
}

func (this *IHeaderTransport) Open() error {
	return this.trans.Open()
}

func (this *IHeaderTransport) IsOpen() bool {
	return true
}
