package storage

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
)

const IRawMemoryPartitionType = "RawMemory"

type IRawMemoryPartition[T any] struct {
	IRawPartition[T]
	buffer *itransport.IMemoryBuffer
}

func NewIRawMemoryPartition[T any](bytes int64, compression int8, native bool) (*IRawMemoryPartition[T], error) {
	buffer := itransport.NewIMemoryBufferWithSize(bytes + int64(HEADER))
	this := &IRawMemoryPartition[T]{
		buffer: buffer,
	}
	this.storageType = IRawMemoryPartitionType
	this.readTransport = this.readTransport_
	this.transport = this.transport_
	this.writeHeader = this.writeHeader_
	this.sync = this.sync_
	this.clear = this.clear_
	if err := newIRawPartition[T](&this.IRawPartition, buffer, compression, native); err != nil {
		return nil, ierror.Raise(err)
	}
	return this, nil
}

func ConvIRawMemoryPartition[T any](other IPartitionBase) (IPartitionBase, error) {
	raw, err := NewIRawMemoryPartition[T](other.Bytes(), other.Compression(), other.Native())
	if err != nil {
		return nil, ierror.Raise(err)
	}
	if err := other.Sync(); err != nil {
		return nil, ierror.Raise(err)
	}
	raw.buffer = other.Inner().(*itransport.IMemoryBuffer)
	raw.zlib, err = itransport.NewIZlibTransportWithLevel(raw.buffer, int(other.Compression()))
	if err != nil {
		return nil, ierror.Raise(err)
	}
	rtrans, err := raw.readTransport()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	zlib, err := itransport.NewIZlibTransport(rtrans)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	_, _, _, err = raw.readHeader(zlib)
	return raw, err
}

func (this *IRawMemoryPartition[T]) Clone() (IPartitionBase, error) {
	newPartition, err := NewIRawMemoryPartition[T](this.Bytes(), this.compression, this.Native())
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return newPartition, this.CopyTo(newPartition)
}

func (this *IRawMemoryPartition[T]) Bytes() int64 {
	sz := this.buffer.GetBufferSize() - int64(HEADER+this.headerSize)
	if sz > 0 {
		return sz
	}
	return 0
}

func (this *IRawMemoryPartition[T]) clear_() error {
	this.buffer.ResetBuffer()
	return this.Sync()
}

func (this *IRawMemoryPartition[T]) Fit() error {
	return this.buffer.SetBufferSize(this.buffer.GetBufferSize())
}

func (this *IRawMemoryPartition[T]) sync_() error {
	return this.writeHeader()
}

func (this *IRawMemoryPartition[T]) readTransport_() (thrift.TTransport, error) {
	init := HEADER - this.headerSize
	rbuffer := itransport.NewIMemoryBufferWrapper(this.buffer.GetBuffer(), this.buffer.WriteEnd(), itransport.OBSERVE)
	return rbuffer, rbuffer.SetReadBuffer(int64(init))
}

func (this *IRawMemoryPartition[T]) writeHeader_() error {
	proto := iprotocol.NewIObjectProtocol(this.zlib)
	if err := this.zlib.Reset(); err != nil {
		return ierror.Raise(err)
	}
	write_pos := this.buffer.WriteEnd()
	this.buffer.ResetBuffer()

	if err := proto.WriteSerialization(this.native); err != nil {
		return ierror.Raise(err)
	}
	if err := this.header.write(proto, this.elems); err != nil {
		return ierror.Raise(err)
	}
	if err := this.zlib.Flush(context.Background()); err != nil {
		return ierror.Raise(err)
	}
	// Align header to the left
	this.headerSize = int(this.buffer.WriteEnd())
	header := make([]byte, this.headerSize)
	if _, err := this.buffer.Read(header); err != nil {
		return ierror.Raise(err)
	}
	if err := this.buffer.SetReadBuffer(0); err != nil {
		return ierror.Raise(err)
	}
	if err := this.buffer.SetWriteBuffer(int64(HEADER - this.headerSize)); err != nil {
		return ierror.Raise(err)
	}
	if _, err := this.buffer.Write(header); err != nil {
		return ierror.Raise(err)
	}
	var err error
	if write_pos > int64(HEADER) {
		err = this.buffer.SetWriteBuffer(write_pos)
	} else {
		err = this.buffer.SetWriteBuffer(int64(HEADER))
	}
	return err
}

func (this *IRawMemoryPartition[T]) transport_() thrift.TTransport {
	return this.buffer
}

func (this *IRawMemoryPartition[T]) Inner() any {
	return this.transport_()
}
