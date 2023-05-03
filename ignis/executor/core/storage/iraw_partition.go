package storage

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"ignis/executor/core/utils"
	"reflect"
	"strings"
	"unsafe"
)

var HEADER int = 30

type IRawPartition[T any] struct {
	native        bool
	headerSize    int
	zlib          *itransport.IZlibTransport
	elems         int64
	compression   int8
	storageType   string
	readTransport func() (thrift.TTransport, error)
	transport     func() thrift.TTransport
	writeHeader   func() error
	sync          func() error
	clear         func() error
	header        IHeaderType
	isAny         bool
}

func newIRawPartition[T any](this *IRawPartition[T], trans thrift.TTransport, compression int8, native bool) (err error) {
	this.compression = compression
	this.native = native
	this.elems = 0
	this.zlib, err = itransport.NewIZlibTransportWithLevel(trans, int(compression))
	this.header, err = getHeaderAux(*new(T), native)
	if err != nil {
		return ierror.Raise(err)
	}
	this.isAny = utils.TypeName[T]() == utils.TypeName[any]()
	return nil
}

func (this *IRawPartition[T]) ReadIterator() (iterator.IReadIterator[T], error) {
	if err := this.Sync(); err != nil {
		return nil, ierror.Raise(err)
	}
	rtransport, err := this.readTransport()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	zlib_it, err := itransport.NewIZlibTransport(rtransport)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlib_it)
	if _, err = proto.ReadSerialization(); err != nil {
		return nil, ierror.Raise(err)
	}
	if _, err = getHeader(*new(T), proto, this.native); err != nil {
		return nil, ierror.Raise(err)
	}
	var reader iio.IReader
	if _, reader, _, err = this.header.read(proto); err != nil {
		return nil, ierror.Raise(err)
	}
	return &iRawReadIterator[T]{
		proto:  proto,
		elems:  this.elems,
		pos:    0,
		reader: reader,
	}, nil
}

func (this *IRawPartition[T]) WriteIterator() (iterator.IWriteIterator[T], error) {
	if this.headerSize == 0 {
		if err := this.writeHeader(); err != nil {
			return nil, ierror.Raise(err)
		}
	}
	rtransport, err := this.readTransport()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	zlib_it, err := itransport.NewIZlibTransport(rtransport)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlib_it)
	if _, err = proto.ReadSerialization(); err != nil {
		return nil, ierror.Raise(err)
	}
	_, err = getHeader(*new(T), proto, this.native)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	var writer iio.IWriter
	var init func(v T) (iio.IWriter, error)
	if this.isAny && this.elems == 0 {
		init = func(v T) (iio.IWriter, error) {
			this.header, err = getHeaderAux(v, this.native)
			this.headerSize = 0
			this.elems = 1
			it, err := this.WriteIterator()
			this.elems = 0
			rawIt := it.(*iRawWriteIterator[T])
			return rawIt.writer, err
		}
	} else {
		if _, _, writer, err = this.header.read(proto); err != nil {
			return nil, ierror.Raise(err)
		}
	}
	return &iRawWriteIterator[T]{
		proto:  iprotocol.NewIObjectProtocol(this.zlib),
		elems:  &this.elems,
		writer: wrapperAnyWriter(this.isAny, writer, this.native),
		init:   init,
	}, nil
}

func (this *IRawPartition[T]) Read(transport thrift.TTransport) error {
	zlib_in, err := itransport.NewIZlibTransport(transport)
	if err != nil {
		return ierror.Raise(err)
	}
	current_elems := this.elems
	compatible, reader, _, err := this.readHeader(zlib_in)
	if err != nil {
		return ierror.Raise(err)
	}
	if err = this.Sync(); err != nil {
		return ierror.Raise(err)
	}

	if compatible {
		bb := make([]byte, 256)
		for true {
			if n, err := zlib_in.Read(bb); err != nil {
				return ierror.Raise(err)
			} else {
				if n == 0 {
					break
				}
				if _, err = this.zlib.Write(bb[:n]); err != nil {
					return ierror.Raise(err)
				}
			}
		}
	} else {
		current_elems, this.elems = this.elems, current_elems
		it, err := this.WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		proto_buffer := iprotocol.NewIObjectProtocol(zlib_in)
		for this.elems < current_elems {
			elem, err := reader.Read(proto_buffer)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = it.Write(elem.(T)); err != nil {
				return ierror.Raise(err)
			}
		}
	}
	return nil
}
func (this *IRawPartition[T]) Write(transport thrift.TTransport, compression int8) error {
	return this.WriteWithNative(transport, compression, this.native)
}

func (this *IRawPartition[T]) WriteWithNative(transport thrift.TTransport, compression int8, native bool) error {
	if err := this.Sync(); err != nil {
		return ierror.Raise(err)
	}
	source, err := this.readTransport()
	if err != nil {
		return ierror.Raise(err)
	}
	if this.native == native {
		bb := make([]byte, 256)
		if compression == this.compression {
			for true {
				if n, err := source.Read(bb); err != nil {
					return ierror.Raise(err)
				} else {
					if n == 0 {
						break
					}
					if _, err = transport.Write(bb[:n]); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			if err = transport.Flush(context.Background()); err != nil {
				return ierror.Raise(err)
			}
		} else {
			zlib_in, err := itransport.NewIZlibTransport(source)
			if err != nil {
				return ierror.Raise(err)
			}
			zlib_out, err := itransport.NewIZlibTransportWithLevel(transport, int(compression))
			if err != nil {
				return ierror.Raise(err)
			}
			for true {
				if n, err := zlib_in.Read(bb); err != nil {
					return ierror.Raise(err)
				} else {
					if n == 0 {
						break
					}
					if _, err = zlib_out.Write(bb[:n]); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			if err = zlib_out.Flush(context.Background()); err != nil {
				return ierror.Raise(err)
			}
		}
	} else {
		zlib_out, err := itransport.NewIZlibTransportWithLevel(transport, int(compression))
		if err != nil {
			return ierror.Raise(err)
		}
		proto_out := iprotocol.NewIObjectProtocol(zlib_out)
		if err = proto_out.WriteSerialization(this.native); err != nil {
			return ierror.Raise(err)
		}
		zlib_in, err := itransport.NewIZlibTransport(source)
		if err != nil {
			return ierror.Raise(err)
		}
		proto_in := iprotocol.NewIObjectProtocol(zlib_in)
		if _, err := proto_in.ReadSerialization(); err != nil {
			return ierror.Raise(err)
		}
		if this.elems > 0 {
			if !this.native {
				it, err := this.ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				elem := this.First()
				header, err := getHeader(elem, proto_in, false)
				if err != nil {
					return ierror.Raise(err)
				}
				_, _, writer, err := header.read(proto_in)
				writer = wrapperAnyWriter(this.isAny, writer, this.native)
				if err != nil {
					return ierror.Raise(err)
				}
				rtp := reflect.TypeOf(new(T)).Elem()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = header.write(proto_out, this.elems); err != nil {
					return ierror.Raise(err)
				}
				for it.HasNext() {
					if v, err := it.Next(); err != nil {
						return ierror.Raise(err)
					} else {
						if err = writer.Write(proto_out, rtp, unsafe.Pointer(&v)); err != nil {
							return ierror.Raise(err)
						}
					}
				}
			} else {
				var ref any
				if utils.TypeName[T]() == utils.TypeName[any]() {
					ref = this.First()
				} else {
					ref = *new(T)
				}
				header, err := getHeader(ref, proto_in, true)
				if err != nil {
					return ierror.Raise(err)
				}
				if err = header.write(proto_out, this.elems); err != nil {
					return ierror.Raise(err)
				}
				writer, err := iio.GetNativeWriter(iio.GetName(ref))
				writer = wrapperAnyWriter(this.isAny, writer, this.native)
				if err != nil {
					return ierror.Raise(err)
				}
				it, err := this.ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				rtp := reflect.TypeOf(new(T)).Elem()
				for it.HasNext() {
					e, err := it.Next()
					if err != nil {
						return ierror.Raise(err)
					}
					if err := writer.Write(proto_out, rtp, unsafe.Pointer(&e)); err != nil {
						return ierror.Raise(err)
					}
				}
			}
		} else {
			header, err := getHeader(*new(T), proto_in, this.native)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = header.write(proto_out, 0); err != nil {
				return ierror.Raise(err)
			}
		}
	}
	return nil
}

func (this *IRawPartition[T]) CopyFrom(source IPartitionBase) error {
	if (source.Type() == "Disk" || source.Type() == "RawMemory") && source.Native() == this.Native() {
		if err := source.Sync(); err != nil {
			return ierror.Raise(err)
		}
		if err := this.Sync(); err != nil {
			return ierror.Raise(err)
		}
		raw_source := source.(*IRawPartition[T])
		bb := make([]byte, 256)
		// Direct copy
		if this.compression == raw_source.compression {
			this.elems += raw_source.elems
			source_buffer, err := raw_source.readTransport()
			if err != nil {
				return ierror.Raise(err)
			}
			if _, err := source_buffer.Read(bb[:raw_source.headerSize]); err != nil { //skip header
				return ierror.Raise(err)
			}
			target := this.transport()
			for true {
				if n, err := source_buffer.Read(bb); err != nil {
					return ierror.Raise(err)
				} else {
					if n == 0 {
						break
					}
					if _, err = target.Write(bb[:n]); err != nil {
						return ierror.Raise(err)
					}
				}
			}
		} else {
			// Read header to initialize zlib
			source_buffer, err := raw_source.readTransport()
			if err != nil {
				return ierror.Raise(err)
			}
			source_zlib, err := itransport.NewIZlibTransport(source_buffer)
			if err != nil {
				return ierror.Raise(err)
			}
			source_proto := iprotocol.NewIObjectProtocol(source_zlib)
			_, err = source_proto.ReadSerialization()
			if err != nil {
				return ierror.Raise(err)
			}
			if _, _, _, err = this.readHeader(source_zlib); err != nil {
				return ierror.Raise(err)
			}
			for true {
				if n, err := source_zlib.Read(bb); err != nil {
					return ierror.Raise(err)
				} else {
					if n == 0 {
						break
					}
					if _, err = this.zlib.Write(bb[:n]); err != nil {
						return ierror.Raise(err)
					}
				}
			}
		}
	} else {
		writer, err := this.WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		reader, err := this.ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		for reader.HasNext() {
			elem, err := reader.Next()
			if err != nil {
				return ierror.Raise(err)
			}
			if err = writer.Write(elem); err != nil {
				return ierror.Raise(err)
			}
		}
	}
	return nil
}

func (this *IRawPartition[T]) CopyTo(target IPartitionBase) error {
	return target.CopyFrom(this)
}

func (this *IRawPartition[T]) MoveFrom(source IPartitionBase) error {
	if err := this.CopyFrom(source); err != nil {
		return ierror.Raise(err)
	}
	if err := source.Clear(); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IRawPartition[T]) MoveTo(target IPartitionBase) error {
	return target.MoveFrom(this)
}

func (this *IRawPartition[T]) Size() int64 {
	return this.elems
}

func (this *IRawPartition[T]) Empty() bool {
	return this.elems == 0
}

func (this *IRawPartition[T]) Clear() error {
	this.elems = 0
	return this.clear()
}

func (this *IRawPartition[T]) Sync() error {
	if this.elems > 0 {
		if err := this.zlib.Flush(context.Background()); err != nil {
			return ierror.Raise(err)
		}
	}
	if err := this.sync(); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IRawPartition[T]) Native() bool {
	return this.native
}

func (this *IRawPartition[T]) Compression() int8 {
	return this.compression
}

func (this *IRawPartition[T]) First() any {
	it, err := this.ReadIterator()
	if err == nil {
		elem, err := it.Next()
		if err == nil {
			return elem
		}
	}
	return nil
}

func (this *IRawPartition[T]) Clone() (IPartitionBase, error) {
	panic(ierror.RaiseMsg("Not implemented in IRawPartition"))
}

func (this *IRawPartition[T]) Bytes() int64 {
	panic(ierror.RaiseMsg("Not implemented in IRawPartition"))
}

func (this *IRawPartition[T]) Fit() error {
	panic(ierror.RaiseMsg("Not implemented in IRawPartition"))
}

func (this *IRawPartition[T]) Type() string {
	return this.storageType
}

func (this *IRawPartition[T]) Inner() any {
	panic(ierror.RaiseMsg("Not implemented in IRawPartition"))
}

func DisableNative[T any](this IPartitionBase) {
	this.(*IRawPartition[T]).native = false
}

func (this *IRawPartition[T]) readHeader(transport thrift.TTransport) (bool, iio.IReader, iio.IWriter, error) {
	proto := iprotocol.NewIObjectProtocol(transport)
	native, err := proto.ReadSerialization()
	if err != nil {
		return false, nil, nil, ierror.Raise(err)
	}
	compatible := this.native == native
	header, err := getHeader(*new(T), proto, native)
	if err != nil {
		return false, nil, nil, ierror.Raise(err)
	}
	elem, reader, writer, err := header.read(proto)
	if err != nil {
		return false, nil, nil, ierror.Raise(err)
	}
	if this.elems > 0 && !this.native && compatible {
		if err = this.Sync(); err != nil {
			return false, nil, nil, ierror.Raise(err)
		}
		rtransport, err := this.readTransport()
		if err != nil {
			return false, nil, nil, ierror.Raise(err)
		}
		zlib_it, err := itransport.NewIZlibTransport(rtransport)
		if err != nil {
			return false, nil, nil, ierror.Raise(err)
		}
		proto2 := iprotocol.NewIObjectProtocol(zlib_it)
		_, err = proto2.ReadSerialization()
		if err != nil {
			return false, nil, nil, ierror.Raise(err)
		}
		header, err := getHeader(*new(T), proto2, this.native)
		if err != nil {
			return false, nil, nil, ierror.Raise(err)
		}
		var writer2 iio.IWriter
		if _, _, writer2, err = header.read(proto2); err != nil {
			return false, nil, nil, ierror.Raise(err)
		}
		if writer.Type() != writer2.Type() {
			return false, nil, nil, ierror.RaiseMsg("Ignis serialization does not support heterogeneous basic types")
		}
	} else if compatible {
		this.header = header
	}
	this.elems += elem
	return compatible, reader, writer, nil
}

type iRawReadIterator[T any] struct {
	proto      thrift.TProtocol
	pos, elems int64
	reader     iio.IReader
}

func (this *iRawReadIterator[T]) HasNext() bool {
	return this.pos < this.elems
}

func (this *iRawReadIterator[T]) Next() (T, error) {
	t, err := this.reader.Read(this.proto)
	this.pos++
	return t.(T), err
}

type iRawWriteIterator[T any] struct {
	proto  thrift.TProtocol
	elems  *int64
	writer iio.IWriter
	rtp    reflect.Type
	init   func(v T) (iio.IWriter, error)
}

func (this *iRawWriteIterator[T]) Write(v T) error {
	if this.rtp == nil {
		this.rtp = reflect.TypeOf(v)
		if this.writer == nil {
			var err error
			if this.writer, err = this.init(v); err != nil {
				return err
			}
		}
	}
	*this.elems++
	return this.writer.Write(this.proto, this.rtp, unsafe.Pointer(&v))
}

type IHeaderType interface {
	read(protocol thrift.TProtocol) (int64, iio.IReader, iio.IWriter, error)
	write(protocol thrift.TProtocol, elems int64) error
}

type IHeaderTypeArray struct {
	w iio.IWriter
}

func (this *IHeaderTypeArray) read(protocol thrift.TProtocol) (int64, iio.IReader, iio.IWriter, error) {
	n, err := iio.ReadSizeAux(protocol)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	tp, err := iio.ReadTypeAux(protocol)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	reader, err := iio.GetReader(tp)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	if this.w.Type() == iio.I_VOID {
		this.w, err = iio.GetWriter(iio.GetName(reader.Empty()))
	}
	return n, reader, this.w, err
}

func (this *IHeaderTypeArray) write(protocol thrift.TProtocol, elems int64) error {
	if err := iio.WriteTypeAux(protocol, iio.I_LIST); err != nil {
		return ierror.Raise(err)
	}
	if err := iio.WriteSizeAux(protocol, elems); err != nil {
		return ierror.Raise(err)
	}
	if err := this.w.WriteType(protocol); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

type IHeaderTypePairArray struct {
	tp1, tp2 int8
	ref      ipair.IAbstractPair
}

type rawPairReader struct {
	ref    ipair.IAbstractPair
	r1, r2 iio.IReader
}

func (this *rawPairReader) Read(protocol thrift.TProtocol) (any, error) {
	var first, second any
	var err error
	if first, err = this.r1.Read(protocol); err != nil {
		return nil, err
	}
	if second, err = this.r2.Read(protocol); err != nil {
		return nil, err
	}
	return this.ref.New(first, second).Get(), nil
}

func (this *rawPairReader) Empty() any {
	return this.ref.NewEmpty().Get()
}

type rawPairWriter struct {
	tp1, tp2 reflect.Type
	w1, w2   iio.IWriter
}

func (this *rawPairWriter) Write(protocol thrift.TProtocol, rtp reflect.Type, obj unsafe.Pointer) error {
	p := (reflect.NewAt(rtp, obj).Interface()).(ipair.IAbstractPair)
	err := this.w1.Write(protocol, this.tp1, p.GetFirstPointer())
	if err != nil {
		return ierror.Raise(err)
	}
	err = this.w2.Write(protocol, this.tp2, p.GetSecondPointer())
	if err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *rawPairWriter) WriteType(protocol thrift.TProtocol) error {
	if err := iio.WriteTypeAux(protocol, this.Type()); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *rawPairWriter) Type() int8 {
	return iio.I_PAIR
}

func (this *IHeaderTypePairArray) read(protocol thrift.TProtocol) (int64, iio.IReader, iio.IWriter, error) {
	n, err := iio.ReadSizeAux(protocol)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	t1, err := iio.ReadTypeAux(protocol)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	t2, err := iio.ReadTypeAux(protocol)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	r1, err := iio.GetReader(t1)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	r2, err := iio.GetReader(t2)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	w1, err := iio.GetWriter(iio.GetName(this.ref.GetFirst()))
	tp1 := reflect.TypeOf(this.ref.GetFirst())
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	w2, err := iio.GetWriter(iio.GetName(this.ref.GetSecond()))
	tp2 := reflect.TypeOf(this.ref.GetSecond())
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	return n, &rawPairReader{this.ref, r1, r2}, &rawPairWriter{tp1, tp2, w1, w2}, nil
}

func (this *IHeaderTypePairArray) write(protocol thrift.TProtocol, elems int64) error {
	if err := iio.WriteTypeAux(protocol, iio.I_PAIR_LIST); err != nil {
		return ierror.Raise(err)
	}
	if err := iio.WriteSizeAux(protocol, elems); err != nil {
		return ierror.Raise(err)
	}
	if err := iio.WriteTypeAux(protocol, this.tp1); err != nil {
		return ierror.Raise(err)
	}
	if err := iio.WriteTypeAux(protocol, this.tp2); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

type IHeaderTypeBinary struct {
}

func (this *IHeaderTypeBinary) read(protocol thrift.TProtocol) (int64, iio.IReader, iio.IWriter, error) {
	n, err := iio.ReadSizeAux(protocol)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	reader, err := iio.GetReader(iio.I_BINARY)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	writer, err := iio.GetWriter(iio.GetName(reader.Empty()))
	return n, reader, writer, err
}

func (this *IHeaderTypeBinary) write(protocol thrift.TProtocol, elems int64) error {
	if err := iio.WriteTypeAux(protocol, iio.I_BINARY); err != nil {
		return ierror.Raise(err)
	}
	if err := iio.WriteSizeAux(protocol, elems); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

type IHeaderTypeNative struct {
	name string
}

func (this *IHeaderTypeNative) read(protocol thrift.TProtocol) (int64, iio.IReader, iio.IWriter, error) {
	isArray, err := protocol.ReadBool(context.Background())
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	var id string
	if id, err = protocol.ReadString(context.Background()); err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	n := int64(1)
	reader, err := iio.GetNativeReader(id)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	if isArray {
		n, err = iio.ReadSizeAux(protocol)
		if err != nil {
			return 0, nil, nil, ierror.Raise(err)
		}
	}
	writer, err := iio.GetNativeWriter(this.name)
	if err != nil {
		return 0, nil, nil, ierror.Raise(err)
	}
	return n, reader, writer, err
}

func (this *IHeaderTypeNative) write(protocol thrift.TProtocol, elems int64) error {
	if err := protocol.WriteBool(context.Background(), true); err != nil {
		return ierror.Raise(err)
	}
	if err := protocol.WriteString(context.Background(), this.name); err != nil {
		return ierror.Raise(err)
	}
	return iio.WriteSizeAux(protocol, elems)
}

func getHeaderAux(ref any, native bool) (IHeaderType, error) {
	if native {
		return &IHeaderTypeNative{iio.GetName(ref)}, nil

	} else {
		name := iio.GetName(ref)
		binaryName := utils.TypeName[byte]()
		pairName := utils.TypeName[ipair.IPair[any, any]]()
		pairName = pairName[:strings.Index(pairName, "[")]
		if name == binaryName {
			return &IHeaderTypeBinary{}, nil
		} else if strings.HasPrefix(name, pairName) {
			p := reflect.New(reflect.TypeOf(ref)).Interface().(ipair.IAbstractPair)
			w1, err1 := iio.GetWriter(p.GetFirstType().String())
			if err1 != nil {
				return nil, ierror.Raise(err1)
			}
			w2, err2 := iio.GetWriter(p.GetSecondType().String())
			if err2 != nil {
				return nil, ierror.Raise(err2)
			}
			return &IHeaderTypePairArray{w1.Type(), w2.Type(), p}, nil
		} else {
			writer, err := iio.GetWriter(name)
			return &IHeaderTypeArray{writer}, err
		}
	}
}

func getHeader(ref any, proto thrift.TProtocol, native bool) (IHeaderType, error) {
	if native {
		return getHeaderAux(ref, native)
	}
	_, err := iio.ReadTypeAux(proto)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return getHeaderAux(ref, native)
}

func wrapperAnyWriter(any bool, w iio.IWriter, native bool) iio.IWriter {
	if any && w != nil { //Avoid any when unwrapping is possible
		return iio.AnyWrapper(w)
	}
	return w
}
