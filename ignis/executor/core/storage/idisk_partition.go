package storage

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"ignis/executor/core/utils"
	"io"
	"os"
	"runtime"
	"strconv"
)

const IDiskPartitionType = "Disk"

type IDiskPreservation interface {
	Persist(p bool)
	GetPath() string
	GetTypeName() string
}

type IDiskPartition[T any] struct {
	IRawPartition[T]
	file        *fileTransport
	path        string
	headerCache []byte
	destroy     bool
}

type fileTransport struct {
	*os.File
}

func (f fileTransport) Flush(ctx context.Context) (err error) {
	return f.Sync()
}

func (f fileTransport) RemainingBytes() (num_bytes uint64) {
	const maxSize = ^uint64(0)
	return maxSize
}

func (f fileTransport) Open() error {
	return nil
}

func (f fileTransport) IsOpen() bool {
	return true
}

func (f fileTransport) Read(b []byte) (n int, err error) {
	n, err = f.File.Read(b)
	if err != io.EOF {
		return n, err
	}
	return n, nil
}

func NewIDiskPartition[T any](path string, compression int8, native bool, persist bool, read bool) (*IDiskPartition[T], error) {
	var err error
	this := &IDiskPartition[T]{
		path:    path,
		destroy: !persist,
	}
	this.storageType = IDiskPartitionType
	this.readTransport = this.readTransport_
	this.transport = this.transport_
	this.writeHeader = this.writeHeader_
	this.sync = this.sync_
	this.clear = this.clear_
	this.file = &fileTransport{}
	this.file.File, err = os.OpenFile("/tmp/null", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	if err = newIRawPartition[T](&this.IRawPartition, this.file, compression, native); err != nil {
		return nil, ierror.Raise(err)
	}
	if _, err = this.zlib.Write([]byte{0}); err != nil {
		return nil, ierror.Raise(err)
	}
	if err = this.zlib.Flush(context.Background()); err != nil {
		return nil, ierror.Raise(err)
	}
	_ = this.file.File.Close()
	this.file.File, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, ierror.Raise(err)
	}

	runtime.SetFinalizer(this, func(disk *IDiskPartition[T]) {
		if disk.destroy {
			_ = os.Remove(this.path)
		}
	})

	if read {
		file, err := os.Open(this.path + ".header")
		defer file.Close()
		zlib_header, err := itransport.NewIZlibTransport(&fileTransport{file})
		if err != nil {
			return nil, ierror.Raise(err)
		}
		compatible, _, _, err := this.readHeader(zlib_header)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		if !compatible {
			this.native = !this.native
		}
		return this, nil
	} else {
		return this, this.Clear()
	}
}

func ConvIDiskPartition[T any](other IPartitionBase) (IPartitionBase, error) {
	info := other.Inner().([]any)
	disk, err := NewIDiskPartition[T](info[0].(string), other.Compression(), other.Native(), info[2].(bool), true)
	other.(IDiskPreservation).Persist(true)
	return disk, err
}

func (this *IDiskPartition[T]) Clone() (IPartitionBase, error) {
	newPath := this.path
	i := 0
	for true {
		if _, err := os.Stat(newPath + "." + strconv.Itoa(i)); err == nil {
			break
		}
	}
	newPath += "." + strconv.Itoa(i)
	newPartition, err := NewIDiskPartition[T](newPath, this.compression, this.native, false, false)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return newPartition, this.CopyTo(newPartition)
}

func (this *IDiskPartition[T]) clear_() error {
	if err := this.zlib.Flush(context.Background()); err != nil {
		return ierror.Raise(err)
	}
	if err := this.file.Truncate(0); err != nil {
		return ierror.Raise(err)
	}
	if _, err := this.file.Seek(0, 0); err != nil {
		return ierror.Raise(err)
	}
	if err := this.Sync(); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IDiskPartition[T]) Fit() error {
	return nil
}

func (this *IDiskPartition[T]) Bytes() int64 {
	offset, err := this.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 1024 * 1024
	}
	return offset + int64(this.headerSize)
}

func (this *IDiskPartition[T]) sync_() error {
	if err := this.writeHeader_(); err != nil {
		return ierror.Raise(err)
	}
	if err := os.WriteFile(this.path+".header", this.headerCache, 0644); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func (this *IDiskPartition[T]) Persist(p bool) {
	this.destroy = !p
}

func (this *IDiskPartition[T]) GetPath() string {
	return this.path
}

func (this *IDiskPartition[T]) GetTypeName() string {
	return utils.TypeName[T]()
}

func (this *IDiskPartition[T]) Rename(newPath string) (err error) {
	_ = this.file.Close()
	err2 := os.Rename(this.path, newPath)
	if _, err := os.Stat(this.path + ".header"); err == nil {
		_ = os.Rename(this.path+".header", newPath+".header")
	}
	if err2 == nil {
		this.path = newPath
	} else {
		err2 = ierror.RaiseMsg("error: " + newPath + " is not a valid storage name, " + err2.Error())
	}
	this.file.File, err = os.OpenFile(this.path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return ierror.Raise(err)
	}
	err = this.Sync()
	if err != nil {
		return ierror.Raise(err)
	}
	return err2
}

func (this *IDiskPartition[T]) transport_() thrift.TTransport {
	return this.file
}

func (this *IDiskPartition[T]) Inner() any {
	return []any{this.path, this.file, this.destroy}
}

func (this *IDiskPartition[T]) writeHeader_() error {
	buffer := itransport.NewIMemoryBufferWithSize(int64(HEADER))
	zlib_buffer, err := itransport.NewIZlibTransportWithLevel(buffer, int(this.compression))
	if err != nil {
		return ierror.Raise(err)
	}
	proto := iprotocol.NewIObjectProtocol(zlib_buffer)
	if err := proto.WriteSerialization(this.native); err != nil {
		return ierror.Raise(err)
	}
	if err := this.header.write(proto, this.elems); err != nil {
		return ierror.Raise(err)
	}
	if err := zlib_buffer.Flush(context.Background()); err != nil {
		return ierror.Raise(err)
	}
	this.headerCache = buffer.GetBufferAsBytes()
	this.headerSize = len(this.headerCache)
	return nil
}

func (this *IDiskPartition[T]) readTransport_() (thrift.TTransport, error) {
	file, err := os.Open(this.path)
	htrans := itransport.NewIHeaderTransport(&fileTransport{file}, this.headerCache)
	runtime.SetFinalizer(htrans, func(trans *itransport.IHeaderTransport) {
		trans.Close()
	})
	return htrans, err
}
