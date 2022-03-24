package itransport

import (
	"context"
	"ignis/executor/core/ierror"
	"ignis/executor/core/utils"
)

type MemoryPolicy int

const MemoryDefaultSize = 1024

const (
	OBSERVE        MemoryPolicy = 1
	COPY                        = 2
	TAKE_OWNERSHIP              = 3
)

type IMemoryBuffer struct {
	buffer        []byte
	maxBufferSize int64
	owner         bool
	rBase         int64
	wBase         int64
}

func NewIMemoryBuffer() *IMemoryBuffer {
	return NewIMemoryBufferWithSize(MemoryDefaultSize)
}

func NewIMemoryBufferWithSize(sz int64) *IMemoryBuffer {
	return NewIMemoryBufferWrapper(nil, sz, TAKE_OWNERSHIP)
}

func NewIMemoryBufferWrapper(buf []byte, sz int64, policy MemoryPolicy) *IMemoryBuffer {
	this := &IMemoryBuffer{}
	if buf == nil {
		this.initCommon(nil, sz, true, 0)
	} else if policy == OBSERVE || policy == TAKE_OWNERSHIP {
		this.initCommon(buf, sz, policy == TAKE_OWNERSHIP, sz)
	} else {
		this.initCommon(nil, sz, true, 0)
		_, _ = this.Write(buf)
	}
	return this
}

func (this *IMemoryBuffer) initCommon(buf []byte, size int64, owner bool, wPos int64) {
	this.maxBufferSize = int64((^uint(0)) >> 1)

	if buf == nil {
		buf = make([]byte, int(size), int(size))
	}
	this.buffer = buf
	this.rBase = 0
	this.wBase = wPos
	this.owner = owner
}

func (this *IMemoryBuffer) ensureCanWrite(sz int64) error {
	// Check available space
	avail := this.AvailableWrite()
	if sz < avail {
		return nil
	}

	if !this.owner {
		return ierror.RaiseMsg("Insufficient space in external MemoryBuffer")
	}

	//Grow the buffer as necessary.
	newSize := utils.Max(int64(len(this.buffer)), 1)
	for sz > avail {
		if newSize > this.maxBufferSize/2 {
			if this.AvailableWrite()+this.maxBufferSize-int64(len(this.buffer)) < sz {
				return ierror.RaiseMsg("Internal buffer size overflow")
			}
			newSize = this.maxBufferSize
		}
		newSize = newSize * 2
		avail = this.AvailableWrite() + (newSize - int64(len(this.buffer)))
	}
	return this.SetBufferSize(newSize)
}

func (this *IMemoryBuffer) IsOpen() bool {
	return true
}

func (this *IMemoryBuffer) Peek() bool {
	return this.ReadEnd() < this.WriteEnd()
}

func (this *IMemoryBuffer) Open() error {
	return nil
}

func (this *IMemoryBuffer) Close() error {
	return nil
}

func (this *IMemoryBuffer) GetBuffer() []byte {
	return this.buffer
}

func (this *IMemoryBuffer) Bytes() []byte {
	return this.buffer[0:this.wBase]
}

func (this *IMemoryBuffer) ResetBuffer() {
	this.rBase = 0
	if this.owner {
		this.wBase = 0
	}
}

func (this *IMemoryBuffer) ReadEnd() int64 {
	return this.rBase
}

func (this *IMemoryBuffer) WriteEnd() int64 {
	return this.wBase
}

func (this *IMemoryBuffer) AvailableRead() int64 {
	return this.WriteEnd() - this.ReadEnd()
}

func (this *IMemoryBuffer) AvailableWrite() int64 {
	return this.GetBufferSize() - this.WriteEnd()
}

func (this *IMemoryBuffer) GetWritePtr(len int64) ([]byte, error) {
	if err := this.ensureCanWrite(len); err != nil {
		return nil, err
	}
	return this.buffer[this.wBase:], nil
}

func (this *IMemoryBuffer) WroteBytes(len int64) error {
	if err := this.ensureCanWrite(len); err != nil {
		return err
	}
	this.wBase += len
	return nil
}

func (this *IMemoryBuffer) GetBufferSize() int64 {
	return int64(len(this.buffer))
}

func (this *IMemoryBuffer) GetMaxBufferSize() int64 {
	return this.maxBufferSize
}

func (this *IMemoryBuffer) SetMaxBufferSize(maxSize int64) error {
	if maxSize < this.maxBufferSize {
		return ierror.RaiseMsg("Maximum buffer size would be less than current buffer size")
	}
	this.maxBufferSize = maxSize
	return nil
}

func (this *IMemoryBuffer) SetBufferSize(newSize int64) error {
	if !this.owner {
		return ierror.RaiseMsg("resize in buffer we don't own")
	}
	if newSize > this.maxBufferSize {
		return ierror.RaiseMsg("resize more than Maximum buffer size")
	}
	newBuffer := make([]byte, int(newSize), int(newSize))
	copy(newBuffer, this.buffer)
	this.buffer = newBuffer
	this.rBase = utils.Min(this.rBase, newSize)
	this.wBase = utils.Min(this.wBase, newSize)
	return nil
}

func (this *IMemoryBuffer) Write(p []byte) (int, error) {
	if err := this.ensureCanWrite(int64(len(p))); err != nil {
		return 0, err
	}
	copy(this.buffer[this.wBase:], p)
	this.wBase += int64(len(p))
	return len(p), nil
}

func (this *IMemoryBuffer) Read(p []byte) (int, error) {
	// Decide how much to give
	give := utils.Min(int64(len(p)), this.AvailableRead())

	oldRBase := this.rBase
	this.rBase = oldRBase + give
	copy(p, this.buffer[oldRBase:this.rBase])
	return int(give), nil
}

func (this *IMemoryBuffer) Flush(ctx context.Context) error {
	return nil
}

func (this *IMemoryBuffer) RemainingBytes() uint64 {
	return uint64(this.AvailableRead())
}
