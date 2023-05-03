package core

import (
	"ignis/executor/api"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/itransport"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
)

type IPartitionTools struct {
	properties     *IPropertyParser
	context        api.IContext
	partitionIdGen atomic.Int32
}

func NewIPartitionTools(properties *IPropertyParser, ctx api.IContext) *IPartitionTools {
	return &IPartitionTools{
		properties,
		ctx,
		atomic.Int32{},
	}
}

func NewPartitionDef[T any](this *IPartitionTools) (storage.IPartition[T], error) {
	name, err := this.properties.PartitionType()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return NewPartitionWithName[T](this, name)
}

func NewPartitionWithName[T any](this *IPartitionTools, name string) (storage.IPartition[T], error) {
	if name == storage.IMemoryPartitionType {
		return NewMemoryPartitionDef[T](this)
	} else if name == storage.IRawMemoryPartitionType {
		return NewRawMemoryPartitionDef[T](this)
	} else if name == storage.IDiskPartitionType {
		return NewDiskPartitionDef[T](this)
	}
	return nil, ierror.RaiseMsg("unknown partition type: " + name)
}

func NewPartitionWithOther[T any](this *IPartitionTools, other storage.IPartition[T]) (storage.IPartition[T], error) {
	name, err := this.properties.PartitionType()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	if name == storage.IMemoryPartitionType {
		return NewMemoryPartition[T](this, other.Size())
	} else if name == storage.IRawMemoryPartitionType {
		return NewRawMemoryPartition[T](this, other.Bytes())
	} else if name == storage.IDiskPartitionType {
		return NewDiskPartitionDef[T](this)
	}
	return nil, ierror.RaiseMsg("unknown partition type: " + name)
}

func NewPartitionGroupWithSize[T any](this *IPartitionTools, sz int) (*storage.IPartitionGroup[T], error) {
	group := storage.NewIPartitionGroup[T]()
	for i := 0; i < sz; i++ {
		part, err := NewPartitionDef[T](this)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		group.Add(part)
	}
	return group, nil
}

func NewPartitionGroupWithOther[T any](this *IPartitionTools, sz int) (*storage.IPartitionGroup[T], error) {
	group := storage.NewIPartitionGroup[T]()
	for i := 0; i < sz; i++ {
		part, err := NewPartitionWithOther(this, group.Get(i))
		if err != nil {
			return nil, ierror.Raise(err)
		}
		group.Add(part)
	}
	return group, nil
}

func NewPartitionGroupDef[T any](this *IPartitionTools) (*storage.IPartitionGroup[T], error) {
	return NewPartitionGroupWithSize[T](this, 0)
}

func NewMemoryPartitionDef[T any](this *IPartitionTools) (*storage.IMemoryPartition[T], error) {
	native, err := this.properties.NativeSerialization()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return storage.NewIMemoryPartition[T](1024*1024, native), nil
}

func NewMemoryPartition[T any](this *IPartitionTools, sz int64) (*storage.IMemoryPartition[T], error) {
	native, err := this.properties.NativeSerialization()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return storage.NewIMemoryPartition[T](sz, native), nil
}

func NewRawMemoryPartitionDef[T any](this *IPartitionTools) (*storage.IRawMemoryPartition[T], error) {
	return NewRawMemoryPartition[T](this, 1024*1024*8)
}

func NewRawMemoryPartition[T any](this *IPartitionTools, bytes int64) (*storage.IRawMemoryPartition[T], error) {
	native, err := this.properties.NativeSerialization()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	compression, err := this.properties.PartitionCompression()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return storage.NewIRawMemoryPartition[T](bytes, compression, native)
}

func NewDiskPartitionDef[T any](this *IPartitionTools) (*storage.IDiskPartition[T], error) {
	return NewDiskPartition[T](this, "", false, false)
}

func NewDiskPartition[T any](this *IPartitionTools, name string, persist bool, read bool) (*storage.IDiskPartition[T], error) {
	native, err := this.properties.NativeSerialization()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	compression, err := this.properties.PartitionCompression()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	path, err := this.Diskpath(name)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	return storage.NewIDiskPartition[T](path, compression, native, persist, read)
}

func (this *IPartitionTools) Diskpath(name string) (string, error) {
	path, err := this.properties.ExecutorDirectory()
	if err != nil {
		return "", ierror.Raise(err)
	}
	path += "/partitions"
	_ = this.CreateDirectoryIfNotExists(path)
	if len(name) == 0 {
		path += "/partition"
		path += strconv.Itoa(this.context.ExecutorId())
		path += "."
		path += strconv.Itoa(int(this.partitionIdGen.Add(1)))
	} else {
		path += "/" + name
	}
	return path, nil
}

func (this *IPartitionTools) IsMemory(part storage.IPartitionBase) bool {
	return part.Type() == storage.IMemoryPartitionType
}

func (this *IPartitionTools) IsMemoryGroup(group storage.IPartitionGroupBase) bool {
	if group.Size() > 0 {
		return group.GetBase(0).Type() == storage.IMemoryPartitionType
	}
	return false
}

func (this *IPartitionTools) IsRawMemory(part storage.IPartitionBase) bool {
	return part.Type() == storage.IRawMemoryPartitionType
}

func (this *IPartitionTools) IsRawMemoryGroup(group storage.IPartitionGroupBase) bool {
	if group.Size() > 0 {
		return group.GetBase(0).Type() == storage.IRawMemoryPartitionType
	}
	return false
}

func (this *IPartitionTools) IsDisk(part storage.IPartitionBase) bool {
	return part.Type() == storage.IDiskPartitionType
}

func (this *IPartitionTools) IsDiskGroup(group storage.IPartitionGroupBase) bool {
	if group.Size() > 0 {
		return group.GetBase(0).Type() == storage.IDiskPartitionType
	}
	return false
}

func ConvertGroupPartitionTo[T any](this *IPartitionTools, other storage.IPartitionGroupBase) (*storage.IPartitionGroup[T], error) {
	group, err := NewPartitionGroupDef[T](this)
	if err != nil {
		return nil, ierror.Raise(err)
	}

	if this.IsMemoryGroup(other) {
		flag := true
		if !group.Type().AssignableTo(other.Type()) && !other.Type().AssignableTo(group.Type()) {
			pt := reflect.TypeOf(ipair.IPair[any, any]{})
			if ipair.IsPairType(group.Type()) && ipair.IsPairType(other.Type()) && (group.Type() == pt || other.Type() == pt) {
				if this.IsMemoryGroup(other) {
					logger.Warn(other.Type().String() + " will be convert to " + group.Type().String())
				}
			} else {
				flag = false
				logger.Warn("Can't convert partition from " + other.Type().String() + " to " + group.Type().String() + "" +
					", using a conversion wfrom bytes")
			}
		}
		if flag {
			for i := 0; i < other.Size(); i++ {
				group.AddBase(storage.ConvIMemoryPartition[T](other.GetBase(i)))
			}
		} else {
			buffer := itransport.NewIMemoryBuffer()
			for i := 0; i < other.Size(); i++ {
				if err := other.GetBase(i).Write(buffer, 0); err != nil {
					return nil, ierror.Raise(err)
				}
				newPart, err := NewMemoryPartition[T](this, other.GetBase(i).Size())
				if err != nil {
					return nil, ierror.Raise(err)
				}
				group.AddBase(newPart)
			}
		}
	} else if this.IsRawMemoryGroup(other) {
		for i := 0; i < other.Size(); i++ {
			part, err := storage.ConvIRawMemoryPartition[T](other.GetBase(i))
			if err != nil {
				return nil, ierror.Raise(err)
			}
			group.AddBase(part)
		}
	} else {
		for i := 0; i < other.Size(); i++ {
			part, err := storage.ConvIDiskPartition[T](other.GetBase(i))
			if err != nil {
				return nil, ierror.Raise(err)
			}
			group.AddBase(part)
		}
	}

	return group, nil
}

func (this *IPartitionTools) CreateDirectoryIfNotExists(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}
