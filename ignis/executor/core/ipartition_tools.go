package core

import (
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"os"
	"reflect"
)

type IPartitionTools struct {
	properties     *IPropertyParser
	partitionIdGen int
}

func NewIPartitionTools(properties *IPropertyParser) *IPartitionTools {
	return &IPartitionTools{
		properties,
		0,
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
		//return NewRawMemoryPartitionDef[T](this) //TODO
	} else if name == storage.IDiskPartitionType {
		//return NewDiskPartitionDef[T](this) //TODO
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
		//return NewRawMemoryPartition[T](this, other.Bytes()) //TODO
	} else if name == storage.IDiskPartitionType {
		//return NewDiskPartitionDef[T](this) //TODO
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

func NewRawMemoryPartitionDef[T any](this *IPartitionTools, sz int64) (*storage.IRawMemoryPartition[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet")
}

func NewRawMemoryPartition[T any](this *IPartitionTools, sz int64) (*storage.IRawMemoryPartition[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet")
}

func NewDiskPartitionDef[T any](this *IPartitionTools) (*storage.IDiskPartition[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet")
}

func NewDiskPartition[T any](this *IPartitionTools, name string, persist bool, read bool) (*storage.IDiskPartition[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet")
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

	if !group.Type().AssignableTo(other.Type()) && !other.Type().AssignableTo(group.Type()) {
		pt := reflect.TypeOf(ipair.IPair[any, any]{})
		if ipair.IsPairType(group.Type()) && ipair.IsPairType(other.Type()) && (group.Type() == pt || other.Type() == pt) {
			if this.IsMemoryGroup(other) {
				logger.Warn(other.Type().String() + " will be convert to " + group.Type().String())
			}
		} else {
			return nil, ierror.RaiseMsg("Can't convert partition from " + other.Type().String() + " to " + group.Type().String())
		}
	}

	var conv func(storage.IPartitionBase) storage.IPartitionBase

	if this.IsMemoryGroup(other) {
		conv = storage.ConvIMemoryPartition[T]
	} else if this.IsRawMemoryGroup(other) {
		return nil, ierror.RaiseMsg("Not implemented yet") //TODO
	} else {
		return nil, ierror.RaiseMsg("Not implemented yet") //TODO
	}

	for i := 0; i < other.Size(); i++ {
		group.AddBase(conv(other.GetBase(i)))
	}

	return group, nil
}

func (this *IPartitionTools) CreateDirectoryIfNotExists(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}
