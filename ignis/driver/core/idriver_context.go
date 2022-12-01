package core

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/executor/api/base"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/modules"
	"ignis/executor/core/storage"
	"ignis/rpc"
	"strconv"
	"sync"
)

type IDriverContext struct {
	modules.IModule
	mu      sync.Mutex
	context map[int64]storage.IPartitionGroupBase
	data    map[int64]func() storage.IPartitionGroupBase
	next_id int64
}

func NewIDriverContext(executorData *core.IExecutorData) *IDriverContext {
	return &IDriverContext{
		modules.NewIModule(executorData),
		sync.Mutex{},
		make(map[int64]storage.IPartitionGroupBase),
		make(map[int64]func() storage.IPartitionGroupBase),
		0,
	}
}

func (this *IDriverContext) Cache(ctx context.Context, id int64, level int8) (_err error) {
	return this.PackError(ierror.RaiseMsg("Driver does not implement cache"))
}

func (this *IDriverContext) LoadCache(ctx context.Context, id int64) (_err error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	value, present := this.data[id]
	if !present {
		return this.PackError(ierror.RaiseMsg("data " + strconv.FormatInt(id, 10) + " not found"))
	}
	this.GetExecutorData().SetPartitionsAny(value())
	return nil
}

func (this *IDriverContext) SaveContext(ctx context.Context) (_r int64, _err error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	id := this.next_id
	this.next_id++
	this.context[id] = this.GetExecutorData().GetPartitionsAny()
	return id, nil
}

func (this *IDriverContext) ClearContext(ctx context.Context) (_err error) {
	this.GetExecutorData().DeletePartitions()
	this.GetExecutorData().ClearVariables()
	vars := this.GetExecutorData().GetContext().Vars()
	for k := range vars {
		delete(vars, k)
	}
	return nil
}

func (this *IDriverContext) LoadContext(ctx context.Context, id int64) (_err error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.GetExecutorData().SetPartitionsAny(this.context[id])
	delete(this.context, id)
	return nil
}

func (this *IDriverContext) LoadContextAsVariable(ctx context.Context, id int64, name string) (_err error) {
	return this.PackError(ierror.RaiseMsg("Driver does not implement loadContextAsVariable"))
}

func Parallelize[T any](this *IDriverContext, data []T, native bool) (int64, error) {
	get := func() storage.IPartitionGroupBase {
		group := storage.NewIPartitionGroup[T]()
		part := storage.NewIMemoryPartitionArray[T](data)
		group.Add(part)
		return group
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	id := this.next_id
	this.next_id++
	this.data[id] = get
	return id, nil
}

func Collect[T any](this *IDriverContext, id int64) ([]T, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	group := this.context[id].(*storage.IPartitionGroup[T])
	delete(this.context, id)
	elems := 0
	for i := 0; i < group.Size(); i++ {
		elems += int(group.Get(i).Size())
	}
	result := make([]T, elems)
	i := 0
	for _, tp := range group.Iter() {
		reader, err := tp.ReadIterator()
		if err != nil {
			return nil, derror.NewGenericIDriverError(err)
		}
		for reader.HasNext() {
			result[i], err = reader.Next()
			if err != nil {
				return nil, derror.NewGenericIDriverError(err)
			}
			i++
		}
	}
	return result, nil
}

func Collect1[T any](this *IDriverContext, id int64) (T, error) {
	results, err := Collect[T](this, id)
	if err != nil {
		return *new(T), err
	}
	if len(results) == 0 {
		return *new(T), derror.NewIDriverError("Empty collection")
	}
	return results[0], nil
}

func RegisterType[T any](this *IDriverContext) *rpc.ISource {
	tp := base.NewTypeA[T]()
	this.GetExecutorData().RegisterType(tp)
	src := rpc.NewISource()
	src.Obj = rpc.NewIEncoded()
	name := ":" + tp.Name()
	src.Obj.Name = &name
	return src
}
