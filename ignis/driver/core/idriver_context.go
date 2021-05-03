package core

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/executor/core"
	"ignis/executor/core/modules"
	"sync"
)

type IDriverContext struct {
	modules.IModule
	mu sync.Mutex
}

func NewIDriverContext(executorData *core.IExecutorData) *IDriverContext {
	return &IDriverContext{
		modules.NewIModule(executorData),
		sync.Mutex{},
	}
}

func (this *IDriverContext) SaveContext(ctx context.Context) (_r int64, _err error) { return 0, nil }

func (this *IDriverContext) ClearContext(ctx context.Context) (_err error) { return nil }

func (this *IDriverContext) LoadContext(ctx context.Context, id int64) (_err error) { return nil }

func (this *IDriverContext) Cache(ctx context.Context, id int64, level int8) (_err error) {
	return nil
}

func (this *IDriverContext) LoadCache(ctx context.Context, id int64) (_err error) { return nil }

func (this *IDriverContext) Parallelize(data interface{}, native bool) (int64, error) {
	return 0, derror.NewIDriverError("Not implemented") //TODO
}

func (this *IDriverContext) Collect(id int64) (interface{}, error) {
	return 0, derror.NewIDriverError("Not implemented") //TODO
}

func (this *IDriverContext) Collect1(id int64) (interface{}, error) {
	return 0, derror.NewIDriverError("Not implemented") //TODO
}
