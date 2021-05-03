package modules

import (
	"context"
	"ignis/executor/core"
)

type ICacheContextModule struct {
	IModule
}

func NewICacheContextModule(executorData *core.IExecutorData) *ICacheContextModule {
	return &ICacheContextModule{
		IModule{executorData},
	}
}

func (this *ICacheContextModule) SaveContext(ctx context.Context) (_r int64, _err error) { return 0, nil }

func (this *ICacheContextModule) ClearContext(ctx context.Context) (_err error) { return nil }

func (this *ICacheContextModule) LoadContext(ctx context.Context, id int64) (_err error) { return nil }

func (this *ICacheContextModule) Cache(ctx context.Context, id int64, level int8) (_err error) {
	return nil
}

func (this *ICacheContextModule) LoadCache(ctx context.Context, id int64) (_err error) { return nil }
