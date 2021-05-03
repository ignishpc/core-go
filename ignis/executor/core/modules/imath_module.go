package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/rpc"
)

type IMathModule struct {
	IModule
}

func NewIMathModule(executorData *core.IExecutorData) *IMathModule {
	return &IMathModule{
		IModule{executorData},
	}
}

func (this *IMathModule) Sample(ctx context.Context, withReplacement bool, num []int64, seed int32) (_err error) {
	return nil
}

func (this *IMathModule) Count(ctx context.Context) (_r int64, _err error) { return 0, nil }

func (this *IMathModule) Max(ctx context.Context) (_err error) { return nil }

func (this *IMathModule) Min(ctx context.Context) (_err error) { return nil }

func (this *IMathModule) Max1(ctx context.Context, cmp *rpc.ISource) (_err error) { return nil }

func (this *IMathModule) Min1(ctx context.Context, cmp *rpc.ISource) (_err error) { return nil }

func (this *IMathModule) SampleByKey(ctx context.Context, withReplacement bool, fractions *rpc.ISource, seed int32) (_err error) {
	return nil
}

func (this *IMathModule) CountByKey(ctx context.Context) (_err error) { return nil }

func (this *IMathModule) CountByValue(ctx context.Context) (_err error) { return nil }
