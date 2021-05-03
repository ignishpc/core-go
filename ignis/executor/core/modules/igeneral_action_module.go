package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/rpc"
)

type IGeneralActionModule struct {
	IModule
}

func NewIGeneralActionModule(executorData *core.IExecutorData) *IGeneralActionModule {
	return &IGeneralActionModule{
		IModule{executorData},
	}
}

func (this *IGeneralActionModule) Execute(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) Reduce(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) TreeReduce(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) Collect(ctx context.Context) (_err error) { return nil }

func (this *IGeneralActionModule) Aggregate(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, combOp *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) TreeAggregate(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, combOp *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) Fold(ctx context.Context, zero *rpc.ISource, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) TreeFold(ctx context.Context, zero *rpc.ISource, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) Take(ctx context.Context, num int64) (_err error) { return nil }

func (this *IGeneralActionModule) Foreach_(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) ForeachPartition(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) ForeachExecutor(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) Top(ctx context.Context, num int64) (_err error) { return nil }

func (this *IGeneralActionModule) Top2(ctx context.Context, num int64, cmp *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) TakeOrdered(ctx context.Context, num int64) (_err error) {
	return nil
}

func (this *IGeneralActionModule) TakeOrdered2(ctx context.Context, num int64, cmp *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralActionModule) Keys(ctx context.Context) (_err error) { return nil }

func (this *IGeneralActionModule) Values(ctx context.Context) (_err error) { return nil }
