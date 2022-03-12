package modules

import (
	"context"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/core"
	"ignis/executor/core/modules/impl"
	"ignis/rpc"
	"reflect"
)

type IGeneralActionModule struct {
	IModule
	pipeImpl *impl.IPipeImpl
	sortImpl *impl.ISortImpl
}

func NewIGeneralActionModule(executorData *core.IExecutorData) *IGeneralActionModule {
	return &IGeneralActionModule{
		IModule{executorData},
		impl.NewIPipeImpl(executorData),
		impl.NewISortImpl(executorData),
	}
}

func (this *IGeneralActionModule) Execute(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(function.IVoidFunction0); ok {
		return this.PackError(impl.Execute(this.pipeImpl, fun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "execute")
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

func (this *IGeneralActionModule) Top(ctx context.Context, num int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Top(this.sortImpl, num))
}

func (this *IGeneralActionModule) Top2(ctx context.Context, num int64, cmp *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(cmp)
	if err != nil {
		return this.PackError(err)
	}
	if mapfun, ok := basefun.(base.ITopByAbs); ok {
		return this.PackError(mapfun.RunTopBy(this.sortImpl, basefun, num))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.TopBy(this.sortImpl, anyfun, num))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "top")
}

func (this *IGeneralActionModule) TakeOrdered(ctx context.Context, num int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.TakeOrdered(this.sortImpl, num))
}

func (this *IGeneralActionModule) TakeOrdered2(ctx context.Context, num int64, cmp *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(cmp)
	if err != nil {
		return this.PackError(err)
	}
	if mapfun, ok := basefun.(base.ITakeOrderedByAbs); ok {
		return this.PackError(mapfun.RunTakeOrderedBy(this.sortImpl, basefun, num))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.TakeOrderedBy(this.sortImpl, anyfun, num))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "takeOrdered")
}

func (this *IGeneralActionModule) Keys(ctx context.Context) (_err error) { return nil }

func (this *IGeneralActionModule) Values(ctx context.Context) (_err error) { return nil }
