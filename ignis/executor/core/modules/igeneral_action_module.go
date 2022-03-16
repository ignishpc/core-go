package modules

import (
	"context"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"ignis/executor/core"
	"ignis/executor/core/modules/impl"
	"ignis/rpc"
	"reflect"
)

type IGeneralActionModule struct {
	IModule
	pipeImpl   *impl.IPipeImpl
	sortImpl   *impl.ISortImpl
	reduceImpl *impl.IReduceImpl
}

func NewIGeneralActionModule(executorData *core.IExecutorData) *IGeneralActionModule {
	return &IGeneralActionModule{
		IModule{executorData},
		impl.NewIPipeImpl(executorData),
		impl.NewISortImpl(executorData),
		impl.NewIReduceImpl(executorData),
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
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IReduceAbs); ok {
		return this.PackError(fun.RunReduce(this.reduceImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, any]); ok {
		return this.PackError(impl.Reduce(this.reduceImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "reduce")
}

func (this *IGeneralActionModule) TreeReduce(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.ITreeReduceAbs); ok {
		return this.PackError(fun.RunTreeReduce(this.reduceImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, any]); ok {
		return this.PackError(impl.TreeReduce(this.reduceImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "treeReduce")
}

func (this *IGeneralActionModule) Collect(ctx context.Context) (_err error) {
	//Do nothing
	return nil
}

func (this *IGeneralActionModule) Aggregate(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, combOp *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	zerofun, err := this.executorData.LoadLibrary(zero)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := zerofun.(base.IZeroAbs); ok {
		err = fun.RunZero(this.reduceImpl, zerofun)
	} else if anyfun, ok := zerofun.(function.IFunction0[any]); ok {
		err = impl.Zero(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(zerofun), "aggregate")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(seqOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.IAggregateAbs); ok {
		err = fun.RunAggregate(this.reduceImpl, zerofun)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.Aggregate(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "aggregate")
	}
	if err != nil {
		return this.PackError(err)
	}
	combfun, err := this.executorData.LoadLibrary(combOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := combfun.(base.IReduceAbs); ok {
		err = fun.RunReduce(this.reduceImpl, zerofun)
	} else if anyfun, ok := combfun.(function.IFunction2[any, any, any]); ok {
		err = impl.Reduce(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(combfun), "aggregate")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralActionModule) TreeAggregate(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, combOp *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	zerofun, err := this.executorData.LoadLibrary(zero)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := zerofun.(base.IZeroAbs); ok {
		err = fun.RunZero(this.reduceImpl, zerofun)
	} else if anyfun, ok := zerofun.(function.IFunction0[any]); ok {
		err = impl.Zero(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(zerofun), "treeAggregate")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(seqOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.ITreeAggregateAbs); ok {
		err = fun.RunTreeAggregate(this.reduceImpl, zerofun)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.TreeAggregate(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "treeAggregate")
	}
	if err != nil {
		return this.PackError(err)
	}
	combfun, err := this.executorData.LoadLibrary(combOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := combfun.(base.IReduceAbs); ok {
		err = fun.RunReduce(this.reduceImpl, zerofun)
	} else if anyfun, ok := combfun.(function.IFunction2[any, any, any]); ok {
		err = impl.Reduce(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(combfun), "treeAggregate")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralActionModule) Fold(ctx context.Context, zero *rpc.ISource, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	zerofun, err := this.executorData.LoadLibrary(zero)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := zerofun.(base.IZeroAbs); ok {
		err = fun.RunZero(this.reduceImpl, zerofun)
	} else if anyfun, ok := zerofun.(function.IFunction0[any]); ok {
		err = impl.Zero(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(zerofun), "fold")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.IFoldAbs); ok {
		err = fun.RunFold(this.reduceImpl, zerofun)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.Fold(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "fold")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralActionModule) TreeFold(ctx context.Context, zero *rpc.ISource, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	zerofun, err := this.executorData.LoadLibrary(zero)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := zerofun.(base.IZeroAbs); ok {
		err = fun.RunZero(this.reduceImpl, zerofun)
	} else if anyfun, ok := zerofun.(function.IFunction0[any]); ok {
		err = impl.Zero(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(zerofun), "treeFold")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.ITreeFoldAbs); ok {
		err = fun.RunTreeFold(this.reduceImpl, zerofun)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.TreeFold(this.reduceImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "treeFold")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralActionModule) Take(ctx context.Context, num int64) (_err error) {
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Take(this.pipeImpl, num))
}

func (this *IGeneralActionModule) Foreach_(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IForeachAbs); ok {
		return this.PackError(fun.RunForeach(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IVoidFunction[any]); ok {
		return this.PackError(impl.Foreach(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "foreach")
}

func (this *IGeneralActionModule) ForeachPartition(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IForeachPartitionAbs); ok {
		return this.PackError(fun.RunForeachPartition(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IVoidFunction[iterator.IReadIterator[any]]); ok {
		return this.PackError(impl.ForeachPartition(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "foreachPartition")
}

func (this *IGeneralActionModule) ForeachExecutor(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IForeachExecutorAbs); ok {
		return this.PackError(fun.RunForeachExecutor(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IVoidFunction[[][]any]); ok {
		return this.PackError(impl.ForeachExecutor(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "foreachExecutor")
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
	if fun, ok := basefun.(base.ITopByAbs); ok {
		return this.PackError(fun.RunTopBy(this.sortImpl, basefun, num))
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
	if fun, ok := basefun.(base.ITakeOrderedByAbs); ok {
		return this.PackError(fun.RunTakeOrderedBy(this.sortImpl, basefun, num))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.TakeOrderedBy(this.sortImpl, anyfun, num))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "takeOrdered")
}

func (this *IGeneralActionModule) Keys(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Keys(this.pipeImpl))
}

func (this *IGeneralActionModule) Values(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Values(this.pipeImpl))
}
