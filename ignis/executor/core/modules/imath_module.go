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

type IMathModule struct {
	IModule
	mathImpl   *impl.IMathImpl
	sortImpl   *impl.ISortImpl
	reduceImpl *impl.IReduceImpl
}

func NewIMathModule(executorData *core.IExecutorData) *IMathModule {
	return &IMathModule{
		IModule{executorData},
		impl.NewIMathImpl(executorData),
		impl.NewISortImpl(executorData),
		impl.NewIReduceImpl(executorData),
	}
}

func (this *IMathModule) Sample(ctx context.Context, withReplacement bool, num []int64, seed int32) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Sample(this.mathImpl, withReplacement, num, seed))
}

func (this *IMathModule) Count(ctx context.Context) (_r int64, _err error) {
	defer this.moduleRecover(&_err)
	n, err := this.mathImpl.Count()
	return n, this.PackError(err)
}

func (this *IMathModule) Max(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Max(this.sortImpl))
}

func (this *IMathModule) Min(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Min(this.sortImpl))
}

func (this *IMathModule) Max1(ctx context.Context, cmp *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(cmp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IMaxByAbs); ok {
		return this.PackError(fun.RunMaxBy(this.sortImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.MaxBy(this.sortImpl, anyfun))
	}
	return this.CompatibilityError(reflect.TypeOf(basefun), "max")
}

func (this *IMathModule) Min1(ctx context.Context, cmp *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(cmp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IMinByAbs); ok {
		return this.PackError(fun.RunMinBy(this.sortImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.MinBy(this.sortImpl, anyfun))
	}
	return this.CompatibilityError(reflect.TypeOf(basefun), "min")
}

func (this *IMathModule) SampleByKey(ctx context.Context, withReplacement bool, fractions *rpc.ISource, seed int32) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	numPartitions, err := base.SampleByKeyFilter(this.mathImpl)
	if err != nil {
		return this.PackError(err)
	}
	if err = base.GroupByKey(this.reduceImpl, numPartitions); err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.SampleByKey(this.mathImpl, withReplacement, seed))
}

func (this *IMathModule) CountByKey(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.CountByKey(this.mathImpl))
}

func (this *IMathModule) CountByValue(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.CountByValue(this.mathImpl))
}
