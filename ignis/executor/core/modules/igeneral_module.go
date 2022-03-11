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

type IGeneralModule struct {
	IModule
	pipeImpl *impl.IPipeImpl
	sortImpl *impl.ISortImpl
}

func NewIGeneralModule(executorData *core.IExecutorData) *IGeneralModule {
	return &IGeneralModule{
		IModule{executorData},
		impl.NewIPipeImpl(executorData),
		impl.NewISortImpl(executorData),
	}
}

func (this *IGeneralModule) ExecuteTo(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if mapfun, ok := basefun.(base.IExecuteToAbs); ok {
		return this.PackError(mapfun.RunExecuteTo(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction0[[][]any]); ok {
		return this.PackError(impl.ExecuteTo(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "executeTo")
}

func (this *IGeneralModule) Map_(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if mapfun, ok := basefun.(base.IMapAbs); ok {
		return this.PackError(mapfun.RunMap(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[any, any]); ok {
		return this.PackError(impl.Map(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "map")
}

func (this *IGeneralModule) Filter(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IFilterAbs); ok {
		return this.PackError(filterfun.RunFilter(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[any, bool]); ok {
		return this.PackError(impl.Filter(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "filter")
}

func (this *IGeneralModule) Flatmap(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IFlatmapAbs); ok {
		return this.PackError(filterfun.RunFlatmap(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[any, []any]); ok {
		return this.PackError(impl.Flatmap(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "flatmap")
}

func (this *IGeneralModule) KeyBy(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IKeyByAbs); ok {
		return this.PackError(filterfun.RunKeyBy(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[any, any]); ok {
		return this.PackError(impl.KeyBy(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "keyBy")
}

func (this *IGeneralModule) MapPartitions(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IMapPartitionsAbs); ok {
		return this.PackError(filterfun.RunMapPartitions(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[iterator.IReadIterator[any], []any]); ok {
		return this.PackError(impl.MapPartitions(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapPartitions")
}

func (this *IGeneralModule) MapPartitionsWithIndex(ctx context.Context, src *rpc.ISource, preservesPartitioning bool) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IMapPartitionsWithIndexAbs); ok {
		return this.PackError(filterfun.RunMapPartitionsWithIndex(this.pipeImpl, basefun, preservesPartitioning))
	} else if anyfun, ok := basefun.(function.IFunction2[int64, iterator.IReadIterator[any], []any]); ok {
		return this.PackError(impl.MapPartitionsWithIndex(this.pipeImpl, anyfun, preservesPartitioning))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapPartitionsWithIndex")
}

func (this *IGeneralModule) MapExecutor(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IMapExecutorAbs); ok {
		return this.PackError(filterfun.RunMapExecutor(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IVoidFunction[[][]any]); ok {
		return this.PackError(impl.MapExecutor(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapExecutor")
}

func (this *IGeneralModule) MapExecutorTo(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.IMapExecutorToAbs); ok {
		return this.PackError(filterfun.RunMapExecutorTo(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[[][]any, [][]any]); ok {
		return this.PackError(impl.MapExecutorTo(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapExecutorTo")
}

func (this *IGeneralModule) GroupBy(ctx context.Context, src *rpc.ISource, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) Sort(ctx context.Context, ascending bool) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Sort(this.sortImpl, ascending))
}

func (this *IGeneralModule) Sort2(ctx context.Context, ascending bool, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.SortWithPartitions(this.sortImpl, ascending, numPartitions))
}

func (this *IGeneralModule) SortBy(ctx context.Context, src *rpc.ISource, ascending bool) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.ISortByAbs); ok {
		return this.PackError(filterfun.RunSortBy(this.sortImpl, basefun, ascending))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.SortBy(this.sortImpl, anyfun, ascending))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "sortBy")
}

func (this *IGeneralModule) SortBy3(ctx context.Context, src *rpc.ISource, ascending bool, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if filterfun, ok := basefun.(base.ISortByAbs); ok {
		return this.PackError(filterfun.RunSortByWithPartitions(this.sortImpl, basefun, ascending, numPartitions))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.SortByWithPartitions(this.sortImpl, anyfun, ascending, numPartitions))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "sortBy")
}

func (this *IGeneralModule) Union_(ctx context.Context, other string, preserveOrder bool) (_err error) {
	return nil
}

func (this *IGeneralModule) Union2(ctx context.Context, other string, preserveOrder bool, src *rpc.ISource) (_err error) {
	return nil
}
func (this *IGeneralModule) Join(ctx context.Context, other string, numPartitions int64) (_err error) {
	return nil
}
func (this *IGeneralModule) Join3(ctx context.Context, other string, numPartitions int64, src *rpc.ISource) (_err error) {
	return nil
}
func (this *IGeneralModule) Distinct(ctx context.Context, numPartitions int64) (_err error) {
	return nil
}
func (this *IGeneralModule) Distinct2(ctx context.Context, numPartitions int64, src *rpc.ISource) (_err error) {
	return nil
}
func (this *IGeneralModule) Repartition(ctx context.Context, numPartitions int64, preserveOrdering bool, global_ bool) (_err error) {
	return nil
}
func (this *IGeneralModule) PartitionByRandom(ctx context.Context, numPartitions int64) (_err error) {
	return nil
}
func (this *IGeneralModule) PartitionByHash(ctx context.Context, numPartitions int64) (_err error) {
	return nil
}
func (this *IGeneralModule) PartitionBy(ctx context.Context, src *rpc.ISource, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) FlatMapValues(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) MapValues(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) GroupByKey(ctx context.Context, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) GroupByKey2(ctx context.Context, numPartitions int64, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) ReduceByKey(ctx context.Context, src *rpc.ISource, numPartitions int64, localReduce bool) (_err error) {
	return nil
}

func (this *IGeneralModule) AggregateByKey(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) AggregateByKey4(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, combOp *rpc.ISource, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) FoldByKey(ctx context.Context, zero *rpc.ISource, src *rpc.ISource, numPartitions int64, localFold bool) (_err error) {
	return nil
}

func (this *IGeneralModule) SortByKey(ctx context.Context, ascending bool) (_err error) {
	return nil
}

func (this *IGeneralModule) SortByKey2a(ctx context.Context, ascending bool, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) SortByKey2b(ctx context.Context, src *rpc.ISource, ascending bool) (_err error) {
	return nil
}

func (this *IGeneralModule) SortByKey3(ctx context.Context, src *rpc.ISource, ascending bool, numPartitions int64) (_err error) {
	return nil
}
