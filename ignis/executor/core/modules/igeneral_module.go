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
	pipeImpl        *impl.IPipeImpl
	sortImpl        *impl.ISortImpl
	reduceImpl      *impl.IReduceImpl
	repartitionImpl *impl.IRepartitionImpl
}

func NewIGeneralModule(executorData *core.IExecutorData) *IGeneralModule {
	return &IGeneralModule{
		IModule{executorData},
		impl.NewIPipeImpl(executorData),
		impl.NewISortImpl(executorData),
		impl.NewIReduceImpl(executorData),
		impl.NewIRepartitionImpl(executorData),
	}
}

func (this *IGeneralModule) ExecuteTo(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IExecuteToAbs); ok {
		return this.PackError(fun.RunExecuteTo(this.pipeImpl, basefun))
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
	if fun, ok := basefun.(base.IMapAbs); ok {
		return this.PackError(fun.RunMap(this.pipeImpl, basefun))
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
	if fun, ok := basefun.(base.IFilterAbs); ok {
		return this.PackError(fun.RunFilter(this.pipeImpl, basefun))
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
	if fun, ok := basefun.(base.IFlatmapAbs); ok {
		return this.PackError(fun.RunFlatmap(this.pipeImpl, basefun))
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
	if fun, ok := basefun.(base.IKeyByAbs); ok {
		return this.PackError(fun.RunKeyBy(this.pipeImpl, basefun))
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
	if fun, ok := basefun.(base.IMapPartitionsAbs); ok {
		return this.PackError(fun.RunMapPartitions(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[iterator.IReadIterator[any], []any]); ok {
		return this.PackError(impl.MapPartitions(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapPartitions")
}

func (this *IGeneralModule) MapPartitionsWithIndex(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IMapPartitionsWithIndexAbs); ok {
		return this.PackError(fun.RunMapPartitionsWithIndex(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction2[int64, iterator.IReadIterator[any], []any]); ok {
		return this.PackError(impl.MapPartitionsWithIndex(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapPartitionsWithIndex")
}

func (this *IGeneralModule) MapExecutor(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IMapExecutorAbs); ok {
		return this.PackError(fun.RunMapExecutor(this.pipeImpl, basefun))
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
	if fun, ok := basefun.(base.IMapExecutorToAbs); ok {
		return this.PackError(fun.RunMapExecutorTo(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[[][]any, [][]any]); ok {
		return this.PackError(impl.MapExecutorTo(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapExecutorTo")
}

func (this *IGeneralModule) GroupBy(ctx context.Context, src *rpc.ISource, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IKeyByAbs); ok {
		err = fun.RunKeyBy(this.pipeImpl, basefun)
	} else if anyfun, ok := basefun.(function.IFunction[any, any]); ok {
		err = impl.KeyBy(this.pipeImpl, anyfun)
	} else {
		return this.CompatibilyError(reflect.TypeOf(basefun), "groupBy")
	}
	if err != nil {
		return this.PackError(err)
	}
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.GroupByKey(this.reduceImpl, numPartitions))
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
	if fun, ok := basefun.(base.ISortByAbs); ok {
		return this.PackError(fun.RunSortBy(this.sortImpl, basefun, ascending))
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
	if fun, ok := basefun.(base.ISortByAbs); ok {
		return this.PackError(fun.RunSortByWithPartitions(this.sortImpl, basefun, ascending, numPartitions))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.SortByWithPartitions(this.sortImpl, anyfun, ascending, numPartitions))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "sortBy")
}

func (this *IGeneralModule) Union_(ctx context.Context, other string, preserveOrder bool) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Union(this.reduceImpl, other, preserveOrder))
}

func (this *IGeneralModule) Union2(ctx context.Context, other string, preserveOrder bool, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromSource(src)
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Union(this.reduceImpl, other, preserveOrder))
}
func (this *IGeneralModule) Join(ctx context.Context, other string, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Join(this.reduceImpl, other, numPartitions))
}
func (this *IGeneralModule) Join3(ctx context.Context, other string, numPartitions int64, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromSource(src)
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Join(this.reduceImpl, other, numPartitions))
}
func (this *IGeneralModule) Distinct(ctx context.Context, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Distinct(this.reduceImpl, numPartitions))
}
func (this *IGeneralModule) Distinct2(ctx context.Context, numPartitions int64, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromSource(src)
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Distinct(this.reduceImpl, numPartitions))
}
func (this *IGeneralModule) Repartition(ctx context.Context, numPartitions int64, preserveOrdering bool, global_ bool) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.Repartition(this.repartitionImpl, numPartitions, preserveOrdering, global_))
}
func (this *IGeneralModule) PartitionByRandom(ctx context.Context, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.PartitionByRandom(this.repartitionImpl, numPartitions))
}
func (this *IGeneralModule) PartitionByHash(ctx context.Context, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.PartitionByHash(this.repartitionImpl, numPartitions))
}
func (this *IGeneralModule) PartitionBy(ctx context.Context, src *rpc.ISource, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IPartitionByAbs); ok {
		return this.PackError(fun.RunPartitionBy(this.repartitionImpl, basefun, numPartitions))
	} else if anyfun, ok := basefun.(function.IFunction[any, int64]); ok {
		return this.PackError(impl.PartitionBy(this.repartitionImpl, anyfun, numPartitions))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "partitionBy")
}

func (this *IGeneralModule) FlatMapValues(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IFlatMapValuesAbs); ok {
		return this.PackError(fun.RunFlatMapValues(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[any, []any]); ok {
		return this.PackError(impl.Map(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "flatMapValues")
}

func (this *IGeneralModule) MapValues(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IMapValuesAbs); ok {
		return this.PackError(fun.RunMapValues(this.pipeImpl, basefun))
	} else if anyfun, ok := basefun.(function.IFunction[any, any]); ok {
		return this.PackError(impl.Map(this.pipeImpl, anyfun))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "mapValues")
}

func (this *IGeneralModule) GroupByKey(ctx context.Context, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.GroupByKey(this.reduceImpl, numPartitions))
}

func (this *IGeneralModule) GroupByKey2(ctx context.Context, numPartitions int64, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromSource(src)
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.GroupByKey(this.reduceImpl, numPartitions))
}

func (this *IGeneralModule) ReduceByKey(ctx context.Context, src *rpc.ISource, numPartitions int64, localReduce bool) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.IReduceByKeyAbs); ok {
		return this.PackError(fun.RunReduceByKey(this.reduceImpl, basefun, numPartitions, localReduce))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, any]); ok {
		return this.PackError(impl.ReduceByKey[any](this.reduceImpl, anyfun, numPartitions, localReduce))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "reduceByKey")
}

func (this *IGeneralModule) AggregateByKey(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, numPartitions int64) (_err error) {
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
		return this.CompatibilyError(reflect.TypeOf(zerofun), "aggregateByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(seqOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.IAggregateByKeyAbs); ok {
		err = fun.RunAggregateByKey(this.reduceImpl, zerofun, numPartitions, true)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.AggregateByKey[any](this.reduceImpl, anyfun, numPartitions, true)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "aggregateByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralModule) AggregateByKey4(ctx context.Context, zero *rpc.ISource, seqOp *rpc.ISource, combOp *rpc.ISource, numPartitions int64) (_err error) {
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
		return this.CompatibilyError(reflect.TypeOf(zerofun), "aggregateByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(seqOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.IAggregateByKeyAbs); ok {
		err = fun.RunAggregateByKey(this.reduceImpl, seqfun, numPartitions, false)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.AggregateByKey[any](this.reduceImpl, anyfun, numPartitions, false)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "aggregateByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	combfun, err := this.executorData.LoadLibrary(combOp)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := combfun.(base.IReduceByKeyAbs); ok {
		err = fun.RunReduceByKey(this.reduceImpl, combfun, numPartitions, false)
	} else if anyfun, ok := combfun.(function.IFunction2[any, any, any]); ok {
		err = impl.ReduceByKey[any](this.reduceImpl, anyfun, numPartitions, false)
	} else {
		return this.CompatibilyError(reflect.TypeOf(combfun), "aggregateByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralModule) FoldByKey(ctx context.Context, zero *rpc.ISource, src *rpc.ISource, numPartitions int64, localFold bool) (_err error) {
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
		return this.CompatibilyError(reflect.TypeOf(zerofun), "foldByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	seqfun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := seqfun.(base.IFoldByKeyAbs); ok {
		err = fun.RunFoldByKey(this.reduceImpl, seqfun, numPartitions, localFold)
	} else if anyfun, ok := seqfun.(function.IFunction2[any, any, any]); ok {
		err = impl.FoldByKey[any](this.reduceImpl, anyfun, numPartitions, localFold)
	} else {
		return this.CompatibilyError(reflect.TypeOf(seqfun), "foldByKey")
	}
	if err != nil {
		return this.PackError(err)
	}
	return nil
}

func (this *IGeneralModule) SortByKey(ctx context.Context, ascending bool) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.SortByKey(this.sortImpl, ascending))
}

func (this *IGeneralModule) SortByKey2a(ctx context.Context, ascending bool, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.SortByKeyWithPartitions(this.sortImpl, ascending, numPartitions))
}

func (this *IGeneralModule) SortByKey2b(ctx context.Context, src *rpc.ISource, ascending bool) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.ISortByKeyAbs); ok {
		return this.PackError(fun.RunSortByKey(this.sortImpl, basefun, ascending))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.SortByKeyBy[any, any](this.sortImpl, anyfun, ascending))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "sortByKey")
}

func (this *IGeneralModule) SortByKey3(ctx context.Context, src *rpc.ISource, ascending bool, numPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	if fun, ok := basefun.(base.ISortByKeyAbs); ok {
		return this.PackError(fun.RunSortByKeyWithPartitions(this.sortImpl, basefun, ascending, numPartitions))
	} else if anyfun, ok := basefun.(function.IFunction2[any, any, bool]); ok {
		return this.PackError(impl.SortByKeyByWithPartitions[any, any](this.sortImpl, anyfun, ascending, numPartitions))
	}
	return this.CompatibilyError(reflect.TypeOf(basefun), "sortByKey")
}
