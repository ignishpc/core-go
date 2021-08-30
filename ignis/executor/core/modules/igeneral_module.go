package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/rpc"
)

type IGeneralModule struct {
	IModule
}

func NewIGeneralModule(executorData *core.IExecutorData) *IGeneralModule {
	return &IGeneralModule{
		IModule{executorData},
	}
}

func (this *IGeneralModule) ExecuteTo(ctx context.Context, src *rpc.ISource) (_err error) { return nil }

func (this *IGeneralModule) Map_(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) Filter(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) Flatmap(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) KeyBy(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) MapPartitions(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) MapPartitionsWithIndex(ctx context.Context, src *rpc.ISource, preservesPartitioning bool) (_err error) {
	return nil
}

func (this *IGeneralModule) MapExecutor(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) MapExecutorTo(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IGeneralModule) GroupBy(ctx context.Context, src *rpc.ISource, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) Sort(ctx context.Context, ascending bool) (_err error) { return nil }

func (this *IGeneralModule) Sort2(ctx context.Context, ascending bool, numPartitions int64) (_err error) {
	return nil
}

func (this *IGeneralModule) SortBy(ctx context.Context, src *rpc.ISource, ascending bool) (_err error) {
	return nil
}

func (this *IGeneralModule) SortBy3(ctx context.Context, src *rpc.ISource, ascending bool, numPartitions int64) (_err error) {
	return nil
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
