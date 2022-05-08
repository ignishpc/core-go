package base

import (
	"ignis/executor/api"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"ignis/executor/core/modules/impl"
)

type IExecuteToAbs interface {
	RunExecuteTo(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IExecuteTo[R any] struct {
}

func (this *IExecuteTo[R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[R]()}
}

func (this *IExecuteTo[R]) RunExecuteTo(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.ExecuteTo[R](i, f.(function.IFunction0[[][]R]))
}

type IMapAbs interface {
	RunMap(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMap[T any, R any] struct {
}

func (this *IMap[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeA[R]()}
}

func (this *IMap[T, R]) RunMap(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.Map[T, R](i, f.(function.IFunction[T, R]))
}

type IFilterAbs interface {
	RunFilter(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IFilter[T any] struct {
}

func (this *IFilter[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IFilter[T]) RunFilter(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.Filter[T](i, f.(function.IFunction[T, bool]))
}

type IFlatmapAbs interface {
	RunFlatmap(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IFlatmap[T any, R any] struct {
}

func (this *IFlatmap[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeA[R]()}
}

func (this *IFlatmap[T, R]) RunFlatmap(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.Flatmap[T, R](i, f.(function.IFunction[T, []R]))
}

type IKeyByAbs interface {
	RunKeyBy(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IKeyBy[T any, R comparable] struct {
}

func (this *IKeyBy[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeC[R](), NewTypeCA[R, T]()}
}

func (this *IKeyBy[T, R]) RunKeyBy(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.KeyBy[T, R](i, f.(function.IFunction[T, R]))
}

type IGroupBy[T any, R comparable] struct {
	IKeyBy[T, R]
}

func (this *IGroupBy[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeC[R](), NewTypeCA[R, T](), NewTypeCA[R, []T]()}
}

type IMapWithIndexAbs interface {
	RunMapWithIndex(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapWithIndex[T any, R any] struct {
}

func (this *IMapWithIndex[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeA[R]()}
}

func (this *IMapWithIndex[T, R]) RunMapWithIndex(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.MapWithIndex[T, R](i, f.(function.IFunction2[int64, T, R]))
}

type IMapPartitionsAbs interface {
	RunMapPartitions(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapPartitions[T any, R any] struct {
}

func (this *IMapPartitions[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeA[R]()}
}

func (this *IMapPartitions[T, R]) RunMapPartitions(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.MapPartitions[T, R](i, f.(function.IFunction[iterator.IReadIterator[T], []R]))
}

type IMapPartitionsWithIndexAbs interface {
	RunMapPartitionsWithIndex(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapPartitionsWithIndex[T any, R any] struct {
}

func (this *IMapPartitionsWithIndex[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeA[R]()}
}

func (this *IMapPartitionsWithIndex[T, R]) RunMapPartitionsWithIndex(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.MapPartitionsWithIndex[T, R](i, f.(function.IFunction2[int64, iterator.IReadIterator[T], []R]))
}

type IMapExecutorAbs interface {
	RunMapExecutor(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapExecutor[T any] struct {
}

func (this *IMapExecutor[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IMapExecutor[T]) RunMapExecutor(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.MapExecutor[T](i, f.(function.IVoidFunction[[][]T]))
}

type IMapExecutorToAbs interface {
	RunMapExecutorTo(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapExecutorTo[T any, R any] struct {
}

func (this *IMapExecutorTo[T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T](), NewTypeA[R]()}
}

func (this *IMapExecutorTo[T, R]) RunMapExecutorTo(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.MapExecutorTo[T, R](i, f.(function.IFunction[[][]T, [][]R]))
}

type IForeachAbs interface {
	RunForeach(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IForeach[T any] struct {
}

func (this *IForeach[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IForeach[T]) RunForeach(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.Foreach[T](i, f.(function.IVoidFunction[T]))
}

type IForeachPartitionAbs interface {
	RunForeachPartition(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IForeachPartition[T any] struct {
}

func (this *IForeachPartition[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IForeach[T]) RunForeachPartition(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.ForeachPartition[T](i, f.(function.IVoidFunction[iterator.IReadIterator[T]]))
}

type IForeachExecutorAbs interface {
	RunForeachExecutor(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IForeachExecutor[T any] struct {
}

func (this *IForeachExecutor[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IForeach[T]) RunForeachExecutor(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.ForeachExecutor[T](i, f.(function.IVoidFunction[[][]T]))
}

type IPartitionByAbs interface {
	RunPartitionBy(i *impl.IRepartitionImpl, f function.IBaseFunction, numPartitions int64) error
}

type IPartitionBy[T any] struct {
}

func (this *IPartitionBy[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IPartitionBy[T]) RunPartitionBy(i *impl.IRepartitionImpl, f function.IBaseFunction, numPartitions int64) error {
	return impl.PartitionBy[T](i, f.(function.IFunction[T, int64]), numPartitions)
}

type IMapValuesAbs interface {
	RunMapValues(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IMapValues[K comparable, T any, R any] struct {
}

func (this *IMapValues[K, T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeC[K](), NewTypeA[T](), NewTypeA[R](), NewTypeCA[K, T](), NewTypeCA[K, R]()}
}

func (this *IMapValues[K, T, R]) RunMapValues(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.MapValues[K](i, f.(function.IFunction[T, R]))
}

type IFlatMapValuesAbs interface {
	RunFlatMapValues(i *impl.IPipeImpl, f function.IBaseFunction) error
}

type IFlatMapValues[K comparable, T any, R any] struct {
}

func (this *IFlatMapValues[K, T, R]) Types() []api.IContextType {
	return []api.IContextType{NewTypeC[K](), NewTypeA[T](), NewTypeA[R](), NewTypeCA[K, T](), NewTypeCA[K, R]()}
}

func (this *IFlatMapValues[K, T, R]) RunFlatMapValues(i *impl.IPipeImpl, f function.IBaseFunction) error {
	return impl.FlatMapValues[K](i, f.(function.IFunction[T, []R]))
}

type IReduceByKeyAbs interface {
	RunReduceByKey(i *impl.IReduceImpl, f function.IBaseFunction, numPartitions int64, localReduce bool) error
}

type IReduceByKey[K comparable, T any] struct {
}

func (this *IReduceByKey[K, T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeC[K](), NewTypeA[T](), NewTypeCA[K, T]()}
}

func (this *IReduceByKey[K, T]) RunReduceByKey(i *impl.IReduceImpl, f function.IBaseFunction, numPartitions int64, localReduce bool) error {
	return impl.ReduceByKey[K](i, f.(function.IFunction2[T, T, T]), numPartitions, localReduce)
}

type IAggregateByKeyAbs interface {
	RunAggregateByKey(i *impl.IReduceImpl, f function.IBaseFunction, numPartitions int64, hashing bool) error
}

type IAggregateByKey[K comparable, T1 any, T2 any] struct {
}

func (this *IAggregateByKey[K, T1, T2]) Types() []api.IContextType {
	return []api.IContextType{NewTypeC[K](), NewTypeA[T1](), NewTypeA[T2](), NewTypeCA[K, T1](), NewTypeCA[K, T2]()}
}

func (this *IAggregateByKey[K, T1, T2]) RunAggregateByKey(i *impl.IReduceImpl, f function.IBaseFunction, numPartitions int64, hashing bool) error {
	return impl.AggregateByKey[K](i, f.(function.IFunction2[T1, T2, T1]), numPartitions, hashing)
}

type IFoldByKeyAbs interface {
	RunFoldByKey(i *impl.IReduceImpl, f function.IBaseFunction, numPartitions int64, localFold bool) error
}

type IFoldByKey[K comparable, T any] struct {
}

func (this *IFoldByKey[K, T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeC[K](), NewTypeA[T](), NewTypeCA[K, T]()}
}

func (this *IFoldByKey[K, T]) RunFoldByKey(i *impl.IReduceImpl, f function.IBaseFunction, numPartitions int64, localFold bool) error {
	return impl.FoldByKey[K](i, f.(function.IFunction2[T, T, T]), numPartitions, localFold)
}

type ISortByAbs interface {
	RunSortBy(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error
	RunSortByWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error
}

type ISortBy[T any] struct {
}

func (this *ISortBy[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *ISortBy[T]) RunSortBy(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error {
	return impl.SortBy(i, f.(function.IFunction2[T, T, bool]), ascending)
}

func (this *ISortBy[T]) RunSortByWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error {
	return impl.SortByWithPartitions(i, f.(function.IFunction2[T, T, bool]), ascending, partitions)
}

type ITopByAbs interface {
	RunTopBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error
}

type ITopBy[T any] struct {
}

func (this *ITopBy[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *ITopBy[T]) RunTopBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error {
	return impl.TopBy(i, f.(function.IFunction2[T, T, bool]), n)
}

type ITakeOrderedByAbs interface {
	RunTakeOrderedBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error
}

type ITakeOrderedBy[T any] struct {
}

func (this *ITakeOrderedBy[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *ITakeOrderedBy[T]) RunTakeOrderedBy(i *impl.ISortImpl, f function.IBaseFunction, n int64) error {
	return impl.TakeOrderedBy(i, f.(function.IFunction2[T, T, bool]), n)
}

type IMaxByAbs interface {
	RunMaxBy(i *impl.ISortImpl, f function.IBaseFunction) error
}

type IMaxBy[T any] struct {
}

func (this *IMaxBy[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IMaxBy[T]) RunMaxBy(i *impl.ISortImpl, f function.IBaseFunction) error {
	return impl.MaxBy(i, f.(function.IFunction2[T, T, bool]))
}

type IMinByAbs interface {
	RunMinBy(i *impl.ISortImpl, f function.IBaseFunction) error
}

type IMinBy[T any] struct {
}

func (this *IMinBy[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IMinBy[T]) RunMinBy(i *impl.ISortImpl, f function.IBaseFunction) error {
	return impl.MinBy(i, f.(function.IFunction2[T, T, bool]))
}

type IReduceAbs interface {
	RunReduce(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type IReduce[T any] struct {
}

func (this *IReduce[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IReduce[T]) RunReduce(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.Reduce(i, f.(function.IFunction2[T, T, T]))
}

type ITreeReduceAbs interface {
	RunTreeReduce(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type ITreeReduce[T any] struct {
}

func (this *ITreeReduce[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *ITreeReduce[T]) RunTreeReduce(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.TreeReduce(i, f.(function.IFunction2[T, T, T]))
}

type IZeroAbs interface {
	RunZero(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type IZero[T any] struct {
}

func (this *IZero[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IZero[T]) RunZero(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.Zero(i, f.(function.IFunction0[T]))
}

type IAggregateAbs interface {
	RunAggregate(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type IAggregate[T1 any, T2 any] struct {
}

func (this *IAggregate[T1, T2]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T1](), NewTypeA[T2]()}
}

func (this *IAggregate[T1, T2]) RunAggregate(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.Aggregate(i, f.(function.IFunction2[T1, T2, T1]))
}

type ITreeAggregateAbs interface {
	RunTreeAggregate(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type ITreeAggregate[T1 any, T2 any] struct {
}

func (this *ITreeAggregate[T1, T2]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T1](), NewTypeA[T2]()}
}

func (this *ITreeAggregate[T1, T2]) RunTreeAggregate(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.TreeAggregate(i, f.(function.IFunction2[T1, T2, T1]))
}

type IFoldAbs interface {
	RunFold(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type IFold[T1 any] struct {
}

func (this *IFold[T]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T]()}
}

func (this *IFold[T]) RunFold(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.Fold(i, f.(function.IFunction2[T, T, T]))
}

type ITreeFoldAbs interface {
	RunTreeFold(i *impl.IReduceImpl, f function.IBaseFunction) error
}

type ITreeFold[T1 any] struct {
}

func (this *ITreeFold[T1]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T1]()}
}

func (this *ITreeFold[T]) RunTreeFold(i *impl.IReduceImpl, f function.IBaseFunction) error {
	return impl.TreeFold(i, f.(function.IFunction2[T, T, T]))
}

type ISortByKeyAbs interface {
	RunSortByKey(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error
	RunSortByKeyWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error
}

type ISortByKey[K any, V any] struct {
}

func (this *ISortByKey[T1, T2]) Types() []api.IContextType {
	return []api.IContextType{NewTypeA[T1](), NewTypeA[T2](), NewTypeAA[T1, T2]()}
}

func (this *ISortByKey[K, V]) RunSortByKey(i *impl.ISortImpl, f function.IBaseFunction, ascending bool) error {
	return impl.SortByKeyBy[V, K](i, f.(function.IFunction2[K, K, bool]), ascending)
}

func (this *ISortByKey[K, V]) RunSortByKeyWithPartitions(i *impl.ISortImpl, f function.IBaseFunction, ascending bool, partitions int64) error {
	return impl.SortByKeyByWithPartitions[V, K](i, f.(function.IFunction2[K, K, bool]), ascending, partitions)
}
