package impl

import (
	"ignis/executor/api/function"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
)

type IReduceImpl struct {
	IBaseImpl
}

func NewIReduceImpl(executorData *core.IExecutorData) *IReduceImpl {
	return &IReduceImpl{
		IBaseImpl{executorData},
	}
}

func Reduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func TreeReduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Zero[T any](this *IReduceImpl, f function.IFunction0[T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Aggregate[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func TreeAggregate[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Fold[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func TreeFold[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Union[T any](this *IReduceImpl, other string, preserveOrder bool) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Join[T any](this *IReduceImpl, other string, numPartitions int64) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Distinct[T comparable](this *IReduceImpl, numPartitions int64) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func GroupByKey[T any, K comparable](this *IReduceImpl, numPartitions int64) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func ReduceByKey[K comparable, T any](this *IReduceImpl, f function.IFunction2[T, T, T], numPartitions int64, localReduce bool) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func AggregateByKey[K comparable, T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T], numPartitions int64, hashing bool) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func FoldByKey[K comparable, T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T], numPartitions int64, localFold bool) error {
	return ierror.RaiseMsg("TODO") //TODO
}
