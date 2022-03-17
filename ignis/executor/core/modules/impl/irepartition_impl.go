package impl

import (
	"ignis/executor/api/function"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
)

type IRepartitionImpl struct {
	IBaseImpl
}

func NewIRepartitionImpl(executorData *core.IExecutorData) *IRepartitionImpl {
	return &IRepartitionImpl{
		IBaseImpl{executorData},
	}
}

func Repartition[T any](this *IRepartitionImpl, numPartitions int64, preserveOrdering bool, global_ bool) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}
func PartitionByRandom[T any](this *IRepartitionImpl, numPartitions int64) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}
func PartitionByHash[T any](this *IRepartitionImpl, numPartitions int64) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}
func PartitionBy[T any](this *IRepartitionImpl, f function.IFunction[T, int64], numPartitions int64) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}
