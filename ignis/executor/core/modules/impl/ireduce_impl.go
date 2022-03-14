package impl

import (
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

func GroupByKey[T any, K comparable](this *IReduceImpl, numPartitions int64) error {
	return ierror.RaiseMsg("TODO") //TODO
}
