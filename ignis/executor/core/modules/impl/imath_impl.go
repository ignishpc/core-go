package impl

import (
	"ignis/executor/core"
)

type IMathImpl struct {
	IBaseImpl
}

func NewIMathImpl(executorData *core.IExecutorData) *IMathImpl {
	return &IMathImpl{
		IBaseImpl{executorData},
	}
}

func Sample[T any](this *IMathImpl, withReplacement bool, num []int64, seed int32) error {
	return nil //TODO
}

func (this *IMathImpl) Count() (int64, error) {
	group := this.executorData.GetPartitionsAny()
	n := int64(0)
	for i := 0; i < group.Size(); i++ {
		n += group.GetBase(i).Size()
	}
	return n, nil
}

func SampleByKey[T any, K any](this *IMathImpl, withReplacement bool, seed int32) error {
	return nil //TODO
}

func CountByKey[T any, K any](this *IMathImpl) error {
	return nil //TODO
}

func CountByValue[T any, K any](this *IMathImpl) error {
	return nil //TODO
}
