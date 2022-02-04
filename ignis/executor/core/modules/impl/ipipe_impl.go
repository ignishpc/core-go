package impl

import (
	"ignis/executor/api/function"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
)

type IPipeImpl struct {
	IBaseImpl
}

func NewIPipeImpl(executorData *core.IExecutorData) *IPipeImpl {
	return &IPipeImpl{
		IBaseImpl{executorData},
	}
}

func Map[T any, R any](this *IPipeImpl, f function.IFunction[T, R]) error {
	pool := ithreads.NewIThreadPool()
	context := this.executorData.GetContext()
	input := core.CheckPartitions[T](this.executorData.GetAndDeletePartitions())
	if err := f.Before(context); err != nil {
		return err
	}
	ouput := core.NewPartitionGroup[R](this.executorData.GetPartitionTools(), input.Size())

	logger.Info("General: map ", +input.Size(), " partitions")
	pool.DinamicRun(0, input.Size(), func(i int) error {
		reader, err := input.Get(i).ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		writer, err := ouput.Get(i).WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		for reader.HasNext() {
			elem, err := reader.Next()
			if err != nil {
				return ierror.Raise(err)
			}
			result, err := f.Call(elem, context)
			if err != nil {
				return ierror.Raise(err)
			}
			writer.Write(result)
		}
		return nil
	})
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	this.executorData.SetPartitions(ouput)
	return nil
}

func Filter[T any](this *IPipeImpl, f function.IFunction[T, bool]) error {

	return nil
}
