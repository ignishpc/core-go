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
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupWithSize[R](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: map ", +input.Size(), " partitions")
	if err := ithreads.New().Dynamic().RunN(input.Size(), func(i int, sync ithreads.ISync) error {
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
			if err = writer.Write(result); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}

func Filter[T any](this *IPipeImpl, f function.IFunction[T, bool]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: filter ", +input.Size(), " partitions")
	if err := ithreads.New().Dynamic().RunN(input.Size(), func(i int, sync ithreads.ISync) error {
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
			if result {
				if err = writer.Write(elem); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}
