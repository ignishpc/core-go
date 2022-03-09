package impl

import (
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
)

type IPipeImpl struct {
	IBaseImpl
}

func NewIPipeImpl(executorData *core.IExecutorData) *IPipeImpl {
	return &IPipeImpl{
		IBaseImpl{executorData},
	}
}

func ExecuteTo[R any](this *IPipeImpl, f function.IFunction0[[][]R]) error {
	context := this.executorData.GetContext()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("General: ExecuteTo")
	ouput, err := core.NewPartitionGroupDef[R](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	partType, err := this.executorData.GetProperties().PartitionType()
	if err != nil {
		return ierror.Raise(err)
	}

	result, err := f.Call(context)
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: moving elements to partitions")
	for _, v := range result {
		ouput.Add(storage.NewIMemoryPartitionArray(v))
	}

	if partType != storage.IMemoryPartitionType {
		logger.Info("General: saving partitions from memory")
		aux, err := core.NewPartitionGroupDef[R](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		for _, men := range ouput.Iter() {
			part, err := core.NewPartitionWithOther(this.executorData.GetPartitionTools(), men)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = men.MoveTo(part); err != nil {
				return ierror.Raise(err)
			}
			aux.Add(part)
		}
		ouput = aux
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
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

func Flatmap[T any, R any](this *IPipeImpl, f function.IFunction[T, []R]) error {
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

	logger.Info("General: flatmap ", +input.Size(), " partitions")
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
			for _, e2 := range result {
				if err = writer.Write(e2); err != nil {
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

func KeyBy[T any, R comparable](this *IPipeImpl, f function.IFunction[T, R]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupWithSize[ipair.IPair[R, T]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: keyBy ", +input.Size(), " partitions")
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
			if err = writer.Write(*ipair.New(result, elem)); err != nil {
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

func MapPartitions[T any, R any](this *IPipeImpl, f function.IFunction[iterator.IReadIterator[T], []R]) error {
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

	logger.Info("General: flatmap ", +input.Size(), " partitions")
	if err := ithreads.New().Dynamic().RunN(input.Size(), func(i int, sync ithreads.ISync) error {
		reader, err := input.Get(i).ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		writer, err := ouput.Get(i).WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		if err != nil {
			return ierror.Raise(err)
		}
		result, err := f.Call(reader, context)
		if err != nil {
			return ierror.Raise(err)
		}
		for _, e2 := range result {
			if err = writer.Write(e2); err != nil {
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

func MapPartitionsWithIndex[T, R any](this *IPipeImpl, f function.IFunction2[int64, iterator.IReadIterator[T], []R],
	preservesPartitioning bool) error {
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

	logger.Info("General: flatmap ", +input.Size(), " partitions")
	if err := ithreads.New().Dynamic().RunN(input.Size(), func(i int, sync ithreads.ISync) error {
		reader, err := input.Get(i).ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		writer, err := ouput.Get(i).WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		if err != nil {
			return ierror.Raise(err)
		}
		result, err := f.Call(int64(i), reader, context)
		if err != nil {
			return ierror.Raise(err)
		}
		for _, e2 := range result {
			if err = writer.Write(e2); err != nil {
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

func MapExecutor[T any](this *IPipeImpl, f function.IVoidFunction[[][]T]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("General: mapExecutor ", +input.Size(), " partitions")
	inMemory := this.executorData.GetPartitionTools().IsMemoryGroup(input)
	if !inMemory || input.Cache() {
		logger.Info("General: loading partitions in memory")
		aux, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		for _, part := range input.Iter() {
			men, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), part.Size())
			if err != nil {
				return ierror.Raise(err)
			}
			if err = part.CopyTo(men); err != nil {
				return ierror.Raise(err)
			}
			aux.Add(men)
		}
		input = aux
	}

	arg := make([][]T, input.Size())
	for i, part := range input.Iter() {
		list := part.Inner().(storage.IList)
		if array, ok := list.Array().([]T); ok {
			arg[i] = array
		} else {
			array := make([]T, list.Size())
			for i := 0; i < list.Size(); i++ {
				array[i] = list.GetAny(i).(T)
			}
			arg[i] = array
			if _, ok := list.Array().([]any); !ok {
				input.Set(i, storage.NewIMemoryPartitionArray(array)) // Preserve array
			} else {
				inMemory = false // Force new array of real type if is any
			}
		}
	}

	err = f.Call(arg, context)
	if err != nil {
		return ierror.Raise(err)
	}

	if !inMemory {
		logger.Info("General: saving partitions from memory")
		aux, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		for _, men := range input.Iter() {
			part, err := core.NewPartitionWithOther(this.executorData.GetPartitionTools(), men)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = men.MoveTo(part); err != nil {
				return ierror.Raise(err)
			}
			aux.Add(part)
		}
		input = aux
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, input)
	return nil
}

func MapExecutorTo[T any, R any](this *IPipeImpl, f function.IFunction[[][]T, [][]R]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("General: mapExecutor ", +input.Size(), " partitions")
	ouput, err := core.NewPartitionGroupWithSize[R](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}
	inMemory := this.executorData.GetPartitionTools().IsMemoryGroup(input)
	if !inMemory || input.Cache() {
		logger.Info("General: loading partitions in memory")
		aux, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		for _, part := range input.Iter() {
			men, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), part.Size())
			if err != nil {
				return ierror.Raise(err)
			}
			if err = part.CopyTo(men); err != nil {
				return ierror.Raise(err)
			}
			aux.Add(men)
		}
		input = aux
	}

	arg := make([][]T, input.Size())
	for i, part := range input.Iter() {
		list := part.Inner().(storage.IList)
		if array, ok := list.Array().([]T); ok {
			arg[i] = array
		} else {
			array := make([]T, list.Size())
			for i := 0; i < list.Size(); i++ {
				array[i] = list.GetAny(i).(T)
			}
			arg[i] = array
			if _, ok := list.Array().([]any); !ok {
				input.Set(i, storage.NewIMemoryPartitionArray(array)) // Preserve array
			} else {
				inMemory = false // Force new array of real type if is any
			}
		}
	}

	result, err := f.Call(arg, context)
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: moving elements to partitions")
	for _, v := range result {
		ouput.Add(storage.NewIMemoryPartitionArray(v))
	}

	if !inMemory {
		logger.Info("General: saving partitions from memory")
		aux, err := core.NewPartitionGroupDef[R](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		for _, men := range ouput.Iter() {
			part, err := core.NewPartitionWithOther(this.executorData.GetPartitionTools(), men)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = men.MoveTo(part); err != nil {
				return ierror.Raise(err)
			}
			aux.Add(part)
		}
		ouput = aux
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}
