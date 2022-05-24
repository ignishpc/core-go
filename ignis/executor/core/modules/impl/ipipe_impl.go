package impl

import (
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
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

func Execute(this *IPipeImpl, f function.IVoidFunction0) error {
	context := this.executorData.GetContext()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("General: Execute")

	err := f.Call(context)
	if err != nil {
		return ierror.Raise(err)
	}

	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
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
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
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
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
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
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
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
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}

func MapWithIndex[T any, R any](this *IPipeImpl, f function.IFunction2[int64, T, R]) error {
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

	logger.Info("General: mapWithIndex ", +input.Size(), " partitions")

	indices := make([]impi.C_int64, context.Executors())
	elems := impi.C_int64(0)
	for _, p := range input.Iter() {
		elems += impi.C_int64(p.Size())
	}
	err = impi.MPI_Allgather(impi.P(&elems), 1, impi.MPI_LONG_LONG_INT, impi.P(&indices[0]), 1,
		impi.MPI_LONG_LONG_INT, this.executorData.Mpi().Native())
	if err != nil {
		return ierror.Raise(err)
	}
	offset := make([]int64, input.Size())
	for i := 0; i < context.ExecutorId(); i++ {
		offset[0] += int64(indices[i])
	}
	for i := 1; i < input.Size(); i++ {
		offset[i] = offset[i-1] + input.Get(i).Size()
	}

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
			reader, err := input.Get(i).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writer, err := ouput.Get(i).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			id := offset[i]
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				result, err := f.Call(id, elem, context)
				if err != nil {
					return ierror.Raise(err)
				}
				id++
				if err = writer.Write(result); err != nil {
					return ierror.Raise(err)
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
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

	logger.Info("General: mapPartitions ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}

func MapPartitionsWithIndex[T, R any](this *IPipeImpl, f function.IFunction2[int64, iterator.IReadIterator[T], []R]) error {
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

	logger.Info("General: mapPartitionsWithIndex ", +input.Size(), " partitions")

	indices := make([]impi.C_int64, context.Executors())
	parts := impi.C_int64(input.Size())
	err = impi.MPI_Allgather(impi.P(&parts), 1, impi.MPI_LONG_LONG_INT, impi.P(&indices[0]), 1,
		impi.MPI_LONG_LONG_INT, this.executorData.Mpi().Native())
	if err != nil {
		return ierror.Raise(err)
	}
	offset := int64(0)
	for i := 0; i < context.ExecutorId(); i++ {
		offset += int64(indices[i])
	}

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
			reader, err := input.Get(i).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writer, err := ouput.Get(i).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			result, err := f.Call(offset+int64(i), reader, context)
			if err != nil {
				return ierror.Raise(err)
			}
			for _, e2 := range result {
				if err = writer.Write(e2); err != nil {
					return ierror.Raise(err)
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
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
	logger.Info("General: mapExecutorTo ", +input.Size(), " partitions")
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

func Foreach[T any](this *IPipeImpl, f function.IVoidFunction[T]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: foreach ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
			reader, err := input.Get(i).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if err := f.Call(elem, context); err != nil {
					return ierror.Raise(err)
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func ForeachPartition[T any](this *IPipeImpl, f function.IVoidFunction[iterator.IReadIterator[T]]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: foreachPartition ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
			reader, err := input.Get(i).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			if err != nil {
				return ierror.Raise(err)
			}
			if err := f.Call(reader, context); err != nil {
				return ierror.Raise(err)
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func ForeachExecutor[T any](this *IPipeImpl, f function.IVoidFunction[[][]T]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("General: foreachExecutor ", +input.Size(), " partitions")
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
		}
	}

	err = f.Call(arg, context)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func Take[T any](this *IPipeImpl, num int64) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	count := int64(0)
	for _, part := range input.Iter() {
		if part.Size()+count > num {
			if this.executorData.GetPartitionTools().IsMemory(part) && !input.Cache() {
				part.Inner().(storage.IList).Resize(int(num-count), false)
			} else {
				cut, err := core.NewPartitionWithName[T](this.executorData.GetPartitionTools(), part.Type())
				if err != nil {
					return ierror.Raise(err)
				}
				reader, err := part.ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				writer, err := cut.WriteIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				for count != num {
					elem, err := reader.Next()
					if err != nil {
						return ierror.Raise(err)
					}
					if err = writer.Write(elem); err != nil {
						return ierror.Raise(err)
					}
					count++
				}
			}
			break
		}
		count += part.Size()
		output.Add(part)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func Keys[T1 any, T2 any](this *IPipeImpl) error {
	input, err := core.GetAndDeletePartitions[ipair.IPair[T1, T2]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[T1](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: keys ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
			reader, err := input.Get(i).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writer, err := output.Get(i).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = writer.Write(elem.First); err != nil {
					return ierror.Raise(err)
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func Values[T1 any, T2 any](this *IPipeImpl) error {
	input, err := core.GetAndDeletePartitions[ipair.IPair[T1, T2]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[T2](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: values ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
			reader, err := input.Get(i).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writer, err := output.Get(i).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = writer.Write(elem.Second); err != nil {
					return ierror.Raise(err)
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func MapValues[K any, T any, R any](this *IPipeImpl, f function.IFunction[T, R]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupWithSize[ipair.IPair[K, R]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: mapValues ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
				result, err := f.Call(elem.Second, context)
				if err != nil {
					return ierror.Raise(err)
				}
				if err = writer.Write(*ipair.New(elem.First, result)); err != nil {
					return ierror.Raise(err)
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}

func FlatMapValues[K any, T any, R any](this *IPipeImpl, f function.IFunction[T, []R]) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupWithSize[ipair.IPair[K, R]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("General: flatMapValues ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		context := this.executorData.GetThreadContext(rctx.ThreadId())
		return rctx.For().Dynamic().Run(input.Size(), func(i int) error {
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
				result, err := f.Call(elem.Second, context)
				if err != nil {
					return ierror.Raise(err)
				}
				for _, e2 := range result {
					if err = writer.Write(*ipair.New(elem.First, e2)); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			input.Set(i, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}
