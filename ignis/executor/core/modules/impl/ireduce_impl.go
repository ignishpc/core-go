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
	"ignis/executor/core/utils"
	"math"
)

type IReduceImpl struct {
	IBaseImpl
}

func NewIReduceImpl(executorData *core.IExecutorData) *IReduceImpl {
	return &IReduceImpl{
		IBaseImpl{executorData},
	}
}

func basicReduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T], result *storage.IMemoryPartition[T]) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: reducing ", input.Size(), " partitions locally")

	return ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		var acum T
		acumFlag := false
		if err := rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			if input.Get(p).Empty() {
				return nil
			}
			var err error
			if acumFlag {
				acum, err = aggregatePartition(this, f, input.Get(p), acum)
			} else {
				acum, err = reducePartition(this, f, input.Get(p))
				acumFlag = true
			}
			if err != nil {
				return ierror.Raise(err)
			}
			input.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		if acumFlag {
			return rctx.Critical(func() error {
				result.Inner().(storage.IList).AddAny(acum)
				return nil
			})
		}
		return nil
	})
}

func basicFold[T any](this *IReduceImpl, f function.IFunction2[T, T, T], result *storage.IMemoryPartition[T]) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: folding ", input.Size(), " partitions locally")

	return ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		acum := core.GetVariable[T](this.executorData, "zero")
		if err := rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			if input.Get(p).Empty() {
				return nil
			}
			if acum, err = aggregatePartition(this, f, input.Get(p), acum); err != nil {
				return ierror.Raise(err)
			}
			input.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		return rctx.Critical(func() error {
			result.Inner().(storage.IList).AddAny(acum)
			return nil
		})
	})
}

func Reduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	elemPart, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), 1)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := basicReduce(this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := finalReduce(this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func TreeReduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	elemPart, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), 1)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := basicReduce(this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := finalTreeReduce(this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func Zero[T any](this *IReduceImpl, f function.IFunction0[T]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if zero, err := f.Call(context); err != nil {
		return ierror.Raise(err)
	} else {
		core.SetVariable(this.executorData, "zero", zero)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func Aggregate[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	input, err := core.GetPartitions[T2](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	partialReduce, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), int64(this.executorData.GetCores()))
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: aggregating ", input.Size(), " partitions locally")

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		acum := core.GetVariable[T](this.executorData, "zero")
		if err := rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			if input.Get(p).Empty() {
				return nil
			}
			var err error
			acum, err = aggregatePartition(this, f, input.Get(p), acum)
			return err
		}); err != nil {
			return ierror.Raise(err)
		}
		return rctx.Critical(func() error {
			partialReduce.Inner().(storage.IList).AddAny(acum)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	output.Add(partialReduce)
	core.SetPartitions(this.executorData, output)
	return nil
}

func TreeAggregate[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	return Aggregate(this, f)
}

func Fold[T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	elemPart, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), 1)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := basicFold(this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := finalReduce[T](this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func TreeFold[T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	elemPart, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), 1)
	if err != nil {
		return ierror.Raise(err)
	}
	if err := basicFold(this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := finalTreeReduce[T](this, f, elemPart); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func GroupByKey[T any, K comparable](this *IReduceImpl, numPartitions int64) error {
	if err := keyHashing[K, T](this, numPartitions); err != nil {
		return ierror.Raise(err)
	}
	if err := keyExchanging[K, T](this); err != nil {
		return ierror.Raise(err)
	}

	input, err := core.GetPartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[ipair.IPair[K, []T]](this.executorData.GetPartitionTools(), int(numPartitions))
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: reducing key elements")

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		acum := map[K][]T{}
		return rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			part := input.Get(p)
			reader, err := part.ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				acum[elem.First] = append(acum[elem.First], elem.Second)
			}
			writer, err := output.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for key, values := range acum {
				if err := writer.Write(*ipair.New(key, values)); err != nil {
					return ierror.Raise(err)
				}
			}
			acum = map[K][]T{}
			input.SetBase(p, nil)
			return output.Get(p).Fit()
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func ReduceByKey[K comparable, T any](this *IReduceImpl, f function.IFunction2[T, T, T], numPartitions int64, localReduce bool) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if localReduce {
		logger.Info("Reduce: local reducing key elements")
		if err := localReduceByKey[K](this, f); err != nil {
			return ierror.Raise(err)
		}
	}
	if err := keyHashing[K, T](this, numPartitions); err != nil {
		return ierror.Raise(err)
	}
	if err := keyExchanging[K, T](this); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: reducing key elements")
	if err := localReduceByKey[K](this, f); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func AggregateByKey[K comparable, T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T], numPartitions int64, hashing bool) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if hashing {
		if err := keyHashing[K, T](this, numPartitions); err != nil {
			return ierror.Raise(err)
		}
		if err := keyExchanging[K, T](this); err != nil {
			return ierror.Raise(err)
		}
	}
	logger.Info("Reduce: aggregating key elements")
	if err := localAggregateByKey[K](this, f); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func FoldByKey[K comparable, T any](this *IReduceImpl, f function.IFunction2[T, T, T], numPartitions int64, localFold bool) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if localFold {
		logger.Info("Reduce: local folding key elements")
		if err := localAggregateByKey[K](this, f); err != nil {
			return ierror.Raise(err)
		}
	}
	if err := keyHashing[K, T](this, numPartitions); err != nil {
		return ierror.Raise(err)
	}
	if err := keyExchanging[K, T](this); err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: folding key elements")
	if err := localReduceByKey[K](this, f); err != nil {
		return ierror.Raise(err)
	}
	err := f.After(context)
	if err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func Union[T any](this *IReduceImpl, other string, preserveOrder bool) error {
	input, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	this.executorData.SetPartitionsAny(core.GetVariable[storage.IPartitionGroupBase](this.executorData, other))
	input2, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: union ", input.Size(), " and ", input2.Size(), " partitions")
	var storage string
	if input.Size() > 0 {
		storage = input.Get(0).Type()
		if input2.Size() > 0 {
			if input.Get(0).Type() != input2.Get(0).Type() {
				for i, part := range input2.Iter() {
					newPart, err := core.NewPartitionWithName[T](this.executorData.GetPartitionTools(), storage)
					if err != nil {
						return ierror.Raise(err)
					}
					if err := part.CopyTo(newPart); err != nil {
						return ierror.Raise(err)
					}
					input2.Set(i, newPart)
				}
			}
		}
	} else if input2.Size() > 0 {
		storage = input2.Get(0).Type()
	} else {
		storage, err = this.executorData.GetProperties().PartitionType()
		if err != nil {
			return ierror.Raise(err)
		}
	}

	if preserveOrder {
		logger.Info("Reduce: union using order mode")
		executors := this.executorData.Mpi().Executors()
		rank := this.executorData.Mpi().Rank()
		count := input.Size()
		globalCount := int64(0)
		offset := int64(0)
		execCounts := make([]impi.C_int64, executors)
		count2 := input2.Size()
		globalCount2 := int64(0)
		offset2 := int64(0)
		execCounts2 := make([]impi.C_int64, executors)
		if err = impi.MPI_Allgather(impi.P(&count), 1, impi.MPI_LONG_LONG_INT, impi.P(&execCounts[0]), 1, impi.MPI_LONG_LONG_INT, this.executorData.Mpi().Native()); err != nil {
			return ierror.Raise(err)
		}
		if err = impi.MPI_Allgather(impi.P(&count2), 1, impi.MPI_LONG_LONG_INT, impi.P(&execCounts2[0]), 1, impi.MPI_LONG_LONG_INT, this.executorData.Mpi().Native()); err != nil {
			return ierror.Raise(err)
		}

		for i := 0; i < executors; i++ {
			if i == rank {
				offset = globalCount
				offset2 = globalCount2
			}
			globalCount += int64(execCounts[i])
			globalCount2 += int64(execCounts2[i])
		}
		tmp, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		create := func(n int64) error {
			for i := int64(0); i < n; i++ {
				part, err := core.NewPartitionWithName[T](this.executorData.GetPartitionTools(), storage)
				if err != nil {
					return ierror.Raise(err)
				}
				tmp.Add(part)
			}
			return nil
		}

		if err = create(offset); err != nil {
			return ierror.Raise(err)
		}
		for _, part := range input.Iter() {
			tmp.Add(part)
		}
		if err = create(globalCount - int64(tmp.Size())); err != nil {
			return ierror.Raise(err)
		}
		if err = create(offset2); err != nil {
			return ierror.Raise(err)
		}
		for _, part := range input2.Iter() {
			tmp.Add(part)
		}
		if err = create(globalCount + globalCount2 - int64(tmp.Size())); err != nil {
			return ierror.Raise(err)
		}
		if err = Exchange[T](&this.IBaseImpl, tmp, output); err != nil {
			return ierror.Raise(err)
		}
	} else {
		logger.Info("Reduce: union using fast mode")
		for _, part := range input.Iter() {
			output.Add(part)
		}
		for _, part := range input2.Iter() {
			output.Add(part)
		}
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func Join[K comparable, T any](this *IReduceImpl, other string, numPartitions int64) error {
	logger.Info("Reduce: preparing first partitions")
	if err := keyHashing[K, T](this, numPartitions); err != nil {
		return ierror.Raise(err)
	}
	if err := keyExchanging[K, T](this); err != nil {
		return ierror.Raise(err)
	}
	input, err := core.GetPartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Reduce: preparing second partitions")
	this.executorData.SetPartitionsAny(core.GetVariable[storage.IPartitionGroupBase](this.executorData, other))
	if err := keyHashing[K, T](this, numPartitions); err != nil {
		return ierror.Raise(err)
	}
	if err := keyExchanging[K, T](this); err != nil {
		return ierror.Raise(err)
	}
	input2, err := core.GetPartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Reduce: joining key elements")
	output, err := core.NewPartitionGroupWithSize[ipair.IPair[K, ipair.IPair[T, T]]](this.executorData.GetPartitionTools(), int(numPartitions))
	if err != nil {
		return ierror.Raise(err)
	}

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		acum := map[K][]T{}
		return rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			writer, err := output.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				acum[elem.First] = append(acum[elem.First], elem.Second)
			}

			reader, err = input2.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if values, present := acum[elem.First]; present {
					for _, value := range values {
						if err = writer.Write(*ipair.New(elem.First, *ipair.New(value, elem.Second))); err != nil {
							return ierror.Raise(err)
						}

					}
				}
			}
			acum = map[K][]T{}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func Distinct[T comparable](this *IReduceImpl, numPartitions int64) error {
	input, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: distinct ", input.Size(), " partitions")
	tmp, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), int(numPartitions))
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: creating ", numPartitions, " new partitions with hashing")
	hasher := utils.GetHasher(utils.TypeObj[T]())
	if err = ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		threadRanges, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), tmp.Size())
		if err != nil {
			return ierror.Raise(err)
		}
		writers := make([]iterator.IWriteIterator[T], tmp.Size())
		for p := 0; p < tmp.Size(); p++ {
			writers[p], err = threadRanges.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
		}
		if err = rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = writers[utils.Hash(elem, hasher)%uint64(numPartitions)].Write(elem); err != nil {
					return ierror.Raise(err)
				}
			}
			input.SetBase(p, nil)

			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		return rctx.Critical(func() error {
			for p, part := range threadRanges.Iter() {
				if err := part.MoveTo(tmp.Get(p)); err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	if err = distinctFilter(this, tmp); err != nil {
		return ierror.Raise(err)
	}
	if err = Exchange(this.Base(), tmp, output); err != nil {
		return ierror.Raise(err)
	}
	if err = distinctFilter(this, output); err != nil {
		return ierror.Raise(err)
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func reducePartition[T any](this *IReduceImpl, f function.IFunction2[T, T, T], part storage.IPartition[T]) (T, error) {
	context := this.executorData.GetContext()
	reader, err := part.ReadIterator()
	if err != nil {
		return *new(T), ierror.Raise(err)
	}
	acum, err := reader.Next()
	if err != nil {
		return acum, ierror.Raise(err)
	}
	for reader.HasNext() {
		elem, err := reader.Next()
		if err != nil {
			return acum, ierror.Raise(err)
		}
		acum, err = f.Call(acum, elem, context)
		if err != nil {
			return acum, ierror.Raise(err)
		}
	}
	return acum, nil
}

func finalReduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T], partial *storage.IMemoryPartition[T]) error {
	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: reducing all elements in the executor")
	if partial.Size() > 1 {
		elem, err := reducePartition[T](this, f, partial)
		if err != nil {
			return ierror.Raise(err)
		}
		list := partial.Inner().(storage.IList)
		list.SetAny(0, elem)
		list.Resize(1, true)
	}

	logger.Info("Reduce: gathering elements for an executor")
	if err = core.Gather[T](this.executorData.Mpi(), partial, 0); err != nil {
		return ierror.Raise(err)
	}
	if this.executorData.Mpi().IsRoot(0) && !partial.Empty() {
		logger.Info("Reduce: final reduce")
		result, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), 1)
		if err != nil {
			return ierror.Raise(err)
		}
		elem, err := reducePartition[T](this, f, partial)
		if err != nil {
			return ierror.Raise(err)
		}
		result.Inner().(storage.IList).AddAny(elem)
		output.Add(result)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func finalTreeReduce[T any](this *IReduceImpl, f function.IFunction2[T, T, T], partial *storage.IMemoryPartition[T]) error {
	executors := this.executorData.Mpi().Executors()
	rank := this.executorData.Mpi().Rank()
	context := this.Context()
	partiall := partial.Inner().(storage.IList)
	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Reduce: reducing all elements in the executor")
	if partial.Size() > 1 {
		if this.executorData.GetCores() == 1 {
			elem, err := reducePartition[T](this, f, partial)
			if err != nil {
				return ierror.Raise(err)
			}
			list := partial.Inner().(storage.IList)
			list.SetAny(0, elem)
			list.Resize(1, true)
		} else {
			if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
				n := int(partial.Size())
				n2 := n / 2
				for n2 > 0 {
					if err := rctx.For().Static().Run(n2, func(i int) error {
						elem, err := f.Call(partiall.GetAny(i).(T), partiall.GetAny(n-i-1).(T), context)
						if err != nil {
							return ierror.Raise(err)
						}
						partiall.SetAny(i, elem)
						return nil
					}); err != nil {
						return ierror.Raise(err)
					}
					n = int(math.Ceil(float64(n) / 2))
					n2 = n / 2
				}
				return nil
			}); err != nil {
				return ierror.Raise(err)
			}
		}
		partiall.Resize(1, false)
	}

	logger.Info("Reduce: performing a final tree reduce")
	distance := 1
	order := 1
	for order < executors {
		order *= 2
		if rank%order == 0 {
			other := rank + distance
			distance = order
			if other >= executors {
				continue
			}
			if err := core.Recv[T](this.executorData.Mpi(), partial, other, 0); err != nil {
				return ierror.Raise(err)
			}
			elem, err := f.Call(partiall.GetAny(0).(T), partiall.GetAny(1).(T), context)
			if err != nil {
				return ierror.Raise(err)
			}
			partiall.SetAny(0, elem)
			partiall.Resize(1, false)
		} else {
			other := rank * distance
			if err := core.Send[T](this.executorData.Mpi(), partial, other, 0); err != nil {
				return ierror.Raise(err)
			}
			break
		}
	}

	if this.executorData.Mpi().IsRoot(0) && partial.Size() > 0 {
		result, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), 1)
		if err != nil {
			return ierror.Raise(err)
		}
		result.Inner().(storage.IList).AddAny(partiall.GetAny(0))
		output.Add(result)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func aggregatePartition[T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T], part storage.IPartition[T2], acum T) (T, error) {
	context := this.Context()
	reader, err := part.ReadIterator()
	if err != nil {
		return acum, ierror.Raise(err)
	}

	for reader.HasNext() {
		elem, err := reader.Next()
		if err != nil {
			return acum, ierror.Raise(err)
		}
		acum, err = f.Call(acum, elem, context)
		if err != nil {
			return acum, ierror.Raise(err)
		}
	}

	return acum, nil
}

func localReduceByKey[K comparable, T any](this *IReduceImpl, f function.IFunction2[T, T, T]) error {
	input, err := core.GetPartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output := input
	if output.Cache() {
		if output, err = core.NewPartitionGroupDef[ipair.IPair[K, T]](this.executorData.GetPartitionTools()); err != nil {
			return ierror.Raise(err)
		}
		for p := 0; p < input.Size(); p++ {
			part, err := core.NewPartitionWithName[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), input.Get(0).Type())
			if err != nil {
				return ierror.Raise(err)
			}
			output.Add(part)
		}
	}

	context := this.executorData.GetContext()
	if err = ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		acum := map[K]T{}
		return rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if val, present := acum[elem.First]; !present {
					acum[elem.First] = elem.Second
				} else {
					acum[elem.First], err = f.Call(val, elem.Second, context)
					if err != nil {
						return ierror.Raise(err)
					}
				}
			}
			if err = output.Get(p).Clear(); err != nil {
				return ierror.Raise(err)
			}
			writer, err := output.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for key, value := range acum {
				if err = writer.Write(*ipair.New(key, value)); err != nil {
					return ierror.Raise(err)
				}
			}
			acum = map[K]T{}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func localAggregateByKey[K comparable, T any, T2 any](this *IReduceImpl, f function.IFunction2[T, T2, T]) error {
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, T2]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}
	context := this.executorData.GetContext()
	baseAcum := core.GetVariable[T](this.executorData, "zero")
	if err = ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		acum := map[K]T{}
		return rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if val, present := acum[elem.First]; !present {
					acum[elem.First], err = f.Call(baseAcum, elem.Second, context)
				} else {
					acum[elem.First], err = f.Call(val, elem.Second, context)
				}
				if err != nil {
					return ierror.Raise(err)
				}
			}
			acum = map[K]T{}
			input.SetBase(p, nil)
			if err = output.Get(p).Fit(); err != nil {
				return ierror.Raise(err)
			}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func keyHashing[K comparable, T any](this *IReduceImpl, numPartitions int64) error {
	input, err := core.GetPartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), int(numPartitions))
	if err != nil {
		return ierror.Raise(err)
	}
	hasher := utils.GetHasher(utils.TypeObj[K]())
	logger.Info("Reduce: creating ", numPartitions, " new partitions with key hashing")

	if err = ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		threadRanges, err := core.NewPartitionGroupWithSize[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), output.Size())
		if err != nil {
			return ierror.Raise(err)
		}
		writers := make([]iterator.IWriteIterator[ipair.IPair[K, T]], threadRanges.Size())
		for p := 0; p < threadRanges.Size(); p++ {
			writers[p], err = threadRanges.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
		}
		if err = rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = writers[utils.Hash(elem.First, hasher)%uint64(numPartitions)].Write(elem); err != nil {
					return ierror.Raise(err)
				}
			}
			input.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		return rctx.Critical(func() error {
			for p, part := range threadRanges.Iter() {
				if err := part.MoveTo(output.Get(p)); err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func keyExchanging[K comparable, T any](this *IReduceImpl) error {
	input, err := core.GetPartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupDef[ipair.IPair[K, T]](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Reduce: exchanging ", input.Size(), " partitions keys")

	if err = Exchange(this.Base(), input, output); err != nil {
		return ierror.Raise(err)
	}

	core.SetPartitions(this.executorData, output)
	return nil
}

func distinctFilter[T comparable](this *IReduceImpl, parts *storage.IPartitionGroup[T]) error {
	return ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		distinct := map[T]bool{}
		return rctx.For().Dynamic().Run(parts.Size(), func(p int) error {
			newPart, err := core.NewPartitionDef[T](this.executorData.GetPartitionTools())
			if err != nil {
				return ierror.Raise(err)
			}
			reader, err := parts.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writer, err := newPart.WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				distinct[elem] = true
			}
			for elem, _ := range distinct {
				if err = writer.Write(elem); err != nil {
					return ierror.Raise(err)
				}
			}
			parts.Set(p, newPart)
			distinct = map[T]bool{}
			return nil
		})
	})
}
