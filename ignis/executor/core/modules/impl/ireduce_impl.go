package impl

import (
	"ignis/executor/api/function"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
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
	return ierror.Raise(f.After(context))
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
	return ierror.Raise(f.After(context))
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
	return ierror.Raise(f.After(context))
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
	return ierror.Raise(f.After(context))
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
	return ierror.Raise(f.After(context))
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

func Union[T any](this *IReduceImpl, other string, preserveOrder bool) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Join[T any](this *IReduceImpl, other string, numPartitions int64) error {
	return ierror.RaiseMsg("TODO") //TODO
}

func Distinct[T comparable](this *IReduceImpl, numPartitions int64) error {
	return ierror.RaiseMsg("TODO") //TODO
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
