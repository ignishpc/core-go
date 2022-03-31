package impl

import (
	"ignis/executor/api"
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
	"sort"
)

type ISortImpl struct {
	IBaseImpl
}

func NewISortImpl(executorData *core.IExecutorData) *ISortImpl {
	return &ISortImpl{
		IBaseImpl{executorData},
	}
}

func Sort[T any](this *ISortImpl, ascending bool) error {
	return SortWithPartitions[T](this, ascending, -1)
}

func SortWithPartitions[T any](this *ISortImpl, ascending bool, partitions int64) error {
	f, err := defaultCmp[T]()
	if err != nil {
		return ierror.Raise(err)
	}
	return sortImpl(this, f, ascending, partitions, true)
}

func SortBy[T any](this *ISortImpl, f function.IFunction2[T, T, bool], ascending bool) error {
	return SortByWithPartitions[T](this, f, ascending, -1)
}

func SortByWithPartitions[T any](this *ISortImpl, f function.IFunction2[T, T, bool], ascending bool, partitions int64) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if err := sortImpl[T](this, func(a T, b T) bool {
		less, err := f.Call(a, b, context)
		if err != nil {
			panic(err)
		}
		return less
	}, ascending, partitions, true); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func Top[T any](this *ISortImpl, n int64) error {
	f, err := defaultCmp[T]()
	if err != nil {
		return ierror.Raise(err)
	}
	return takeOrderedImpl(this, f, false, n)
}

func TopBy[T any](this *ISortImpl, f function.IFunction2[T, T, bool], n int64) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if err := takeOrderedImpl[T](this, func(a T, b T) bool {
		less, err := f.Call(a, b, context)
		if err != nil {
			panic(err)
		}
		return less
	}, false, n); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func TakeOrdered[T any](this *ISortImpl, n int64) error {
	f, err := defaultCmp[T]()
	if err != nil {
		return ierror.Raise(err)
	}
	return takeOrderedImpl(this, f, true, n)
}

func TakeOrderedBy[T any](this *ISortImpl, f function.IFunction2[T, T, bool], n int64) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if err := takeOrderedImpl[T](this, func(a T, b T) bool {
		less, err := f.Call(a, b, context)
		if err != nil {
			panic(err)
		}
		return less
	}, true, n); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func Max[T any](this *ISortImpl) error {
	f, err := defaultCmp[T]()
	if err != nil {
		return ierror.Raise(err)
	}
	return maxImpl(this, f, false)
}

func Min[T any](this *ISortImpl) error {
	f, err := defaultCmp[T]()
	if err != nil {
		return ierror.Raise(err)
	}
	return maxImpl(this, f, true)
}

func MaxBy[T any](this *ISortImpl, f function.IFunction2[T, T, bool]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if err := maxImpl[T](this, func(a T, b T) bool {
		less, err := f.Call(a, b, context)
		if err != nil {
			panic(err)
		}
		return less
	}, false); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func MinBy[T any](this *ISortImpl, f function.IFunction2[T, T, bool]) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if err := maxImpl[T](this, func(a T, b T) bool {
		less, err := f.Call(a, b, context)
		if err != nil {
			panic(err)
		}
		return less
	}, true); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func SortByKey[T any, K any](this *ISortImpl, ascending bool) error {
	return SortByKeyWithPartitions[T, K](this, ascending, -1)
}

func SortByKeyWithPartitions[T any, K any](this *ISortImpl, ascending bool, partitions int64) error {
	f, err := defaultCmp[K]()
	if err != nil {
		return ierror.Raise(err)
	}
	if _, ok := this.executorData.GetPartitionsAny().First().(ipair.IPair[K, T]); ok {
		pf := func(a, b ipair.IPair[K, T]) bool {
			return f(a.First, b.First)
		}
		return sortImpl[ipair.IPair[K, T]](this, pf, ascending, partitions, true)
	} else {
		pf := func(a, b ipair.IPair[any, any]) bool {
			return f(a.GetFirst().(K), b.GetFirst().(K))
		}
		return sortImpl[ipair.IPair[any, any]](this, pf, ascending, partitions, true)
	}
}

func SortByKeyBy[T any, K any](this *ISortImpl, f function.IFunction2[K, K, bool], ascending bool) error {
	return SortByKeyByWithPartitions[T, K](this, f, ascending, -1)
}

func SortByKeyByWithPartitions[T any, K any](this *ISortImpl, f function.IFunction2[K, K, bool], ascending bool, partitions int64) error {
	context := this.Context()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if _, ok := this.executorData.GetPartitionsAny().First().(*ipair.IPair[K, T]); ok {
		pf := func(a, b ipair.IPair[K, T]) bool {
			less, err := f.Call(a.First, b.First, context)
			if err != nil {
				panic(err)
			}
			return less
		}
		if err := sortImpl[ipair.IPair[K, T]](this, pf, ascending, partitions, true); err != nil {
			return ierror.Raise(err)
		}
	} else {
		pf := func(a, b any) bool {
			less, err := f.Call(a.(ipair.IAbstractPair).GetFirst().(K), b.(ipair.IAbstractPair).GetFirst().(K), context)
			if err != nil {
				panic(err)
			}
			return less
		}
		if err := sortImpl[any](this, pf, ascending, partitions, true); err != nil {
			return ierror.Raise(err)
		}
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func sortImpl[T any](this *ISortImpl, f func(T, T) bool, ascending bool, partitions int64, localSort bool) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	executors := this.executorData.Mpi().Executors()
	/*Copy the data if they are reused*/
	if input.Cache() {
		/*Work directly on the array to improve performance*/
		if this.executorData.GetPartitionTools().IsMemoryGroup(input) {
			group, err := input.Clone()
			if err != nil {
				return ierror.Raise(err)
			}
			input = group
		} else {
			/*Only group will be affected*/
			input = input.ShadowCopy()
		}
	}

	/*Sort each partition*/
	logger.Info("Sort: sorting ", input.Size(), " partitions locally")
	if err := parallelLocalSort[T](this, f, input, ascending); err != nil {
		return ierror.Raise(err)
	}

	localPartitions := impi.C_int64(input.Size())
	totalPartitions := impi.C_int64(0)
	if err := impi.MPI_Allreduce(impi.P(&localPartitions), impi.P(&totalPartitions), 1, impi.MPI_LONG_LONG_INT,
		impi.MPI_SUM, this.executorData.Mpi().Native()); err != nil {
		return ierror.Raise(err)
	}
	if totalPartitions < 2 {
		core.SetPartitions(this.executorData, input)
		return nil
	}
	if partitions < 0 {
		partitions = int64(totalPartitions)
	}

	/*Generates pivots to separate the elements in order*/
	sr, err := this.executorData.GetProperties().SortSamples()
	if err != nil {
		return ierror.Raise(err)
	}
	samples := int64(0)
	if sr > 1 || sr == 0 {
		samples = int64(sr)
	} else {
		send := []impi.C_int64{0, 0}
		rcv := []impi.C_int64{0, 0}
		send[0] = impi.C_int64(input.Size())
		for _, part := range input.Iter() {
			send[1] += impi.C_int64(part.Size())
		}
		if err := impi.MPI_Allreduce(impi.P(&send[0]), impi.P(&rcv[0]), 2, impi.MPI_LONG_LONG_INT,
			impi.MPI_SUM, this.executorData.Mpi().Native()); err != nil {
			return ierror.Raise(err)
		}
		samples = int64(math.Ceil(float64(rcv[1]) / float64(rcv[0]) * sr))
	}

	samples = utils.Max(partitions, samples)
	logger.Info("Sort: selecting ", samples, " pivots")
	pivots, err := selectPivots[T](this, f, input, ascending, samples)
	if err != nil {
		return ierror.Raise(err)
	}

	resampling, err := this.executorData.GetProperties().SortResampling()
	if err != nil {
		return ierror.Raise(err)
	}
	if sr < 1 && resampling && executors > 1 && localSort {
		logger.Info("Sort: -- resampling pivots begin --")
		tmp, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		tmp.Add(pivots)
		core.SetPartitions(this.executorData, tmp)
		if err = sortImpl(this, f, ascending, int64(executors*this.executorData.GetCores()), false); err != nil {
			return ierror.Raise(err)
		}
		logger.Info("Sort: -- resampling pivots end --")
		samples = partitions - 1
		logger.Info("Sort: selecting ", samples, " partition pivots")
		if pivots, err = parallelSelectPivots[T](this, samples); err != nil {
			return ierror.Raise(err)
		}
		logger.Info("Sort: collecting pivots")
		if err = core.Gather[T](this.executorData.Mpi(), pivots, 0); err != nil {
			return ierror.Raise(err)
		}
	} else {
		logger.Info("Sort: collecting pivots")
		if err = core.Gather[T](this.executorData.Mpi(), pivots, 0); err != nil {
			return ierror.Raise(err)
		}

		if this.executorData.Mpi().IsRoot(0) {
			group, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
			if err != nil {
				return ierror.Raise(err)
			}
			group.Add(pivots)
			if err := parallelLocalSort[T](this, f, group, ascending); err != nil {
				return ierror.Raise(err)
			}
			samples = partitions - 1

			logger.Info("Sort: selecting ", samples, " partition pivots")
			if pivots, err = selectPivots[T](this, f, group, ascending, samples); err != nil {
				return ierror.Raise(err)
			}
		}
	}

	logger.Info("Sort: broadcasting pivots ranges")
	if err = core.Bcast[T](this.executorData.Mpi(), pivots, 0); err != nil {
		return ierror.Raise(err)
	}

	ranges, err := generateRanges[T](this, f, input, ascending, pivots)
	if err != nil {
		return ierror.Raise(err)
	}
	if err = pivots.Clear(); err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Sort: exchanging ranges")
	if err = Exchange(this.Base(), ranges, output); err != nil {
		return ierror.Raise(err)
	}

	/*Sort final partitions*/
	logger.Info("Sort: sorting again ", output.Size(), " partitions locally")
	if err := parallelLocalSort[T](this, f, output, ascending); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func parallelLocalSort[T any](this *ISortImpl, f func(T, T) bool, group *storage.IPartitionGroup[T], ascending bool) error {
	inMemory := this.executorData.GetPartitionTools().IsMemoryGroup(group)
	/*Sort each partition locally*/
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(i int) error {
			if inMemory {
				sortPartition[T](this, f, group.Get(i).(*storage.IMemoryPartition[T]), ascending)
			} else {
				tmp := storage.NewIMemoryPartition[T](group.Get(i).Size(), false)
				if err := group.Get(i).CopyTo(tmp); err != nil {
					return ierror.Raise(err)
				}
				sortPartition[T](this, f, tmp, ascending)
				if err := group.Get(i).CopyFrom(tmp); err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func sortPartition[T any](this *ISortImpl, f func(T, T) bool, part *storage.IMemoryPartition[T], ascending bool) {
	var data sort.Interface
	list := part.Inner().(storage.IList)
	if a, fast := list.Array().([]T); fast {
		data = &arrayCmp[T]{a, ascending, f}
	} else {
		data = &listCmp[T]{list, ascending, f}
	}
	sort.Sort(data)
}

func selectPivots[T any](this *ISortImpl, f func(T, T) bool, group *storage.IPartitionGroup[T], ascending bool, samples int64) (*storage.IMemoryPartition[T], error) {
	if this.executorData.GetPartitionTools().IsMemoryGroup(group) {
		return selectMemoryPivots(this, f, group, ascending, samples)
	}
	pivots, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), samples)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	writer, err := pivots.WriteIterator()
	if err != nil {
		return nil, ierror.Raise(err)
	}

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			if group.Get(p).Size() < samples {
				return ierror.Raise(group.Get(p).CopyTo(pivots))
			}

			skip := (group.Get(p).Size() - samples) / (samples + 1)
			rem := (group.Get(p).Size() - samples) % (samples + 1)
			reader, err := group.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for n := int64(0); n < samples; n++ {
				for i := int64(0); i < skip; i++ {
					if _, err := reader.Next(); err != nil {
						return ierror.Raise(err)
					}
				}
				if n < rem {
					if _, err := reader.Next(); err != nil {
						return ierror.Raise(err)
					}
				}
				if err = rctx.Critical(func() error {
					if elem, err := reader.Next(); err != nil {
						return ierror.Raise(err)
					} else {
						return writer.Write(elem)
					}
				}); err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		})
	}); err != nil {
		return nil, ierror.Raise(err)
	}
	return pivots, nil
}

func selectMemoryPivots[T any](this *ISortImpl, f func(T, T) bool, group *storage.IPartitionGroup[T], ascending bool, samples int64) (*storage.IMemoryPartition[T], error) {
	pivots, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), samples)
	if err != nil {
		return nil, ierror.Raise(err)
	}

	threadPivots := make([]storage.IPartition[T], this.executorData.GetCores())
	for i := 0; i < this.executorData.GetCores(); i++ {
		part, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), samples)
		if err != nil {
			return nil, ierror.Raise(err)
		}
		threadPivots[i] = part
	}

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			if group.Get(p).Size() < samples {
				return ierror.Raise(group.Get(p).CopyTo(threadPivots[rctx.ThreadId()]))
			}
			writer, err := threadPivots[rctx.ThreadId()].WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}

			skip := (group.Get(p).Size() - samples) / (samples + 1)
			rem := (group.Get(p).Size() - samples) % (samples + 1)
			pos := skip + int64(utils.Ternary(rem > 0, 1, 0))
			list := group.Get(p).Inner().(storage.IList)
			if array, ok := list.Array().([]T); ok {
				for n := int64(0); n < samples; n++ {
					if err = writer.Write(array[pos]); err != nil {
						return ierror.Raise(err)
					}
					pos += skip + 1
					if n < rem-1 {
						pos++
					}
				}
			} else {
				for n := int64(0); n < samples; n++ {
					if err = writer.Write(list.GetAny(p).(T)); err != nil {
						return ierror.Raise(err)
					}
					pos += skip + 1
					if n < rem-1 {
						pos++
					}
				}
			}
			return nil
		})
	}); err != nil {
		return nil, ierror.Raise(err)
	}

	for _, part := range threadPivots {
		if err := part.MoveTo(pivots); err != nil {
			return nil, ierror.Raise(err)
		}
	}
	return pivots, nil
}

func parallelSelectPivots[T any](this *ISortImpl, samples int64) (*storage.IMemoryPartition[T], error) {
	rank := this.executorData.Mpi().Rank()
	executors := this.executorData.Mpi().Executors()
	result, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), samples)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	aux := make([]impi.C_int64, executors)
	sz := impi.C_int64(0)
	disp := int64(0)
	pos := int64(0)
	sample := int64(0)
	tmp, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	for _, part := range tmp.Iter() {
		sz += impi.C_int64(part.Size())
	}

	if err = impi.MPI_Allgather(impi.P(&sz), 1, impi.MPI_LONG_LONG_INT, impi.P(&aux[0]), 1,
		impi.MPI_LONG_LONG_INT, this.executorData.Mpi().Native()); err != nil {
		return nil, ierror.Raise(err)
	}

	sz = 0
	for i := 0; i < executors; i++ {
		sz += aux[i]
	}
	for i := 0; i < rank; i++ {
		disp += int64(aux[i])
	}

	skip := (int64(sz) - samples) / (samples + 1)
	rem := (int64(sz) - samples) % (samples + 1)

	pos = skip + int64(utils.Ternary(rem > 0, 1, 0))
	for sample = 0; sample < samples; sample++ {
		if pos >= disp {
			break
		}
		if sample < rem-1 {
			pos += skip + 2
		} else {
			pos += skip + 1
		}
	}
	pos -= disp

	writer, err := result.WriteIterator()
	if err != nil {
		return nil, ierror.Raise(err)
	}

	for _, part := range tmp.Iter() {
		list := part.Inner().(storage.IList)
		if array, ok := list.Array().([]T); ok {
			for pos < part.Size() && sample < samples {
				if err := writer.Write(array[pos]); err != nil {
					return nil, ierror.Raise(err)
				}
				pos += skip + 1
				if sample < rem-1 {
					pos++
				}
				sample++
			}
		} else {
			for pos < part.Size() && sample < samples {
				if err := writer.Write(list.GetAny(int(pos)).(T)); err != nil {
					return nil, ierror.Raise(err)
				}
				pos += skip + 1
				if sample < rem-1 {
					pos++
				}
				sample++
			}
		}
		pos -= part.Size()
	}

	return result, nil
}

func generateRanges[T any](this *ISortImpl, f func(T, T) bool, group *storage.IPartitionGroup[T], ascending bool, pivots *storage.IMemoryPartition[T]) (*storage.IPartitionGroup[T], error) {
	if this.executorData.GetPartitionTools().IsMemoryGroup(group) {
		return generateMemoryRanges(this, f, group, ascending, pivots)
	}
	ranges, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), int(pivots.Size()+1))
	if err != nil {
		return nil, ierror.Raise(err)
	}
	pivotsList := pivots.Inner().(storage.IList)

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		threadRanges, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), ranges.Size())
		writers := make([]iterator.IWriteIterator[T], ranges.Size())
		if err != nil {
			return ierror.Raise(err)
		}
		for i, part := range threadRanges.Iter() {
			it, err := part.WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writers[i] = it
		}

		if err := rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			reader, err := group.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				err = writers[searchRange(this, f, elem, ascending, pivotsList)].Write(elem)
				if err != nil {
					return ierror.Raise(err)
				}
			}
			group.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}

		return rctx.Critical(func() error {
			for p := 0; p < threadRanges.Size(); p++ {
				if err := threadRanges.Get(p).MoveTo(ranges.Get(p)); err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		})

	}); err != nil {
		return nil, ierror.Raise(err)
	}
	group.Clear()
	return ranges, nil
}

func generateMemoryRanges[T any](this *ISortImpl, f func(T, T) bool, group *storage.IPartitionGroup[T], ascending bool, pivots *storage.IMemoryPartition[T]) (*storage.IPartitionGroup[T], error) {
	ranges, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), int(pivots.Size()+1))
	if err != nil {
		return nil, ierror.Raise(err)
	}
	pivotsList := pivots.Inner().(storage.IList)

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		threadRanges, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), ranges.Size())
		writers := make([]iterator.IWriteIterator[T], ranges.Size())
		if err != nil {
			return ierror.Raise(err)
		}
		for i, part := range threadRanges.Iter() {
			it, err := part.WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writers[i] = it
		}

		if err := rctx.For().Dynamic().Run(group.Size(), func(p int) error {
			part := group.Get(p)
			list := part.Inner().(storage.IList)
			if part.Empty() {
				return nil
			}
			elemsStack := []ipair.IPair[int64, int64]{*ipair.New(int64(0), part.Size()-1)}
			rangesStack := []ipair.IPair[int64, int64]{*ipair.New(int64(0), pivots.Size())}

			for len(elemsStack) > 0 {
				back := len(elemsStack) - 1
				start := elemsStack[back].First
				end := elemsStack[back].Second
				mid := (start + end) / 2
				elemsStack = elemsStack[:back]

				back = len(rangesStack) - 1
				first := rangesStack[back].First
				last := rangesStack[back].Second
				rangesStack = rangesStack[:back]

				elem := list.GetAny(int(mid)).(T)
				r := searchRange(this, f, elem, ascending, pivotsList)
				if err := writers[r].Write(elem); err != nil {
					return ierror.Raise(err)
				}

				if first == r {
					if array, ok := list.Array().([]T); ok {
						for i := start; i < mid; i++ {
							if err := writers[r].Write(array[i]); err != nil {
								return ierror.Raise(err)
							}
						}
					} else {
						for i := start; i < mid; i++ {
							if err := writers[r].Write(list.GetAny(int(i)).(T)); err != nil {
								return ierror.Raise(err)
							}
						}
					}
				} else if start < mid {
					elemsStack = append(elemsStack, *ipair.New(start, mid-1))
					rangesStack = append(rangesStack, *ipair.New(first, r))
				}

				if r == last {
					if array, ok := list.Array().([]T); ok {
						for i := mid + 1; i <= end; i++ {
							if err := writers[r].Write(array[i]); err != nil {
								return ierror.Raise(err)
							}
						}
					} else {
						for i := mid + 1; i <= end; i++ {
							if err := writers[r].Write(list.GetAny(int(i)).(T)); err != nil {
								return ierror.Raise(err)
							}
						}
					}
				} else if mid < end {
					elemsStack = append(elemsStack, *ipair.New(mid+1, end))
					rangesStack = append(rangesStack, *ipair.New(r, last))
				}

			}
			group.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}

		return rctx.Critical(func() error {
			for p := 0; p < threadRanges.Size(); p++ {
				if err := threadRanges.Get(p).MoveTo(ranges.Get(p)); err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		})

	}); err != nil {
		return nil, ierror.Raise(err)
	}
	group.Clear()
	return ranges, nil
}

func searchRange[T any](this *ISortImpl, f func(T, T) bool, elem T, ascending bool, pivots storage.IList) int64 {
	start := int64(0)
	end := int64(pivots.Size() - 1)
	mid := int64(0)

	if array, ok := pivots.Array().([]T); ok {
		for start < end {
			mid = (start + end) / 2
			if f(elem, array[mid]) == ascending {
				end = mid - 1
			} else {
				start = mid + 1
			}
		}
		if f(elem, array[start]) == ascending {
			return start
		} else {
			return start + 1
		}
	} else {
		for start < end {
			mid = (start + end) / 2
			if f(elem, pivots.GetAny(int(mid)).(T)) == ascending {
				end = mid - 1
			} else {
				start = mid + 1
			}
		}
		if f(elem, pivots.GetAny(int(start)).(T)) == ascending {
			return start
		} else {
			return start + 1
		}
	}
}

func takeOrderedImpl[T any](this *ISortImpl, f func(T, T) bool, ascending bool, n int64) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Sort: top/takeOrdered ", n, " elements")
	top, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), n*int64(input.Size()))
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Sort: local partition top/takeOrdered")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		localTop, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), n)
		if err != nil {
			return ierror.Raise(err)
		}
		if err := rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			ltop := localTop.Inner().(*storage.IListImpl[T])
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				err = takeOrderedAdd(this, f, ascending, ltop, elem, n)
				if err != nil {
					return ierror.Raise(err)
				}
			}
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		return rctx.Critical(func() error {
			return ierror.Raise(localTop.MoveTo(top))
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Sort: local executor top/takeOrdered")
	sortPartition[T](this, f, top, ascending)
	top.Inner().(storage.IList).Resize(int(n), false)
	logger.Info("Sort: global top/takeOrdered")
	if err := core.Gather[T](this.executorData.Mpi(), top, 0); err != nil {
		return ierror.Raise(err)
	}

	if this.executorData.Mpi().IsRoot(0) {
		sortPartition[T](this, f, top, ascending)
		top.Inner().(storage.IList).Resize(int(n), false)
		ouput.Add(top)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}

func takeOrderedAdd[T any](this *ISortImpl, f func(T, T) bool, ascending bool, top *storage.IListImpl[T], elem T, n int64) error {
	if top.Size() == 0 {
		top.Add(elem)
		return nil
	}

	if top.Size() == int(n) {
		back := top.Get(int(n - 1))
		if f(back, elem) != ascending || f(elem, back) != ascending {
			return nil
		}
		top.Resize(int(n-1), false)
	}
	i := int(searchRange[T](this, f, elem, ascending, top))
	top.Insert(i)
	top.Set(i, elem)
	return nil
}

func maxImpl[T any](this *ISortImpl, f func(T, T) bool, ascending bool) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Sort: max/min")
	first := this.executorData.GetPartitionsAny().First()
	if first == nil {
		core.SetPartitions(this.executorData, ouput)
		return nil
	}

	logger.Info("Sort: local max/min")

	elem := first.(T)

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		localElem := elem
		if err := rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if f(localElem, elem) != ascending {
					localElem = elem
				}
			}
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		return rctx.Critical(func() error {
			if f(elem, localElem) != ascending {
				elem = localElem
			}
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Sort: global max/min")
	result, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), int64(this.executorData.Mpi().Executors()))
	if err != nil {
		return ierror.Raise(err)
	}
	list := result.Inner().(*storage.IListImpl[T])
	list.Add(elem)
	if err := core.Gather[T](this.executorData.Mpi(), result, 0); err != nil {
		return ierror.Raise(err)
	}

	if this.executorData.Mpi().IsRoot(0) {
		for i := 0; i < list.Size(); i++ {
			if f(elem, list.Get(i)) != ascending {
				elem = list.Get(i)
			}
		}
		list.Resize(1, false)
		list.Set(0, elem)
		ouput.Add(result)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}

func defaultCmp[T any]() (func(T, T) bool, error) {
	var f any
	var t any = new(T)
	switch v := t.(type) {
	case *int:
		f = func(a, b int) bool { return a < b }
	case *int8:
		f = func(a, b int8) bool { return a < b }
	case *int16:
		f = func(a, b int16) bool { return a < b }
	case *int32:
		f = func(a, b int32) bool { return a < b }
	case *int64:
		f = func(a, b int64) bool { return a < b }
	case *uint:
		f = func(a, b uint) bool { return a < b }
	case *uint8:
		f = func(a, b uint8) bool { return a < b }
	case *uint16:
		f = func(a, b uint16) bool { return a < b }
	case *uint32:
		f = func(a, b uint32) bool { return a < b }
	case *uint64:
		f = func(a, b uint64) bool { return a < b }
	case *float32:
		f = func(a, b float32) bool { return a < b || (a != a && b == b) }
	case *float64:
		f = func(a, b float64) bool { return a < b || (a != a && b == b) }
	case *string:
		f = func(a, b string) bool { return a < b }
	case api.Sortable[T]:
		f = v.Less
	default:
		return nil, ierror.RaiseMsg(utils.TypeName[T]() + " is not [int, int8 , int16, int32 , int64, uint , uint8, uint16, " +
			"uint32, uint64 , float32, float64, string] o implement api.Sortable")
	}
	return f.(func(T, T) bool), nil
}

type arrayCmp[T any] struct {
	array     []T
	ascending bool
	f         func(T, T) bool
}

func (this *arrayCmp[T]) Len() int {
	return len(this.array)
}

func (this *arrayCmp[T]) Less(i, j int) bool {
	return this.f(this.array[i], this.array[j]) == this.ascending
}

func (this *arrayCmp[T]) Swap(i, j int) {
	this.array[i], this.array[j] = this.array[j], this.array[i]
}

type listCmp[T any] struct {
	list      storage.IList
	ascending bool
	f         func(T, T) bool
}

func (this *listCmp[T]) Len() int {
	return this.list.Size()
}

func (this *listCmp[T]) Less(i, j int) bool {
	return this.f(this.list.GetAny(i).(T), this.list.GetAny(j).(T)) == this.ascending
}

func (this *listCmp[T]) Swap(i, j int) {
	aux := this.list.GetAny(i)
	this.list.SetAny(i, this.list.GetAny(j))
	this.list.SetAny(j, aux)
}
