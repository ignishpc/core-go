package impl

import (
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
	"math/rand"
	"reflect"
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
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	isMemory := this.executorData.GetPartitionTools().IsMemoryGroup(input)
	output, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Math: sample ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		id := ithreads.ThreadId()
		dist := rand.New(rand.NewSource(int64(int(seed) + id)))
		return rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			writer, err := output.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			part := input.Get(p)
			size := part.Size()
			if !isMemory {
				aux, err := core.NewMemoryPartition[T](this.executorData.GetPartitionTools(), int64(part.Size()))
				if err != nil {
					return ierror.Raise(err)
				}
				if err = part.CopyTo(aux); err != nil {
					return ierror.Raise(err)
				}
				part = aux
			}
			list := part.Inner().(storage.IList)
			array, ok := list.Array().([]T)

			if withReplacement {
				if ok {
					for i := int64(0); i < num[p]; i++ {
						if err := writer.Write(array[dist.Intn(int(size-1))]); err != nil {
							return ierror.Raise(err)
						}
					}
				} else {
					for i := int64(0); i < num[p]; i++ {
						if err := writer.Write(list.GetAny(dist.Intn(int(size - 1))).(T)); err != nil {
							return ierror.Raise(err)
						}
					}
				}
			} else {
				picked := int64(0)
				for i := int64(0); i < int64(list.Size()) && num[p] > picked; i++ {
					prob := float64(num[p]-picked) / float64(size-i)
					random := dist.Float64()
					if random < prob {
						if ok {
							if err := writer.Write(array[i]); err != nil {
								return ierror.Raise(err)
							}
						} else {
							if err := writer.Write(list.GetAny(int(i)).(T)); err != nil {
								return ierror.Raise(err)
							}
						}
						picked++
					}
				}

			}
			input.SetBase(p, nil)
			return ierror.Raise(output.Get(p).Fit())
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func (this *IMathImpl) Count() (int64, error) {
	group := this.executorData.GetPartitionsAny()
	n := int64(0)
	for i := 0; i < group.Size(); i++ {
		n += group.GetBase(i).Size()
	}
	return n, nil
}

func SampleByKeyFilter[T any, K comparable](this *IMathImpl) (int64, error) {
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return 0, ierror.Raise(err)
	}
	tmp, err := core.NewPartitionGroupWithSize[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return 0, ierror.Raise(err)
	}
	fractions := this.Context().Vars()["fractions"].(map[K]float64)

	logger.Info("Math: filtering key before sample ", +input.Size(), " partitions")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		return rctx.For().Dynamic().Run(input.Size(), func(p int) error {
			reader, err := input.Get(p).ReadIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			writer, err := tmp.Get(p).WriteIterator()
			if err != nil {
				return ierror.Raise(err)
			}
			for reader.HasNext() {
				elem, err := reader.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if _, present := fractions[elem.First]; present {
					if err = writer.Write(elem); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			input.SetBase(p, nil)
			return ierror.Raise(tmp.Get(p).Fit())
		})
	}); err != nil {
		return 0, ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return 0, ierror.Raise(err)
	}
	for _, part := range tmp.Iter() {
		if !part.Empty() {
			output.Add(part)
		}
	}
	numPartitions := impi.C_int64(utils.Min(output.Size(), len(fractions)))
	if err := impi.MPI_Allreduce(impi.MPI_IN_PLACE, impi.P(&numPartitions), 1, impi.MPI_LONG, impi.MPI_MAX,
		this.executorData.Mpi().Native()); err != nil {
		return 0, ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return int64(numPartitions), nil
}

func SampleByKey[T any, K comparable](this *IMathImpl, withReplacement bool, seed int32) error {
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, []T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupWithSize[ipair.IPair[K, T]](this.executorData.GetPartitionTools(), input.Size())
	if err != nil {
		return ierror.Raise(err)
	}
	fractions := this.Context().Vars()["fractions"].(map[K]float64)
	num := make([]int64, len(fractions))
	pmap := map[K]int{}
	for key, _ := range fractions {
		pmap[key] = output.Size()
		part, err := core.NewPartitionDef[ipair.IPair[K, T]](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		output.Add(part)
	}
	logger.Info("Math: sampleByKey copying values to single partition")
	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
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
				pos := pmap[elem.First]
				num[pos] = int64(float64(len(elem.Second)) * fractions[elem.First])
				writer, err := output.Get(p).WriteIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				for _, value := range elem.Second {
					if err = writer.Write(*ipair.New[K, T](elem.First, value)); err != nil {
						return ierror.Raise(err)
					}
				}
			}
			input.SetBase(p, nil)
			return nil
		})
	}); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, output)
	return Sample[T](this, withReplacement, num, seed)
}

func CountByKey[T any, K comparable](this *IMathImpl) error {
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	threads := this.executorData.GetCores()
	acum := make([]map[K]int64, threads)
	logger.Info("Math: counting local keys ", input.Size(), " partitions")

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		id := ithreads.ThreadId()
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
				acum[id][elem.First]++
			}
			input.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		distance := 1
		order := 1
		for order < threads {
			rctx.Barrier()
			order *= 2
			if id%order == 0 {
				other := id + distance
				distance = order
				if order >= threads {
					continue
				}
				for key, value := range acum[other] {
					acum[id][key] += value
				}
			}
		}
		return nil
	}); err != nil {
		return ierror.Raise(err)
	}
	return countByReduce[K](this, acum[0])
}

func CountByValue[T comparable, K any](this *IMathImpl) error {
	input, err := core.GetAndDeletePartitions[ipair.IPair[K, T]](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	threads := this.executorData.GetCores()
	acum := make([]map[T]int64, threads)
	logger.Info("Math: counting local keys ", input.Size(), " partitions")

	if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
		id := ithreads.ThreadId()
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
				acum[id][elem.Second]++
			}
			input.SetBase(p, nil)
			return nil
		}); err != nil {
			return ierror.Raise(err)
		}
		distance := 1
		order := 1
		for order < threads {
			rctx.Barrier()
			order *= 2
			if id%order == 0 {
				other := id + distance
				distance = order
				if order >= threads {
					continue
				}
				for key, value := range acum[other] {
					acum[id][key] += value
				}
			}
		}
		return nil
	}); err != nil {
		return ierror.Raise(err)
	}
	return countByReduce[T](this, acum[0])
}

func countByReduce[K comparable](this *IMathImpl, acum map[K]int64) error {
	logger.Info("Math: reducing global counting")
	executors := this.executorData.Mpi().Executors()
	group, err := core.NewPartitionGroupDef[ipair.IPair[K, int64]](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	output, err := core.NewPartitionGroupDef[ipair.IPair[K, int64]](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}
	writers := make([]iterator.IWriteIterator[ipair.IPair[K, int64]], executors)
	for i := 0; i < executors; i++ {
		group.AddMemoryPartition(1000)
		wit, err := group.Get(i).WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		writers[i] = wit
	}
	hasher := utils.GetHasher(reflect.TypeOf(*new(K)))
	for key, value := range acum {
		if err := writers[utils.Hash(key, hasher)%uint64(executors)].Write(*ipair.New(key, value)); err != nil {
			return ierror.Raise(err)
		}
	}
	if err := Exchange(this.Base(), group, output); err != nil {
		return ierror.Raise(err)
	}
	acum = map[K]int64{}
	part := output.Get(0)

	if array, ok := part.Inner().(storage.IList).Array().([]ipair.IPair[K, int64]); ok {
		for i := 0; i < len(array); i++ {
			acum[array[i].First] += array[i].Second
		}
	} else {
		reader, err := part.ReadIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		for reader.HasNext() {
			elem, err := reader.Next()
			if err != nil {
				return ierror.Raise(err)
			}
			acum[elem.First] += elem.Second
		}
	}
	if err = part.Clear(); err != nil {
		return ierror.Raise(err)
	}
	writer, err := part.WriteIterator()
	if err != nil {
		return ierror.Raise(err)
	}

	for key, value := range acum {
		if err = writer.Write(*ipair.New(key, value)); err != nil {
			return ierror.Raise(err)
		}
	}

	core.SetPartitions(this.executorData, output)
	return nil
}
