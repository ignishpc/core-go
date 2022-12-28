package impl

import (
	"ignis/executor/api"
	"ignis/executor/api/function"
	"ignis/executor/api/iterator"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
	"math/rand"
)

type IRepartitionImpl struct {
	IBaseImpl
}

func NewIRepartitionImpl(executorData *core.IExecutorData) *IRepartitionImpl {
	return &IRepartitionImpl{
		IBaseImpl{executorData},
	}
}

func Repartition[T any](this *IRepartitionImpl, numPartitions int64, preserveOrdering bool, global_ bool) error {
	if !global_ || this.executorData.Mpi().Executors() == 1 {
		return local_repartition[T](this, numPartitions)
	} else if preserveOrdering {
		return ordered_repartition[T](this, numPartitions)
	} else {
		return unordered_repartition[T](this, numPartitions)
	}
}

func local_repartition[T any](this *IRepartitionImpl, numPartitions int64) error {
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	executors := int64(this.executorData.GetContext().Executors())
	var elements int64 = 0
	for _, part := range input.Iter() {
		elements += part.Size()
	}
	localPartitions := int(numPartitions / executors)
	if numPartitions%executors > int64(this.executorData.GetContext().ExecutorId()) {
		localPartitions++
	}
	output, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), localPartitions)
	if err != nil {
		return ierror.Raise(err)
	}
	logger.Info("Repartition: local repartition from ", +input.Size(), " to ", localPartitions, " partitions")
	partition_elems := int(elements / int64(localPartitions))
	remainder := int(elements % int64(localPartitions))
	i := int(1)
	ew := int(0)
	er := input.Get(0).Size()
	it, err := input.Get(0).ReadIterator()
	if err != nil {
		return ierror.Raise(err)
	}
	for p := 0; p < localPartitions; p++ {
		part := output.Get(p)
		writer, err := part.WriteIterator()
		if err != nil {
			return ierror.Raise(err)
		}
		ew = partition_elems
		if p < remainder {
			ew++
		}

		for ew > 0 && (i < input.Size() || er > 0) {
			if er == 0 {
				input.Set(i-1, nil)
				er = input.Get(i).Size()
				it, err = input.Get(i).ReadIterator()
				i++
				if err != nil {
					return ierror.Raise(err)
				}
			}
			for ; ew > 0 && er > 0; ew, er = ew-1, er-1 {
				elem, err := it.Next()
				if err != nil {
					return ierror.Raise(err)
				}
				if writer.Write(elem) != nil {
					return ierror.Raise(err)
				}
			}
		}
		if part.Fit() != nil {
			return ierror.Raise(err)
		}
	}
	core.SetPartitions(this.executorData, output)
	return nil
}

func ordered_repartition[T any](this *IRepartitionImpl, numPartitions int64) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

func unordered_repartition[T any](this *IRepartitionImpl, numPartitions int64) error {
	return ierror.RaiseMsg("Not implemented yet") //TODO
}

type Partitioner[T any] interface {
	Call(e T, ctx api.IContext) (int64, error)
}

type RandomPartitioner[T any] struct {
	r *rand.Rand
}

func (this *RandomPartitioner[T]) Call(e T, ctx api.IContext) (int64, error) {
	return this.r.Int63(), nil
}

type HashPartitioner[T any] struct {
	h utils.Hasher
}

func (this *HashPartitioner[T]) Call(e T, ctx api.IContext) (int64, error) {
	return int64(utils.Hash(e, this.h)), nil
}

func PartitionByRandom[T any](this *IRepartitionImpl, numPartitions int64, seed int32) error {
	return PartitionByImpl[T](this, func() Partitioner[T] {
		return &RandomPartitioner[T]{r: rand.New(rand.NewSource(int64(seed)))}
	}, numPartitions)
}

func PartitionByHash[T any](this *IRepartitionImpl, numPartitions int64) error {
	return PartitionByImpl[T](this, func() Partitioner[T] {
		return &HashPartitioner[T]{h: utils.GetHasher(utils.TypeObj[T]())}
	}, numPartitions)
}

func PartitionBy[T any](this *IRepartitionImpl, f function.IFunction[T, int64], numPartitions int64) error {
	context := this.executorData.GetContext()
	if err := f.Before(context); err != nil {
		return ierror.Raise(err)
	}
	if err := PartitionByImpl[T](this, func() Partitioner[T] {
		return f
	}, numPartitions); err != nil {
		return ierror.Raise(err)
	}
	if err := f.After(context); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func PartitionByImpl[T any](this *IRepartitionImpl, fparticioner func() Partitioner[T], numPartitions int64) error {
	context := this.executorData.GetContext()
	input, err := core.GetAndDeletePartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	ouput, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
	if err != nil {
		return ierror.Raise(err)
	}

	logger.Info("Repartition: partitionBy in ", +input.Size(), " partitions")
	global, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), int(numPartitions))
	if err != nil {
		return ierror.Raise(err)
	}

	if input.Size() > 0 {
		locals := make([]*storage.IPartitionGroup[T], this.executorData.GetCores())
		for i := 0; i < len(locals); i++ {
			g, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), int(numPartitions))
			if err != nil {
				return ierror.Raise(err)
			}
			locals[i] = g
		}
		if err := ithreads.Parallel(func(rctx ithreads.IRuntimeContext) error {
			local := locals[rctx.ThreadId()]
			particioner := fparticioner()
			writers := make([]iterator.IWriteIterator[T], int(numPartitions))
			for i := 0; i < local.Size(); i++ {
				writers[i], err = local.Get(i).WriteIterator()
				if err != nil {
					return ierror.Raise(err)
				}
			}

			if err := rctx.For().Dynamic().Run(input.Size(), func(i int) error {
				reader, err := input.Get(i).ReadIterator()
				if err != nil {
					return ierror.Raise(err)
				}
				for reader.HasNext() {
					elem, err := reader.Next()
					if err != nil {
						return ierror.Raise(err)
					}
					p, err := particioner.Call(elem, context)
					if err != nil {
						return ierror.Raise(err)
					}
					if p < 0 {
						p *= -1
					}
					if err = writers[p%numPartitions].Write(elem); err != nil {
						return ierror.Raise(err)
					}
				}
				input.Set(i, nil)
				return nil
			}); err != nil {
				return ierror.Raise(err)
			}

			return rctx.For().Dynamic().Run(global.Size(), func(p int) error {
				for l := 0; l < len(locals); l++ {
					if err := locals[l].Get(p).MoveTo(global.Get(p)); err != nil {
						return ierror.Raise(err)
					}
				}
				return nil
			})
		}); err != nil {
			return ierror.Raise(err)
		}
	}
	logger.Info("Repartition: exchanging new partitions")
	if err := Exchange(this.Base(), global, ouput); err != nil {
		return ierror.Raise(err)
	}
	core.SetPartitions(this.executorData, ouput)
	return nil
}
