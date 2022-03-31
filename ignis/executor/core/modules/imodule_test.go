package modules

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"ignis/executor/api/ipair"
	"ignis/executor/core"
	"ignis/rpc"
	"math/rand"
	"sort"
	"testing"
)

func newSource(name string) *rpc.ISource {
	src := rpc.NewISource()
	src.Obj = rpc.NewIEncoded()
	src.Obj.Name = &name
	return src
}

func normalize[T any](obj T) T {
	switch v := any(obj).(type) {
	case string:
		chars := []rune(v)
		sort.Slice(chars, func(i, j int) bool {
			return chars[i] < chars[j]
		})
		return any(string(chars)).(T)
	}
	return obj
}

func rankVector[T any](executorData *core.IExecutorData, array []T) []T {
	rank := executorData.Mpi().Rank()
	execs := executorData.Mpi().Executors()
	block := len(array) / execs
	rem := len(array) % execs
	if rank < rem {
		return array[block*rank+rank : block*(rank+1)+rank+1]
	} else {
		return array[block*rank+rem : block*(rank+1)+rem]
	}
}

func loadToPartitions[T any](t *testing.T, executorData *core.IExecutorData, array []T, partitions int) {
	group, err := core.NewPartitionGroupWithSize[T](executorData.GetPartitionTools(), partitions)
	require.Nil(t, err)
	core.SetPartitions(executorData, group)
	block := len(array) / partitions
	rem := len(array) % partitions
	pos := 0
	for i := 0; i < partitions; i++ {
		writer, err := group.Get(i).WriteIterator()
		require.Nil(t, err)
		for j := 0; j < block; j++ {
			require.Nil(t, writer.Write(array[pos]))
			pos++
		}
		if i < rem {
			require.Nil(t, writer.Write(array[pos]))
			pos++
		}
	}
}

func getFromPartitions[T any](t *testing.T, executorData *core.IExecutorData) []T {
	array := make([]T, 0)
	group, err := core.GetPartitions[T](executorData)
	require.Nil(t, err)
	for _, part := range group.Iter() {
		reader, err := part.ReadIterator()
		require.Nil(t, err)
		for reader.HasNext() {
			elem, err := reader.Next()
			require.Nil(t, err)
			array = append(array, elem)
		}
	}
	return array
}

func sepUpDefault(executorData *core.IExecutorData) {
	props := executorData.GetContext().Props()
	props["ignis.transport.compression"] = "6"
	props["ignis.partition.compression"] = "6"
	props["ignis.partition.serialization"] = "ignis"
	props["ignis.modules.exchange.type"] = "sync"
	props["ignis.transport.cores"] = "0"
	props["ignis.executor.directory"] = "./"
}

type IElements[T any] interface {
	create(n int, seed int) []T
}

type IElemensInt struct {
}

func (this *IElemensInt) create(n int, seed int) []int64 {
	array := make([]int64, n)
	rand.Seed(int64(seed))
	for i := 0; i < n; i++ {
		array[i] = rand.Int63() % int64(n)
	}
	return array
}

type IElemensString struct {
}

func (this *IElemensString) create(n int, seed int) []string {
	array := make([]string, n)
	rand.Seed(int64(seed))
	for i := 0; i < n; i++ {
		array[i] = fmt.Sprint(rand.Int63() % int64(n))
	}
	return array
}

type IElemensPair[T1 any, T2 any] struct {
	elemsT1 IElements[T1]
	elemsT2 IElements[T2]
}

func (this *IElemensPair[T1, T2]) create(n int, seed int) []ipair.IPair[T1, T2] {
	first := this.elemsT1.create(n, seed)
	second := this.elemsT2.create(n, seed)

	array := make([]ipair.IPair[T1, T2], n)
	for i := 0; i < n; i++ {
		array[i] = *ipair.New(first[i], second[i])
	}
	return array
}
