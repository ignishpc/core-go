package storage

import (
	"ignis/executor/api/ipair"
	"ignis/executor/core/iio"
	"math/rand"
)

func init() {
	iio.AddBasicType[int64]()
	iio.AddBasicType[string]()
	iio.AddBasicType[ipair.IPair[int64, int64]]()
	addPartitionTest(&IPartitionTest[int64]{
		"IRawMemoryPartitionInt64Test",
		func() IPartition[int64] {
			part, err := NewIRawMemoryPartition[int64](100*8, 6, false)
			if err != nil {
				panic(err)
			}
			return part
		},
		func(n int, seed int) []int64 {
			array := make([]int64, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	})
	addPartitionTest(&IPartitionTest[any]{
		"IRawMemoryPartitionAnyIntTest",
		func() IPartition[any] {
			part, err := NewIRawMemoryPartition[any](100*8, 6, false)
			if err != nil {
				panic(err)
			}
			return part
		},
		func(n int, seed int) []any {
			array := make([]any, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	})
	addPartitionTest(&IPartitionTest[int64]{
		"IRawMemoryPartitionInt64NativeTest",
		func() IPartition[int64] {
			part, err := NewIRawMemoryPartition[int64](100*8, 6, true)
			if err != nil {
				panic(err)
			}
			return part
		},
		func(n int, seed int) []int64 {
			array := make([]int64, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	})
	addPartitionTest(&IPartitionTest[ipair.IPair[int64, int64]]{
		"IRawMemoryPartitionPairInt64Int64Test",
		func() IPartition[ipair.IPair[int64, int64]] {
			part, err := NewIRawMemoryPartition[ipair.IPair[int64, int64]](100*8, 6, false)
			if err != nil {
				panic(err)
			}
			return part
		},
		func(n int, seed int) []ipair.IPair[int64, int64] {
			array := make([]ipair.IPair[int64, int64], n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = *ipair.New(rand.Int63()%int64(n), rand.Int63()%int64(n))
			}
			return array
		},
	})
	addPartitionTest(&IPartitionTest[any]{
		"IRawMemoryPartitionAnyIntNativeTest",
		func() IPartition[any] {
			part, err := NewIRawMemoryPartition[any](100*8, 6, true)
			if err != nil {
				panic(err)
			}
			return part
		},
		func(n int, seed int) []any {
			array := make([]any, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	})
}
