package storage

import (
	"math/rand"
	"strconv"
)

func init() {
	CreateList[int64]()
	addPartitionTest(&IPartitionTest[int64]{
		"IMemoryPartitionInt64Test",
		func() IPartition[int64] {
			return NewIMemoryPartition[int64](100, false)
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
		"IMemoryPartitionAnyTest",
		func() IPartition[any] {
			return NewIMemoryPartition[any](100, false)
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
	addPartitionTest(&IPartitionTest[any]{
		"IMemoryPartitionAnyAnyTest",
		func() IPartition[any] {
			return NewIMemoryPartition[any](100, false)
		},
		func(n int, seed int) []any {
			array := make([]any, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = strconv.Itoa(int(rand.Int63() % int64(n)))
			}
			return array
		},
	})
}
