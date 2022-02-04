package storage

import "math/rand"

func init() {
	addPartitionTest(&IMemoryPartitionTest[int64]{
		IPartitionTest[int64]{
			"IMemoryPartitionInt64Test",
			func() IPartition[int64] {
				return NewIMemoryPartitionWithNative[int64](100, false)
			},
			func(n int) []int64 {
				array := make([]int64, n)
				for i := 0; i < n; i++ {
					array[i] = rand.Int63() % int64(n)
				}
				return array
			},
		}})
	addPartitionTest(&IMemoryPartitionTest[any]{
		IPartitionTest[any]{
			"IMemoryPartitionAnyTest",
			func() IPartition[any] {
				return NewIMemoryPartitionWithNative[any](100, false)
			},
			func(n int) []any {
				array := make([]any, n)
				for i := 0; i < n; i++ {
					array[i] = rand.Int63() % int64(n)
				}
				return array
			},
		}})
}

type IMemoryPartitionTest[T any] struct {
	IPartitionTest[T]
}
