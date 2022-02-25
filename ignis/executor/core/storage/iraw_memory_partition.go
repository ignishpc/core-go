package storage

const IRawMemoryPartitionType = "IRawMemoryPartition"

type IRawMemoryPartition[T any] struct {
	IRawPartition[T]
}
