package storage

const IRawMemoryPartitionType = "RawMemory"

type IRawMemoryPartition[T any] struct {
	IRawPartition[T]
}
