package storage

import (
	"ignis/executor/core/ierror"
)

const IDiskPartitionType = "Disk"

type IDiskPartition[T any] struct {
	IRawPartition[T]
}

func NewIDiskPartition[T any](path string, compression int8, native bool, persist bool, read bool) (*IDiskPartition[T], error) {
	return nil, ierror.RaiseMsg("Not implemented yet")
}
