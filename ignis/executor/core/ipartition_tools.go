package core

import "ignis/executor/core/storage"

type IPartitionTools struct {
}

func NewIPartitionTools() *IPartitionTools {
	return &IPartitionTools{}
}

func NewPartition[T any](this *IPartitionTools) *storage.IPartition[T] {
	return nil
}

func NewPartitionWithName[T any](this *IPartitionTools, name string) *storage.IPartition[T] {
	return nil
}

func NewPartitionGroup[T any](this *IPartitionTools, sz int) *storage.IPartitionGroup[T] {
	return nil
}

func NewMemoryPartition[T any](this *IPartitionTools, sz int) *storage.IPartition[T] {
	return nil
}
