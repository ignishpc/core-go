package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/rpc"
)

type IIOModule struct {
	IModule
}

func NewIIOModule(executorData *core.IExecutorData) *IIOModule {
	return &IIOModule{
		IModule{executorData},
	}
}

func (this *IIOModule) LoadClass(ctx context.Context, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IIOModule) LoadLibrary(ctx context.Context, path string) (_err error) {
	return nil
}

func (this *IIOModule) PartitionCount(ctx context.Context) (_r int64, _err error) {
	return 0, nil
}

func (this *IIOModule) CountByPartition(ctx context.Context) (_r []int64, _err error) {
	return nil, nil
}

func (this *IIOModule) PartitionApproxSize(ctx context.Context) (_r int64, _err error) {
	return 0, nil
}

func (this *IIOModule) TextFile(ctx context.Context, path string) (_err error) {
	return nil
}

func (this *IIOModule) TextFile2(ctx context.Context, path string, minPartitions int64) (_err error) {
	return nil
}

func (this *IIOModule) PartitionObjectFile(ctx context.Context, path string, first int64, partitions int64) (_err error) {
	return nil
}

func (this *IIOModule) PartitionObjectFile4(ctx context.Context, path string, first int64, partitions int64, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IIOModule) PartitionTextFile(ctx context.Context, path string, first int64, partitions int64) (_err error) {
	return nil
}

func (this *IIOModule) PartitionJsonFile4a(ctx context.Context, path string, first int64, partitions int64, objectMapping bool) (_err error) {
	return nil
}

func (this *IIOModule) PartitionJsonFile4b(ctx context.Context, path string, first int64, partitions int64, src *rpc.ISource) (_err error) {
	return nil
}

func (this *IIOModule) SaveAsObjectFile(ctx context.Context, path string, compression int8, first int64) (_err error) {
	return nil
}

func (this *IIOModule) SaveAsTextFile(ctx context.Context, path string, first int64) (_err error) {
	return nil
}

func (this *IIOModule) SaveAsJsonFile(ctx context.Context, path string, first int64, pretty bool) (_err error) {
	return nil
}
