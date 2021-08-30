package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/rpc"
)

type ICommModule struct {
	IModule
}

func NewICommModule(executorData *core.IExecutorData) *ICommModule {
	return &ICommModule{
		IModule{executorData},
	}
}
func (this *ICommModule) OpenGroup(ctx context.Context) (_r string, _err error) { return "", nil }

func (this *ICommModule) CloseGroup(ctx context.Context) (_err error)           { return nil }

func (this *ICommModule) JoinToGroup(ctx context.Context, id string, leader bool) (_err error) {
	return nil
}
func (this *ICommModule) JoinToGroupName(ctx context.Context, id string, leader bool, name string) (_err error) {
	return nil
}
func (this *ICommModule) HasGroup(ctx context.Context, name string) (_r bool, _err error) {
	return true, nil
}
func (this *ICommModule) DestroyGroup(ctx context.Context, name string) (_err error) { return nil }

func (this *ICommModule) DestroyGroups(ctx context.Context) (_err error)             { return nil }

func (this *ICommModule) GetProtocol(ctx context.Context) (_r int8, _err error)      { return 0, nil }

func (this *ICommModule) GetPartitions(ctx context.Context, protocol int8) (_r [][]byte, _err error) {
	return nil, nil
}
func (this *ICommModule) GetPartitions2(ctx context.Context, protocol int8, minPartitions int64) (_r [][]byte, _err error) {
	return nil, nil
}
func (this *ICommModule) SetPartitions(ctx context.Context, partitions [][]byte) (_err error) {
	return nil
}
func (this *ICommModule) SetPartitions2(ctx context.Context, partitions [][]byte, src *rpc.ISource) (_err error) {
	return nil
}
func (this *ICommModule) DriverGather(ctx context.Context, group string, src *rpc.ISource) (_err error) {
	return nil
}
func (this *ICommModule) DriverGather0(ctx context.Context, group string, src *rpc.ISource) (_err error) {
	return nil
}
func (this *ICommModule) DriverScatter(ctx context.Context, group string, partitions int64) (_err error) {
	return nil
}
func (this *ICommModule) DriverScatter3(ctx context.Context, group string, partitions int64, src *rpc.ISource) (_err error) {
	return nil
}
func (this *ICommModule) ImportData(ctx context.Context, group string, source bool, threads int64) (_err error) {
	return nil
}
func (this *ICommModule) ImportData4(ctx context.Context, group string, source bool, threads int64, src *rpc.ISource) (_err error) {
	return nil
}
