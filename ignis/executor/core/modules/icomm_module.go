package modules

import (
	"context"
	"ignis/executor/api/base"
	"ignis/executor/core"
	"ignis/executor/core/modules/impl"
	"ignis/rpc"
)

type ICommModule struct {
	IModule
	impl *impl.ICommImpl
}

func NewICommModule(executorData *core.IExecutorData) *ICommModule {
	return &ICommModule{
		IModule{executorData},
		impl.NewICommImpl(executorData),
	}
}
func (this *ICommModule) OpenGroup(ctx context.Context) (_r string, _err error) {
	defer this.moduleRecover(&_err)
	_r, _err = this.impl.OpenGroup()
	_err = this.PackError(_err)
	return
}

func (this *ICommModule) CloseGroup(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.CloseGroup())
}

func (this *ICommModule) JoinToGroup(ctx context.Context, id string, leader bool) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.JoinToGroup(id, leader))
}

func (this *ICommModule) JoinToGroupName(ctx context.Context, id string, leader bool, name string) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.JoinToGroupName(id, leader, name))
}

func (this *ICommModule) HasGroup(ctx context.Context, name string) (_r bool, _err error) {
	defer this.moduleRecover(&_err)
	_r, _err = this.impl.HasGroup(name)
	_err = this.PackError(_err)
	return
}

func (this *ICommModule) DestroyGroup(ctx context.Context, name string) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.DestroyGroup(name))
}

func (this *ICommModule) DestroyGroups(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.DestroyGroups())
}

func (this *ICommModule) GetProtocol(ctx context.Context) (_r int8, _err error) {
	defer this.moduleRecover(&_err)
	_r, _err = this.impl.GetProtocol()
	_err = this.PackError(_err)
	return
}

func (this *ICommModule) GetPartitions(ctx context.Context, protocol int8) (_r [][]byte, _err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return nil, this.PackError(err)
	}
	_r, _err = base.GetPartitions(this.impl, protocol, 0)
	_err = this.PackError(_err)
	return
}

func (this *ICommModule) GetPartitions2(ctx context.Context, protocol int8, minPartitions int64) (_r [][]byte, _err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return nil, this.PackError(err)
	}
	_r, _err = base.GetPartitions(this.impl, protocol, minPartitions)
	_err = this.PackError(_err)
	return
}

func (this *ICommModule) SetPartitions(ctx context.Context, partitions [][]byte) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromDefault()
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.SetPartitions(this.impl, partitions))
}

func (this *ICommModule) SetPartitions2(ctx context.Context, partitions [][]byte, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromSource(src)
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(base.SetPartitions(this.impl, partitions))
}

func (this *ICommModule) DriverGather(ctx context.Context, group string, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	var base base.ITypeBase
	if this.executorData.HasPartitions() {
		base, _err = this.TypeFromPartition()
	} else {
		base, _err = this.TypeFromSource(src)
	}
	if _err != nil {
		return this.PackError(_err)
	}
	return this.PackError(base.DriverGather(this.impl, group))
}

func (this *ICommModule) DriverGather0(ctx context.Context, group string, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	var base base.ITypeBase
	if this.executorData.HasPartitions() {
		base, _err = this.TypeFromPartition()
	} else {
		base, _err = this.TypeFromSource(src)
	}
	if _err != nil {
		return this.PackError(_err)
	}
	return this.PackError(base.DriverGather0(this.impl, group))
}

func (this *ICommModule) DriverScatter(ctx context.Context, group string, partitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	var base base.ITypeBase
	if this.executorData.HasPartitions() {
		base, _err = this.TypeFromPartition()
	} else {
		base, _err = this.TypeFromDefault()
	}
	if _err != nil {
		return this.PackError(_err)
	}
	return this.PackError(base.DriverScatter(this.impl, group, partitions))
}

func (this *ICommModule) DriverScatter3(ctx context.Context, group string, partitions int64, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	var base base.ITypeBase
	if this.executorData.HasPartitions() {
		base, _err = this.TypeFromPartition()
	} else {
		base, _err = this.TypeFromSource(src)
	}
	if _err != nil {
		return this.PackError(_err)
	}
	return this.PackError(base.DriverScatter(this.impl, group, partitions))
}

func (this *ICommModule) ImportData(ctx context.Context, group string, source bool, threads int64) (_err error) {
	defer this.moduleRecover(&_err)
	var base base.ITypeBase
	if this.executorData.HasPartitions() {
		base, _err = this.TypeFromPartition()
	} else {
		base, _err = this.TypeFromDefault()
	}
	if _err != nil {
		return this.PackError(_err)
	}
	return this.PackError(base.ImportData(this.impl, group, source, threads))
}

func (this *ICommModule) ImportData4(ctx context.Context, group string, source bool, threads int64, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	var base base.ITypeBase
	if this.executorData.HasPartitions() {
		base, _err = this.TypeFromPartition()
	} else {
		base, _err = this.TypeFromSource(src)
	}
	if _err != nil {
		return this.PackError(_err)
	}
	return this.PackError(base.ImportData(this.impl, group, source, threads))
}
