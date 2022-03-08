package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/executor/core/modules/impl"
	"ignis/rpc"
)

type IIOModule struct {
	IModule
	ioImpl *impl.IIOImpl
}

func NewIIOModule(executorData *core.IExecutorData) *IIOModule {
	return &IIOModule{
		IModule{executorData},
		impl.NewIIOImpl(executorData),
	}
}

func (this *IIOModule) LoadClass(ctx context.Context, src *rpc.ISource) (_err error) {
	defer this.moduleRecover(&_err)
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return this.PackError(err)
	}
	return this.PackError(basefun.Before(this.executorData.GetContext()))
}

func (this *IIOModule) LoadLibrary(ctx context.Context, path string) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.executorData.LoadLibraryFunctions(path))
}

func (this *IIOModule) PartitionCount(ctx context.Context) (_r int64, _err error) {
	defer this.moduleRecover(&_err)
	group := this.executorData.GetPartitionsAny()
	_r = 0
	for i := 0; i < group.Size(); i++ {
		_r += group.GetBase(i).Size()
	}
	return
}

func (this *IIOModule) CountByPartition(ctx context.Context) (_r []int64, _err error) {
	defer this.moduleRecover(&_err)
	group := this.executorData.GetPartitionsAny()
	_r = make([]int64, int(group.Size()))
	for i := 0; i < group.Size(); i++ {
		_r[i] = group.GetBase(i).Size()
	}
	return
}

func (this *IIOModule) PartitionApproxSize(ctx context.Context) (_r int64, _err error) {
	defer this.moduleRecover(&_err)
	base, err := this.TypeFromPartition()
	if err != nil {
		return 0, this.PackError(err)
	}
	_r, _err = base.PartitionApproxSize(this.ioImpl)
	_err = this.PackError(_err)
	return
}

func (this *IIOModule) TextFile(ctx context.Context, path string) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.ioImpl.TextFile(path, 0))
}

func (this *IIOModule) TextFile2(ctx context.Context, path string, minPartitions int64) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.ioImpl.TextFile(path, minPartitions))
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
