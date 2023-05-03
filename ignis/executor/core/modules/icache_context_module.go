package modules

import (
	"context"
	"ignis/executor/core"
	"ignis/executor/core/logger"
	"ignis/executor/core/modules/impl"
)

type ICacheContextModule struct {
	IModule
	impl *impl.ICacheImpl
}

func NewICacheContextModule(executorData *core.IExecutorData) *ICacheContextModule {
	this := &ICacheContextModule{
		IModule{executorData},
		impl.NewICacheImpl(executorData),
	}

	if err := this.executorData.ReloadLibraries(); err != nil {
		logger.Error(err)
	}
	//load partition cache when the executor has previously crashed
	diskCache, err := this.impl.LoadCacheFromDisk()
	if err != nil {
		logger.Error(err)
		return this
	}
	for i := 0; i < len(diskCache); i++ {
		if tp, err := this.TypeFromName(diskCache[i][1]); err != nil {
			logger.Error(err)
		} else {
			err = tp.LoadFromDisk(this.impl, diskCache[i])
			if err != nil {
				logger.Error(err)
			}
		}
	}

	return this
}

func (this *ICacheContextModule) SaveContext(ctx context.Context) (_r int64, _err error) {
	defer this.moduleRecover(&_err)
	_r, _err = this.impl.SaveContext()
	_err = this.PackError(_err)
	return
}

func (this *ICacheContextModule) ClearContext(ctx context.Context) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.ClearContext())
}

func (this *ICacheContextModule) LoadContext(ctx context.Context, id int64) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.LoadContext(id))
}

func (this *ICacheContextModule) LoadContextAsVariable(ctx context.Context, id int64, name string) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.LoadContextAsVariable(id, name))
}

func (this *ICacheContextModule) Cache(ctx context.Context, id int64, level int8) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.Cache(id, level))
}

func (this *ICacheContextModule) LoadCache(ctx context.Context, id int64) (_err error) {
	defer this.moduleRecover(&_err)
	return this.PackError(this.impl.LoadCache(id))
}
