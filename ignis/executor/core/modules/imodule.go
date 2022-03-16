package modules

import (
	"fmt"
	"ignis/executor/api/base"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
	"ignis/executor/core/logger"
	"ignis/rpc"
	"reflect"
)

type IModule struct {
	executorData *core.IExecutorData
}

func NewIModule(executorData *core.IExecutorData) IModule { //Only for IDriverContext
	return IModule{executorData}
}

func (this *IModule) PackError(err error) error {
	if err == nil {
		return nil
	}

	ex := rpc.NewIExecutorException()

	if ee, ok := err.(*ierror.IExecutorError); ok {
		ex.Message = ee.GetMessage()
		ex.Cause_ = ee.Error()
	} else {
		ex.Message = err.Error()
		ex.Cause_ = ""

	}

	return ex
}

func (this *IModule) TypeFromDefault() (base.ITypeFunctions, error) {
	return base.NewTypeCC[any, any](), nil
}

func (this *IModule) TypeFromPartition() (base.ITypeFunctions, error) {
	this.executorData.LoadContextType()
	group, err := core.GetPartitions[any](this.executorData)
	if err != nil {
		return nil, ierror.Raise(err)
	}

	for _, part := range group.Iter() {
		it, err := part.ReadIterator()
		if err != nil {
			return nil, ierror.Raise(err)
		}
		for it.HasNext() {
			elem, err := it.Next()
			if err != nil {
				return nil, ierror.Raise(err)
			}
			return this.TypeFromName(iio.GetName(elem))
		}
	}

	return nil, ierror.RaiseMsg("Type not found in partition")
}

func (this *IModule) TypeFromName(name string) (base.ITypeFunctions, error) {
	this.executorData.LoadContextType()
	typeBase := this.executorData.GetType(name)
	if typeBase == nil {
		logger.Warn("Type '", name, "' not found, using 'any' as default")
		return this.TypeFromDefault()
	}
	return typeBase.(base.ITypeFunctions), nil
}

func (this *IModule) TypeFromSource(src *rpc.ISource) (base.ITypeFunctions, error) {
	basefun, err := this.executorData.LoadLibrary(src)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	if err = basefun.Before(this.executorData.GetContext()); err != nil {
		return nil, ierror.Raise(err)
	}

	typeBase := this.executorData.LoadContextType()
	if typeBase == nil {
		return nil, ierror.RaiseMsg("No type found in source")
	}

	return typeBase.(base.ITypeFunctions), nil
}

func (this *IModule) CompatibilyError(f reflect.Type, m string) error {
	return this.PackError(ierror.RaiseMsg(f.String() + " is not compatible with " + m))
}

func (this *IModule) moduleRecover(err *error) {
	if r := recover(); r != nil {
		if err2, ok := r.(error); ok {
			*err = ierror.Raise(err2)
			return
		}
		if msg, ok := r.(string); ok {
			*err = ierror.RaiseMsg(msg)
			return
		}
		*err = ierror.RaiseMsg(fmt.Sprint(r))
	}
}
