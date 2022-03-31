package modules

import (
	"fmt"
	"ignis/executor/api/base"
	"ignis/executor/api/ipair"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/logger"
	"ignis/executor/core/utils"
	"ignis/rpc"
	"reflect"
	"strings"
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

func (this *IModule) TypeFromDefault(pair bool) (base.ITypeFunctions, error) {
	if pair {
		return base.NewTypeCC[any, any](), nil
	}
	return base.NewTypeC[any](), nil
}

func (this *IModule) TypeFromPartition() (base.ITypeFunctions, error) {
	this.executorData.LoadContextType()
	group := this.executorData.GetPartitionsAny()
	if elem := group.First(); elem != nil {
		return this.TypeFromName(reflect.TypeOf(elem).String())
	}

	return this.TypeFromDefault(false)
}

func (this *IModule) TypeFromName(name string) (base.ITypeFunctions, error) {
	this.executorData.LoadContextType()
	typeBase := this.executorData.GetType(name)
	if typeBase == nil {
		pairName := utils.TypeName[ipair.IPair[any, any]]()
		pairName = pairName[:strings.Index(pairName, "[")]
		if strings.HasPrefix(name, pairName) {
			logger.Warn("Type '", name, "' not found, using 'ipair.Pair[any, any]' as default")
			return this.TypeFromDefault(true)
		} else {
			logger.Warn("Type '", name, "' not found, using 'any' as default")
			return this.TypeFromDefault(false)
		}
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
