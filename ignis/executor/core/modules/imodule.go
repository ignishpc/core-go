package modules

import (
	"fmt"
	"ignis/executor/api/base"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/rpc"
	"reflect"
)

type IModule struct {
	executorData *core.IExecutorData
}

func NewIModule(executorData *core.IExecutorData) IModule { //Only for IDriverContext
	return IModule{executorData}
}

func (this *IModule) Pack_error(err error) error {
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

func (this *IModule) TypeFromDefault() (base.IBasicBase, error) {
	return nil, ierror.RaiseMsg("Not implemented error") //TODO
}

func (this *IModule) TypeFromPartition() (base.IBasicBase, error) {
	return nil, ierror.RaiseMsg("Not implemented error") //TODO
}

func (this *IModule) TypeFromName(name string) (base.IBasicBase, error) {
	return nil, ierror.RaiseMsg("Not implemented error") //TODO
}

func (this *IModule) TypeFromSource(src *rpc.ISource) (base.IBasicBase, error) {
	return nil, ierror.RaiseMsg("Not implemented error") //TODO
}

func (this *IModule) CompatibilyError(f reflect.Type, m string) error {
	return this.Pack_error(ierror.RaiseMsg(f.String() + " is not compatible with " + m))
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
