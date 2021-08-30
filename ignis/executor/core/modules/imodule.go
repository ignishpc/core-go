package modules

import (
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/rpc"
)

type IModule struct {
	executorData *core.IExecutorData
}

func NewIModule(executorData *core.IExecutorData) IModule { //Only for IDriverContext
	return IModule{executorData}
}

func (this *IModule) _pack_error(err error) error {
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
