package core

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core"
	"ignis/executor/core/logger"
	"ignis/executor/core/modules"
	"ignis/rpc/executor"
)

type ICallBack struct {
	server  *modules.IExecutorServerModule
	context *IDriverContext
}

func NewICallBack(port, compression int) (*ICallBack, error) {
	logger.Init()

	executorData := core.NewIExecutorData()
	driverContext := NewIDriverContext(executorData)

	services := func(processor *thrift.TMultiplexedProcessor) {
		processor.RegisterProcessor("IIO", executor.NewIIOModuleProcessor(modules.NewIIOModule(executorData)))
		processor.RegisterProcessor("ICacheContext", executor.NewICacheContextModuleProcessor(driverContext))
		processor.RegisterProcessor("IComm", executor.NewICommModuleProcessor(modules.NewICommModule(executorData)))
	}

	server := modules.NewIExecutorServerModule(executorData)
	go server.Serve("IExecutorServer", port, compression, services)
	return &ICallBack{
		server,
		driverContext,
	}, nil
}

func (this *ICallBack) Stop() error {
	return this.server.Stop(context.Background())
}

func (this *ICallBack) DriverContext() *IDriverContext {
	return this.context
}
