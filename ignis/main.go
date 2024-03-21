package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/itype"
	"ignis/executor/core/logger"
	"ignis/executor/core/modules"
	"ignis/rpc/executor"
	"os"
	"strconv"
)

func _init_(args []string) error {
	logger.Init()
	if len(os.Args) < 2 {
		return ierror.RaiseMsg("Executor requires a socket address")
	}

	usock := os.Args[1]
	var compression int
	if value, found := os.LookupEnv("IGNIS_TRANSPORT_COMPRESSION"); found {
		var err error
		compression, err = strconv.Atoi(value)
		if err != nil {
			return ierror.RaiseMsg("Executor arguments are not valid")
		}
	} else {
		compression = 0
	}

	executorData := core.NewIExecutorData()

	services := func(processor *thrift.TMultiplexedProcessor) {
		processor.RegisterProcessor("IGeneral", executor.NewIGeneralModuleProcessor(modules.NewIGeneralModule(executorData)))
		processor.RegisterProcessor("IGeneralAction", executor.NewIGeneralActionModuleProcessor(modules.NewIGeneralActionModule(executorData)))
		processor.RegisterProcessor("IMath", executor.NewIMathModuleProcessor(modules.NewIMathModule(executorData)))
		processor.RegisterProcessor("IIO", executor.NewIIOModuleProcessor(modules.NewIIOModule(executorData)))
		processor.RegisterProcessor("ICacheContext", executor.NewICacheContextModuleProcessor(modules.NewICacheContextModule(executorData)))
		processor.RegisterProcessor("IComm", executor.NewICommModuleProcessor(modules.NewICommModule(executorData)))

		for _, dtype := range itype.DefaultTypes() {
			executorData.RegisterType(dtype)
		}

		for _, df := range itype.DefaultFunctions() {
			executorData.RegisterFunction(df)
		}

	}

	server := modules.NewIExecutorServerModule(executorData, services)
	return server.Serve("IExecutorServer", usock, compression)
}

func main() {
	if err := _init_(os.Args); err != nil {
		logger.Error(err)
		os.Exit(-1)
	}
}
