package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/itype"
	"ignis/executor/core/logger"
	"ignis/executor/core/modules"
	"ignis/rpc/executor"
	"os"
	"strconv"
)

func _init_(args []string) error {
	logger.Init()
	if len(os.Args) < 3 {
		return ierror.RaiseMsg("Executor need a server port and compression as argument")
	}

	port, err := strconv.Atoi(os.Args[1])
	compression, err2 := strconv.Atoi(os.Args[2])

	if err != nil || err2 != nil {
		return ierror.RaiseMsg("Executor need a valid server port and compression as argument")
	}

	executorData := core.NewIExecutorData()

	services := func(processor *thrift.TMultiplexedProcessor) {
		processor.RegisterProcessor("IGeneral", executor.NewIGeneralModuleProcessor(modules.NewIGeneralModule(executorData)))
		processor.RegisterProcessor("IGeneralAction", executor.NewIGeneralActionModuleProcessor(modules.NewIGeneralActionModule(executorData)))
		processor.RegisterProcessor("IMath", executor.NewIMathModuleProcessor(modules.NewIMathModule(executorData)))
		processor.RegisterProcessor("IIO", executor.NewIIOModuleProcessor(modules.NewIIOModule(executorData)))
		processor.RegisterProcessor("ICacheContext", executor.NewICacheContextModuleProcessor(modules.NewICacheContextModule(executorData)))
		processor.RegisterProcessor("IComm", executor.NewICommModuleProcessor(modules.NewICommModule(executorData)))

		itype.DefaultTypes(executorData)
		itype.DefaultFunctions(executorData)
	}

	if _, present := os.LookupEnv(""); present {
		err = impi.MPI_Init_thread(nil, nil, impi.MPI_THREAD_MULTIPLE, nil)
	} else {
		err = impi.MPI_Init(nil, nil)
	}
	if err != nil {
		return ierror.Raise(err)
	}

	server := modules.NewIExecutorServerModule(executorData)
	return server.Serve("IExecutorServer", port, compression, services)
}

func main() {
	if err := _init_(os.Args); err != nil {
		logger.Error(err)
		os.Exit(-1)
	}
}
