package modules

import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/logger"
	"ignis/executor/core/mpi"
	"ignis/rpc/executor"
	"os"
)

type IExecutorServerModule struct {
	IModule
	server    *thrift.TSimpleServer
	processor *thrift.TMultiplexedProcessor
}

type IExecutorServerServices func(processor *thrift.TMultiplexedProcessor)

func NewIExecutorServerModule(executorData *core.IExecutorData) *IExecutorServerModule {
	return &IExecutorServerModule{
		IModule{executorData},
		nil,
		nil,
	}
}

func (this *IExecutorServerModule) Serve(name string, port int, compression int, services IExecutorServerServices) error {
	if this.server != nil {
		return nil
	}
	this.processor = thrift.NewTMultiplexedProcessor()
	trans, err := thrift.NewTServerSocket(fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return err
	}
	this.server = thrift.NewTSimpleServer4(
		this.processor,
		trans,
		thrift.NewTZlibTransportFactory(compression),
		thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}),
	)
	this.processor.RegisterProcessor(name, executor.NewIExecutorServerModuleProcessor(this))
	logger.Info("ServerModule: go executor started")
	err = this.server.Serve()
	if err != nil {
		err = ierror.RaiseMsgCause("ServerModule fails", err)
	}
	logger.Info("ServerModule: go executor stopped")
	return err
}

func (this *IExecutorServerModule) Start(ctx context.Context, properties map[string]string, env map[string]string) (_err error) {
	for key, value := range properties {
		this.executorData.GetContext().Vars()[key] = value
	}
	this.executorData.SetCores(this.executorData.GetCores())
	for key, value := range env {
		os.Setenv(key, value)
	}
	var err error
	if _, present := os.LookupEnv("MPI_THREAD_MULTIPLE"); present {
		err = mpi.MPI_Init_thread(nil, nil, mpi.MPI_THREAD_MULTIPLE, nil)
		logger.Info("ServerModule: Mpi started in thread mode")
	} else {
		err = mpi.MPI_Init(nil, nil)
		logger.Info("ServerModule: Mpi started")
	}
	if err != nil {
		return err
	}
	logger.Info("ServerModule: go executor ready")
	return nil
}

func (this *IExecutorServerModule) Stop(ctx context.Context) (_err error) {
	mpi.MPI_Finalize()
	err := this.server.Stop()
	this.processor = nil
	this.server = nil
	return err
}

func (this *IExecutorServerModule) Test(ctx context.Context) (_r bool, _err error) {
	return true, nil
}
