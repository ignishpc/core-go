package modules

import (
	"context"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/logger"
	"ignis/rpc/executor"
	"net"
	"os"
	"time"
)

type IExecutorServerModule struct {
	IModule
	server    *thrift.TSimpleServer
	processor *thrift.TMultiplexedProcessor
	services  IExecutorServerServices
	usock     *string
}

type IExecutorServerServices func(processor *thrift.TMultiplexedProcessor)

func NewIExecutorServerModule(executorData *core.IExecutorData, services IExecutorServerServices) *IExecutorServerModule {
	return &IExecutorServerModule{
		IModule{executorData},
		nil,
		nil,
		services,
		nil,
	}
}

func (this *IExecutorServerModule) Serve(name string, usock string, compression int) error {
	if this.server != nil {
		return nil
	}
	this.processor = thrift.NewTMultiplexedProcessor()
	this.usock = &usock
	_ = os.Remove(usock)

	trans := thrift.NewTServerSocketFromAddrTimeout(&net.UnixAddr{Name: "unix", Net: usock}, 0)
	this.server = thrift.NewTSimpleServer4(
		this.processor,
		trans,
		thrift.NewTZlibTransportFactory(compression),
		thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}),
	)
	this.processor.RegisterProcessor(name, executor.NewIExecutorServerModuleProcessor(this))
	logger.Info("ServerModule: go executor started")
	err := this.server.Serve()
	if err != nil {
		err = ierror.RaiseMsgCause("ServerModule fails", err)
	}
	logger.Info("ServerModule: go executor stopped")
	return err
}

func (this *IExecutorServerModule) Start(ctx context.Context, properties map[string]string, env map[string]string) (_err error) {
	for key, value := range properties {
		this.executorData.GetContext().Props()[key] = value
	}
	cores, err := this.executorData.GetProperties().Cores()
	if err != nil {
		return ierror.Raise(err)
	}

	this.executorData.SetCores(int(cores))
	for key, value := range env {
		if err = os.Setenv(key, value); err != nil {
			return ierror.Raise(err)
		}
	}

	if _, present := os.LookupEnv("MPI_THREAD_MULTIPLE"); present {
		err = impi.MPI_Init_thread(nil, nil, impi.MPI_THREAD_MULTIPLE, nil)
		logger.Info("ServerModule: Mpi started in thread mode")
	} else {
		err = impi.MPI_Init(nil, nil)
		logger.Info("ServerModule: Mpi started")
	}
	if err != nil {
		return err
	}
	this.services(this.processor)
	logger.Info("ServerModule: go executor ready")
	return nil
}

func (this *IExecutorServerModule) Stop(ctx context.Context) (_err error) {
	server := this.server
	this.processor = nil
	this.server = nil
	var flag impi.C_int
	err := impi.MPI_Initialized(&flag)
	if flag != 0 && err == nil {
		_ = impi.MPI_Finalize()
	}
	go func() {
		time.Sleep(5 * time.Second)
		_ = server.Stop()
		if this.usock != nil {
			_ = os.Remove(*this.usock)
		}
	}()
	return nil
}

func (this *IExecutorServerModule) Test(ctx context.Context) (_r bool, _err error) {
	return true, nil
}
