package core

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/driver/api/derror"
	"ignis/rpc/driver"
	"time"
)

type IClient struct {
	transport         thrift.TTransport
	backendService    *driver.IBackendServiceClient
	propertiesService *driver.IPropertiesServiceClient
	clusterService    *driver.IClusterServiceClient
	workerService     *driver.IWorkerServiceClient
	dataframeService  *driver.IDataFrameServiceClient
}

func NewIClient(port int, compression int) (*IClient, error) {
	socket := thrift.NewTSocketConf(fmt.Sprintf("localhost:%d", port), &thrift.TConfiguration{})
	trans, err2 := thrift.NewTZlibTransport(socket, compression)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	for i := 0; i < 10; i++ {
		if err3 := trans.Open(); err3 != nil {
			if i == 9 {
				return nil, derror.NewIDriverErrorCause("Backend conection error", err3)
			}
			time.Sleep(time.Duration(i) * time.Second)
			continue
		}
		break
	}

	proto := thrift.NewTCompactProtocolConf(trans, &thrift.TConfiguration{})

	return &IClient{
		trans,
		driver.NewIBackendServiceClient(toClient(thrift.NewTMultiplexedProtocol(proto, "IBackend"))),
		driver.NewIPropertiesServiceClient(toClient(thrift.NewTMultiplexedProtocol(proto, "IProperties"))),
		driver.NewIClusterServiceClient(toClient(thrift.NewTMultiplexedProtocol(proto, "ICluster"))),
		driver.NewIWorkerServiceClient(toClient(thrift.NewTMultiplexedProtocol(proto, "IWorker"))),
		driver.NewIDataFrameServiceClient(toClient(thrift.NewTMultiplexedProtocol(proto, "IDataFrame"))),
	}, nil
}

func (this *IClient) close() {
	this.transport.Close()
}

func (this *IClient) GetBackendService() *driver.IBackendServiceClient {
	return this.backendService
}

func (this *IClient) GetPropertiesService() *driver.IPropertiesServiceClient {
	return this.propertiesService
}

func (this *IClient) GetClusterService() *driver.IClusterServiceClient {
	return this.clusterService
}

func (this *IClient) GetWorkerService() *driver.IWorkerServiceClient {
	return this.workerService
}

func (this *IClient) GetDataframeService() *driver.IDataFrameServiceClient {
	return this.dataframeService
}

func toClient(proto thrift.TProtocol) thrift.TClient {
	return thrift.NewTStandardClient(proto, proto)
}
