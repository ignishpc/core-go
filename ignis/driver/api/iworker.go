package api

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/rpc/driver"
)

type IWorker struct {
	cluster *ICluster
	id      *driver.IWorkerId
}

func NewIWorkerDefault(cluster *ICluster, tp string) (*IWorker, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().NewInstance_(context.Background(), cluster.id, tp)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IWorker{cluster, id}, nil
}

func NewIWorkerName(cluster *ICluster, tp, name string) (*IWorker, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().NewInstance3a_(context.Background(), cluster.id, tp, name)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IWorker{cluster, id}, nil
}

func NewIWorkerCores(cluster *ICluster, tp string, cores int) (*IWorker, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().NewInstance3b_(context.Background(), cluster.id, tp, int32(cores))
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IWorker{cluster, id}, nil
}

func NewIWorker(cluster *ICluster, tp, name string, cores int) (*IWorker, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().NewInstance4_(context.Background(), cluster.id, tp, name, int32(cores))
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IWorker{cluster, id}, nil
}

func (this *IWorker) Start() error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetWorkerService().Start(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IWorker) Destroy() error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetWorkerService().Destroy(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IWorker) GetCluster() *ICluster {
	return this.cluster
}

func (this *IWorker) SetName(name string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetWorkerService().SetName(context.Background(), this.id, name)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IWorker) Parallelize(data interface{}, partitions int64, src *ISource, native bool) (*IDataFrame, error) {
	id, err := Ignis.callback.DriverContext().Parallelize(data, native)
	if err != nil {
		return nil, err
	}
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var err2 error
	var dataId *driver.IDataFrameId
	if src == nil {
		dataId, err2 = client.Services().GetWorkerService().Parallelize(context.Background(), this.id, id, partitions)
	} else {
		dataId, err2 = client.Services().GetWorkerService().Parallelize4(context.Background(), this.id, id, partitions, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame{
		this,
		dataId,
	}, nil
}

func (this *IWorker) ImportDataFrameDefault(data *IDataFrame, src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var err2 error
	var id *driver.IDataFrameId
	if src == nil {
		id, err2 = client.Services().GetWorkerService().ImportDataFrame(context.Background(), this.id, data.id)
	} else {
		id, err2 = client.Services().GetWorkerService().ImportDataFrame3b(context.Background(), this.id, data.id, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) ImportDataFrame(data *IDataFrame, partitions int64, src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var err2 error
	var id *driver.IDataFrameId
	if src == nil {
		id, err2 = client.Services().GetWorkerService().ImportDataFrame3a(context.Background(), this.id, data.id, partitions)
	} else {
		id, err2 = client.Services().GetWorkerService().ImportDataFrame4(context.Background(), this.id, data.id, partitions, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) TextFileDefault(path string) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().TextFile(context.Background(), this.id, path)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) TextFile(path string, minPartitions int64) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().TextFile3(context.Background(), this.id, path, minPartitions)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) PartitionObjectFile(path string, src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var err2 error
	var id *driver.IDataFrameId
	if src == nil {
		id, err2 = client.Services().GetWorkerService().PartitionObjectFile(context.Background(), this.id, path)
	} else {
		id, err2 = client.Services().GetWorkerService().PartitionObjectFile3(context.Background(), this.id, path, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) PartitionTextFile(path string) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().PartitionTextFile(context.Background(), this.id, path)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) PartitionJsonFileDefault(path string) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().PartitionJsonFile3a(context.Background(), this.id, path, false)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) PartitionJsonFile(path string, src *ISource, objectMapping bool) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var err2 error
	var id *driver.IDataFrameId
	if src == nil {
		id, err2 = client.Services().GetWorkerService().PartitionJsonFile3a(context.Background(), this.id, path, objectMapping)
	} else {
		id, err2 = client.Services().GetWorkerService().PartitionJsonFile3b(context.Background(), this.id, path, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) LoadLibrary(path string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetWorkerService().LoadLibrary(context.Background(), this.id, path)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IWorker) Execute(src *ISource) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil
	}
	defer client.Free()
	err2 := client.Services().GetWorkerService().Execute(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IWorker) ExecuteTo(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetWorkerService().ExecuteTo(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}

func (this *IWorker) VoidCall(src *ISource, data *IDataFrame) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	var err2 error
	if data == nil {
		err2 = client.Services().GetWorkerService().VoidCall(context.Background(), this.id, src.rpc())
	} else {
		err2 = client.Services().GetWorkerService().VoidCall3(context.Background(), this.id, data.id, src.rpc())
	}
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IWorker) Call(src *ISource, data *IDataFrame) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var err2 error
	var id *driver.IDataFrameId
	if src == nil {
		id, err2 = client.Services().GetWorkerService().Call(context.Background(), this.id, src.rpc())
	} else {
		id, err2 = client.Services().GetWorkerService().Call3(context.Background(), this.id, data.id, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this,
		id,
	}, nil
}
