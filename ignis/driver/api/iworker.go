package api

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/driver/core"
	"ignis/rpc/driver"
)

type IWorker struct {
	cluster *ICluster
	id      *driver.IWorkerId
}

func NewIWorkerDefault(cluster *ICluster, tp string) (*IWorker, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().NewInstance_(context.Background(), cluster.id, tp)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IWorker{cluster, id}, nil
}

func NewIWorkerName(cluster *ICluster, tp, name string) (*IWorker, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().NewInstance3_(context.Background(), cluster.id, tp, name)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IWorker{cluster, id}, nil
}

func NewIWorkerCores(cluster *ICluster, tp string, cores int, instances int) (*IWorker, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().NewInstance4_(context.Background(), cluster.id, tp, int32(cores), int32(instances))
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IWorker{cluster, id}, nil
}

func NewIWorker(cluster *ICluster, tp, name string, cores int, instances int) (*IWorker, error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().NewInstance5_(context.Background(), cluster.id, tp, name, int32(cores), int32(instances))
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IWorker{cluster, id}, nil
}

func (this *IWorker) Start() error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetWorkerService().Start(context.Background(), this.id)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IWorker) Destroy() error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetWorkerService().Destroy(context.Background(), this.id)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IWorker) GetCluster() *ICluster {
	return this.cluster
}

func (this *IWorker) SetName(name string) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetWorkerService().SetName(context.Background(), this.id, name)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func Parallelize[T any](this *IWorker, data []T, partitions int64, src *ISource, native bool) (*IDataFrame[T], error) {
	id, err := core.Parallelize(Ignis.driverContext(), data, native)
	if err != nil {
		return nil, err
	}
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var dataId *driver.IDataFrameId
	if src == nil {
		dataId, err = client.Services().GetWorkerService().Parallelize(context.Background(), this.id, id, partitions)
	} else {
		dataId, err = client.Services().GetWorkerService().Parallelize4(context.Background(), this.id, id, partitions, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		dataId,
	}, nil
}

func ImportDataFrame[T any](this *IWorker, data *IDataFrame[T], src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetWorkerService().ImportDataFrame(context.Background(), this.id, data.id)
	} else {
		id, err = client.Services().GetWorkerService().ImportDataFrame3(context.Background(), this.id, data.id, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		id,
	}, nil
}

func (this *IWorker) TextFile(path string) (*IDataFrame[string], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().TextFile(context.Background(), this.id, path)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[string]{
		this,
		id,
	}, nil
}

func TextFile(this *IWorker, path string) (*IDataFrame[string], error) {
	return this.TextFile(path)
}

func (this *IWorker) TextFileN(path string, minPartitions int64) (*IDataFrame[string], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().TextFile3(context.Background(), this.id, path, minPartitions)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[string]{
		this,
		id,
	}, nil
}

func TextFileN(this *IWorker, path string, minPartitions int64) (*IDataFrame[string], error) {
	return this.TextFileN(path, minPartitions)
}

func (this *IWorker) PlainFile(path string, delim string) (*IDataFrame[string], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().PlainFile(context.Background(), this.id, path, delim)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[string]{
		this,
		id,
	}, nil
}

func PlainFile(this *IWorker, path string, delim string) (*IDataFrame[string], error) {
	return this.PlainFile(path, delim)
}

func (this *IWorker) PlainFileN(path string, minPartitions int64, delim string) (*IDataFrame[string], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().PlainFile4(context.Background(), this.id, path, minPartitions, delim)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[string]{
		this,
		id,
	}, nil
}

func PlainFileN(this *IWorker, path string, minPartitions int64, delim string) (*IDataFrame[string], error) {
	return this.PlainFileN(path, minPartitions, delim)
}

func PartitionObjectFile[T any](this *IWorker, path string, src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetWorkerService().PartitionObjectFile(context.Background(), this.id, path)
	} else {
		id, err = client.Services().GetWorkerService().PartitionObjectFile3(context.Background(), this.id, path, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		id,
	}, nil
}

func (this *IWorker) PartitionTextFile(path string) (*IDataFrame[string], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().PartitionTextFile(context.Background(), this.id, path)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[string]{
		this,
		id,
	}, nil
}

func PartitionTextFile(this *IWorker, path string) (*IDataFrame[string], error) {
	return this.PartitionTextFile(path)
}

func PartitionJsonFile[T any](this *IWorker, path string) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().PartitionJsonFile3a(context.Background(), this.id, path, false)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		id,
	}, nil
}

func PartitionJsonFileMapping[T any](this *IWorker, path string, src *ISource, objectMapping bool) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if src == nil {
		id, err = client.Services().GetWorkerService().PartitionJsonFile3a(context.Background(), this.id, path, objectMapping)
	} else {
		id, err = client.Services().GetWorkerService().PartitionJsonFile3b(context.Background(), this.id, path, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		id,
	}, nil
}

func (this *IWorker) LoadLibrary(path string) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetWorkerService().LoadLibrary(context.Background(), this.id, path)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IWorker) Execute(src *ISource) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil
	}
	defer client.Free()
	err = client.Services().GetWorkerService().Execute(context.Background(), this.id, src.rpc())
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func Execute(this *IWorker, src *ISource) error {
	return this.Execute(src)
}

func ExecuteTo[T any](this *IWorker, src *ISource) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetWorkerService().ExecuteTo(context.Background(), this.id, src.rpc())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		id,
	}, nil
}

func VoidCall[T any](this *IWorker, src *ISource, data *IDataFrame[T]) error {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	if data == nil {
		err = client.Services().GetWorkerService().VoidCall(context.Background(), this.id, src.rpc())
	} else {
		err = client.Services().GetWorkerService().VoidCall3(context.Background(), this.id, data.id, src.rpc())
	}
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func Call[T any](this *IWorker, src *ISource, data *IDataFrame[T]) (*IDataFrame[T], error) {
	client, err := Ignis.clientPool().GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	if data == nil {
		id, err = client.Services().GetWorkerService().Call(context.Background(), this.id, src.rpc())
	} else {
		id, err = client.Services().GetWorkerService().Call3(context.Background(), this.id, data.id, src.rpc())
	}
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IDataFrame[T]{
		this,
		id,
	}, nil
}
