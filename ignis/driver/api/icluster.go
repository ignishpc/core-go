package api

import (
	"context"
	"ignis/driver/api/derror"
)

type ICluster struct {
	id int64
}

func NewIClusterDefault() (*ICluster, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetClusterService().NewInstance0_(context.Background())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &ICluster{id}, nil
}

func NewIClusterProps(properties *IProperties) (*ICluster, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetClusterService().NewInstance1b_(context.Background(), properties.id)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &ICluster{id}, nil
}

func NewIClusterName(name string) (*ICluster, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetClusterService().NewInstance1a_(context.Background(), name)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &ICluster{id}, nil
}

func NewICluster(properties *IProperties, name string) (*ICluster, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetClusterService().NewInstance2_(context.Background(), name, properties.id)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &ICluster{id}, nil
}

func (this *ICluster) Start() error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().Start(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *ICluster) Destroy() error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().Destroy(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *ICluster) SetName(name string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().SetName(context.Background(), this.id, name)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *ICluster) Execute(cmd ...string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().Execute(context.Background(), this.id, cmd)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *ICluster) ExecuteScript(script string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().ExecuteScript(context.Background(), this.id, script)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *ICluster) SendFile(source, target string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().SendFile(context.Background(), this.id, source, target)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *ICluster) SendCompressedFile(source, target string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetClusterService().SendCompressedFile(context.Background(), this.id, source, target)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}
