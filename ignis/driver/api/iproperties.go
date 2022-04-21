package api

import (
	"context"
	"ignis/driver/api/derror"
)

type IProperties struct {
	id int64
}

func NewIProperties() (*IProperties, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetPropertiesService().NewInstance_(context.Background())
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IProperties{
		id,
	}, nil
}

func NewIPropertiesCopy(props *IProperties) (*IProperties, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err := client.Services().GetPropertiesService().NewInstance2_(context.Background(), props.id)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return &IProperties{
		id,
	}, nil
}

func (this *IProperties) Set(key string, value string) (string, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Free()
	old, err := client.Services().GetPropertiesService().SetProperty(context.Background(), this.id, key, value)
	if err != nil {
		return "", derror.NewGenericIDriverError(err)
	}
	return old, nil
}

func (this *IProperties) Get(key string) (string, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Free()
	value, err := client.Services().GetPropertiesService().GetProperty(context.Background(), this.id, key)
	if err != nil {
		return "", derror.NewGenericIDriverError(err)
	}
	return value, nil
}

func (this *IProperties) Rm(key string) (string, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return "", err
	}
	defer client.Free()
	value, err := client.Services().GetPropertiesService().RmProperty(context.Background(), this.id, key)
	if err != nil {
		return "", derror.NewGenericIDriverError(err)
	}
	return value, nil
}

func (this *IProperties) Contains(key string) (bool, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return false, err
	}
	defer client.Free()
	found, err := client.Services().GetPropertiesService().Contains(context.Background(), this.id, key)
	if err != nil {
		return false, derror.NewGenericIDriverError(err)
	}
	return found, nil
}

func (this *IProperties) ToMap(defaults bool) (map[string]string, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	m, err := client.Services().GetPropertiesService().ToMap(context.Background(), this.id, defaults)
	if err != nil {
		return nil, derror.NewGenericIDriverError(err)
	}
	return m, nil
}

func (this *IProperties) FromMap(m map[string]string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetPropertiesService().FromMap(context.Background(), this.id, m)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IProperties) Load(path string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetPropertiesService().Load(context.Background(), this.id, path)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IProperties) Store(path string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetPropertiesService().Store(context.Background(), this.id, path)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}

func (this *IProperties) Clear() error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err = client.Services().GetPropertiesService().Clear(context.Background(), this.id)
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	return nil
}
