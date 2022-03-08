package itype

import (
	"ignis/executor/api/base"
	"ignis/executor/core"
)

func DefaultTypes(executorData *core.IExecutorData) {
	base.RegisterType[bool](executorData.GetContext())
	base.RegisterType[int8](executorData.GetContext())
	base.RegisterType[int16](executorData.GetContext())
	base.RegisterType[int32](executorData.GetContext())
	base.RegisterType[int64](executorData.GetContext())
	base.RegisterType[int](executorData.GetContext())
	base.RegisterType[float32](executorData.GetContext())
	base.RegisterType[float64](executorData.GetContext())
	base.RegisterType[string](executorData.GetContext())
	//TODO
}

func DefaultFunctions(executorData *core.IExecutorData) {
	//TODO
}
