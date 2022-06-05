package itype

import (
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
)

func DefaultTypes() []api.IContextType {
	return []api.IContextType{
		base.NewTypeC[bool](),
		base.NewTypeC[int8](),
		base.NewTypeC[int16](),
		base.NewTypeC[int32](),
		base.NewTypeC[int64](),
		base.NewTypeC[int](),
		base.NewTypeC[uint8](),
		base.NewTypeC[uint16](),
		base.NewTypeC[uint32](),
		base.NewTypeC[uint64](),
		base.NewTypeC[uint](),
		base.NewTypeC[float32](),
		base.NewTypeC[float64](),
		base.NewTypeC[string](),
		//
		base.NewTypeCC[int64, bool](),
		base.NewTypeCC[int64, int64](),
		base.NewTypeCC[int64, float64](),
		base.NewTypeCC[int64, string](),
		base.NewTypeCC[float64, bool](),
		base.NewTypeCC[float64, int64](),
		base.NewTypeCC[float64, float64](),
		base.NewTypeCC[float64, string](),
		base.NewTypeCC[string, bool](),
		base.NewTypeCC[string, int64](),
		base.NewTypeCC[string, float64](),
		base.NewTypeCC[string, string](),
	}
}

func DefaultFunctions() []function.IBaseFunction {
	return []function.IBaseFunction{}
}
