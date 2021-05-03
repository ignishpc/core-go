package function

import (
	"ignis/executor/api"
)

type IBeforeFunction interface {
	before(context *api.IContext) error
}

type IFunction interface {
	before(context *api.IContext) error
	call(v interface{}, context *api.IContext) (interface{}, error)
	after(context *api.IContext) error
}

type IFunction0 interface {
	before(context *api.IContext) error
	call(context *api.IContext) (interface{}, error)
	after(context *api.IContext) error
}

type IFunction2 interface {
	before(context *api.IContext) error
	call(v1, v2 interface{}, context *api.IContext) (interface{}, error)
	after(context *api.IContext) error
}

type IVoidFunction interface {
	before(context *api.IContext) error
	call(v interface{}, context *api.IContext) error
	after(context *api.IContext) error
}

type IVoidFunction0 interface {
	before(context *api.IContext) error
	call(context *api.IContext) error
	after(context *api.IContext) error
}

type IVoidFunction2 interface {
	before(context *api.IContext) error
	call(v1, v2 interface{}, context *api.IContext) error
	after(context *api.IContext) error
}
