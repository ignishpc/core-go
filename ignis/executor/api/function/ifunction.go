package function

import (
	"ignis/executor/api"
)

type IBaseFunction interface {
	Before(context api.IContext) error
}

type IBeforeFunction interface {
	Before(context api.IContext) error
}

type IFunction[T any, R any] interface {
	Before(context api.IContext) error
	Call(v T, context api.IContext) (R, error)
	After(context api.IContext) error
}

type IFunction0[R any] interface {
	Before(context api.IContext) error
	Call(context api.IContext) (R, error)
	After(context api.IContext) error
}

type IFunction2[T1 any, T2 any, R any] interface {
	Before(context api.IContext) error
	Call(v1 T1, v2 T2, context api.IContext) (R, error)
	After(context api.IContext) error
}

type IVoidFunction[T any] interface {
	Before(context api.IContext) error
	Call(v T, context api.IContext) error
	After(context api.IContext) error
}

type IVoidFunction0 interface {
	Before(context api.IContext) error
	Call(context api.IContext) error
	After(context api.IContext) error
}

type IVoidFunction2[T1 any, T2 any] interface {
	Before(context api.IContext) error
	Call(v1 T1, v2 T2, context api.IContext) error
	After(context api.IContext) error
}

type IBeforeNone struct{}

type IAfterNone struct{}

type IOnlyCall struct {
	IBeforeNone
	IAfterNone
}

func (this *IBeforeNone) Before(context api.IContext) error {
	return nil
}

func (this *IAfterNone) After(context api.IContext) error {
	return nil
}
