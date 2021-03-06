package api

import (
	"ignis/executor/core/impi"
)

type IContext interface {
	Threads() int
	ThreadId() int
	Executors() int
	ExecutorId() int
	MpiGroup() impi.C_MPI_Comm
	Props() map[string]string
	Vars() map[string]any
	Register(tp IContextType)
}

type ITypeBase interface {
	Types() []IContextType
}

type IContextType interface {
	Name() string
	AddType(tp IContextType)
	LoadType()
}
