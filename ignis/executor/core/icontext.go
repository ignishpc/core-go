package core

import (
	"ignis/executor/api"
	"ignis/executor/core/impi"
	"ignis/executor/core/ithreads"
)

type iContextImpl struct {
	properties map[string]string
	variables  map[string]any
	comm       impi.C_MPI_Comm
}

func NewIContext() api.IContext {
	return &iContextImpl{}
}

func (this *iContextImpl) Cores() int {
	return ithreads.GetPoolCores()
}

func (this *iContextImpl) Executors() int {
	var sz impi.C_int
	impi.MPI_Comm_size(this.comm, (*impi.C_int)(&sz))
	return int(sz)
}

func (this *iContextImpl) ExecutorId() int {
	var rank impi.C_int
	impi.MPI_Comm_rank(this.comm, (*impi.C_int)(&rank))
	return int(rank)
}

func (this *iContextImpl) ThreadId() int {
	return ithreads.GetThreadId()
}

func (this *iContextImpl) MpiGroup() impi.C_MPI_Comm {
	return this.comm
}

func (this *iContextImpl) Props() map[string]string {
	return this.properties
}

func (this *iContextImpl) Vars() map[string]any {
	return this.variables
}
