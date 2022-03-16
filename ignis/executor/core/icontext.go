package core

import (
	"ignis/executor/api"
	"ignis/executor/core/impi"
	"ignis/executor/core/ithreads"
)

type iContextImpl struct {
	properties     map[string]string
	variables      map[string]any
	arrayTypes     []api.IContextType
	mpiThreadGroup []impi.C_MPI_Comm
}

func NewIContext() api.IContext {
	return &iContextImpl{
		mpiThreadGroup: []impi.C_MPI_Comm{impi.MPI_COMM_WORLD},
	}
}

func (this *iContextImpl) Cores() int {
	return ithreads.DefaultCores()
}

func (this *iContextImpl) Executors() int {
	var sz impi.C_int
	impi.MPI_Comm_size(this.MpiGroup(), (*impi.C_int)(&sz))
	return int(sz)
}

func (this *iContextImpl) ExecutorId() int {
	var rank impi.C_int
	impi.MPI_Comm_rank(this.MpiGroup(), (*impi.C_int)(&rank))
	return int(rank)
}

func (this *iContextImpl) ThreadId() int {
	return ithreads.ThreadId()
}

func (this *iContextImpl) MpiGroup() impi.C_MPI_Comm {
	if len(this.mpiThreadGroup) == 1 {
		return this.mpiThreadGroup[0]
	}

	id := this.ThreadId()

	if id < len(this.mpiThreadGroup) {
		return this.mpiThreadGroup[id]
	}

	return impi.MPI_COMM_NULL
}

func (this *iContextImpl) Props() map[string]string {
	return this.properties
}

func (this *iContextImpl) Vars() map[string]any {
	return this.variables
}

func (this *iContextImpl) Register(tp api.IContextType) {
	this.arrayTypes = append(this.arrayTypes, tp)
}
