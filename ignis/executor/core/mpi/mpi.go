package mpi
/*
// #cgo LDFLAGS: -lmpi
// #include <mpi.h>
import "C"
*/

const (
	THREAD_SINGLE = 0
	THREAD_FUNNELED = 1
	THREAD_SERIALIZED = 2
	THREAD_MULTIPLE = 3
)

func Is_initialized() bool{
	return true
}

func Init_thread(level int) {
	//var provided C.int
	//C.MPI_Init_thread(nil, nil, C.int(level), &provided)
}

func Init() {
	//C.MPI_Init(nil, nil)
}

func Finalize() {
	//C.MPI_Finalize()
}


