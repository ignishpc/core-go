package core

import (
	"ignis/executor/api"
	"ignis/executor/api/function"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/rpc"
	"math"
	"strconv"
)

type IExecutorData struct {
	cores           int
	partitions      storage.IPartitionGroupBase
	conv_partitions storage.IPartitionGroupBase
	variables       map[string]any
	library_loader  ILibraryLoader
	properties      IPropertyParser
	partitionTools  IPartitionTools
	context         api.IContext
	mpi_            IMpi
}

func NewIExecutorData() *IExecutorData {
	return &IExecutorData{}
}

func (this *IExecutorData) GetPartitionsAny() storage.IPartitionGroupBase {
	return this.partitions
}

func (this *IExecutorData) SetPartitionsAny(group storage.IPartitionGroupBase) {
	this.DeletePartitions()
	this.partitions = group
}

func GetPartitions[T any](this *IExecutorData) (*storage.IPartitionGroup[T], error) {
	if this.partitions == nil {
		return nil, nil
	}
	if group, ok := this.partitions.(*storage.IPartitionGroup[T]); ok {
		return group, nil
	}
	if group, ok := this.conv_partitions.(*storage.IPartitionGroup[T]); ok {
		return group, nil
	}
	conv_partitions, err := ConvertGroupPartitionTo[T](&this.partitionTools, this.partitions)
	if err != nil {
		return nil, err
	}
	this.conv_partitions = conv_partitions
	return conv_partitions, nil
}

func GetAndDeletePartitions[T any](this *IExecutorData) (*storage.IPartitionGroup[T], error) {
	group, err := GetPartitions[T](this)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	this.DeletePartitions()
	if group.Cache() {
		return group.ShadowCopy(), nil
	}
	return group, nil
}

func SetPartitions[T any](this *IExecutorData, group *storage.IPartitionGroup[T]) {
	this.DeletePartitions()
	this.partitions = group
}

func (this *IExecutorData) HasPartitions() bool {
	return this.partitions != nil
}

func (this *IExecutorData) DeletePartitions() {
	this.partitions = nil
	this.conv_partitions = nil
}

func SetVariable[T any](this *IExecutorData, key string, value T) {
	this.variables[key] = value
}

func GetVariable[T any](this *IExecutorData, key string) T {
	return this.variables[key].(T)
}

func (this *IExecutorData) HasVariable(key string) bool {
	_, ok := this.variables[key]
	return ok
}

func (this *IExecutorData) RemoveVariable(key string) {
	delete(this.variables, key)
}

func (this *IExecutorData) ClearVariables() {
	this.variables = make(map[string]any)
}

func (this *IExecutorData) InfoDirectory() (string, error) {
	info, err := this.properties.ExecutorDirectory()
	if err != nil {
		return "", ierror.Raise(err)
	}
	info += "/info"
	if err = this.partitionTools.CreateDirectoryIfNotExists(info); err != nil {
		return "", ierror.Raise(err)
	}
	return info, nil
}

func (this *IExecutorData) SetMpiGroup(comm impi.C_MPI_Comm) error {
	return nil //TODO
}

func (this *IExecutorData) ReloadLibraries() error {
	return nil //TODO
}

func (this *IExecutorData) GetContext() api.IContext {
	return this.context
}

func (this *IExecutorData) GetProperties() *IPropertyParser {
	return &this.properties
}

func (this *IExecutorData) GetPartitionTools() *IPartitionTools {
	return &this.partitionTools
}

func (this *IExecutorData) Mpi() *IMpi {
	return &this.mpi_
}

func (this *IExecutorData) SetCores(n int) {
	this.cores = n
}

func (this *IExecutorData) GetCores() int {
	return this.cores
}

func (this *IExecutorData) GetMpiCores() int {
	return 0 //TODO
}

func (this *IExecutorData) EnableMpiCores() error {
	ratio, err := this.properties.TransportCores()
	if err != nil {
		return ierror.Raise(err)
	}
	mpiCores := 0
	if ratio > 1 {
		mpiCores = int(math.Min(float64(this.GetCores()), math.Ceil(ratio)))
	} else {
		mpiCores = int(math.Ceil(float64(this.GetCores()) * math.Ceil(ratio)))
	}

	context := this.context.(*iContextImpl)

	if mpiCores > 1 && len(context.mpiThreadGroup) == 1 && context.Executors() > 1 {
		context.mpiThreadGroup, err = this.Duplicate(context.mpiThreadGroup[0], mpiCores)
		if err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

func (this *IExecutorData) Duplicate(comm impi.C_MPI_Comm, threads int) ([]impi.C_MPI_Comm, error) {
	var result []impi.C_MPI_Comm
	result = append(result, comm)
	if threads == 1 {
		return result, nil
	}
	name := impi.C_ArrayFromString("ignis_thread_0")
	if err := impi.MPI_Comm_set_name(result[0], &name[0]); err != nil {
		return nil, ierror.Raise(err)
	}
	logger.Info("Duplicating mpi group for ", threads, " threads")
	for i := 0; i < threads; i++ {
		var dup impi.C_MPI_Comm
		if err := impi.MPI_Comm_dup(result[i-1], &dup); err != nil {
			return nil, ierror.Raise(err)
		}
		result = append(result, dup)
		name := impi.C_ArrayFromString("ignis_thread_" + strconv.Itoa(i))
		if err := impi.MPI_Comm_set_name(result[i], &name[0]); err != nil {
			return nil, ierror.Raise(err)
		}
		logger.Info("mpi group ", i, " ready")
	}
	return result, nil
}

func (this *IExecutorData) LoadLibrary(src *rpc.ISource) (function.IBaseFunction, error) {
	return nil, nil //TODO
}

func (this *IExecutorData) LoadLibraryNoBackup(src *rpc.ISource) (function.IBaseFunction, error) {
	return nil, nil //TODO
}
