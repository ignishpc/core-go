package core

import (
	"bufio"
	"errors"
	"ignis/executor/api"
	"ignis/executor/api/function"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/itransport"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/rpc"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type IExecutorData struct {
	partitions     storage.IPartitionGroupBase
	convPartitions storage.IPartitionGroupBase
	variables      map[string]any
	functions      map[string]function.IBaseFunction
	baseTypes      map[string]api.IContextType
	libraryLoader  ILibraryLoader
	properties     IPropertyParser
	partitionTools IPartitionTools
	context        *iContextImpl
	mpi_           IMpi
}

func NewIExecutorData() *IExecutorData {
	this := &IExecutorData{
		variables: make(map[string]any),
		functions: make(map[string]function.IBaseFunction),
		baseTypes: make(map[string]api.IContextType),
		context:   NewIContext().(*iContextImpl),
	}

	this.libraryLoader.executorData = this
	this.partitionTools.properties = &this.properties
	this.properties.properties = this.context.properties

	this.mpi_.propertyParser = &this.properties
	this.mpi_.partitionTools = &this.partitionTools
	this.mpi_.context = this.context

	return this
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
	if group, ok := this.convPartitions.(*storage.IPartitionGroup[T]); ok {
		return group, nil
	}
	conv_partitions, err := ConvertGroupPartitionTo[T](&this.partitionTools, this.partitions)
	if err != nil {
		return nil, err
	}
	this.convPartitions = conv_partitions
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
	this.convPartitions = nil
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
	this.context.mpiThreadGroup = []impi.C_MPI_Comm{comm}
	return nil
}

func (this *IExecutorData) DestroyMpiGroup() {
	for _, comm := range this.context.mpiThreadGroup {
		if comm != impi.MPI_COMM_WORLD {
			impi.MPI_Comm_free(&comm)
		}
	}
	this.SetMpiGroup(impi.MPI_COMM_WORLD)
}

func (this *IExecutorData) GetContext() api.IContext {
	return this.context
}

func (this *IExecutorData) GetThreadContext(thread int) api.IContext {
	return newIThreadContext(this.context, thread)
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
	ithreads.SetDefaultCores(n)
}

func (this *IExecutorData) GetCores() int {
	return ithreads.DefaultCores()
}

func (this *IExecutorData) GetMpiCores() int {
	return len(this.context.mpiThreadGroup)
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

	if mpiCores > 1 && len(this.context.mpiThreadGroup) == 1 && this.context.Executors() > 1 {
		this.context.mpiThreadGroup, err = this.Duplicate(this.context.mpiThreadGroup[0], mpiCores)
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
	for i := 1; i < threads; i++ {
		var dup impi.C_MPI_Comm
		if err := impi.MPI_Comm_dup(result[len(result)-1], &dup); err != nil {
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
	return this.loadLibrary(src, true, false)
}

func (this *IExecutorData) loadLibrary(src *rpc.ISource, withBackup bool, fast bool) (function.IBaseFunction, error) {
	if src.Obj.IsSetBytes() {
		return nil, ierror.RaiseMsg("Go not support function serialization")
	}
	sep := strings.IndexByte(*src.Obj.Name, ':')
	var baseFunction function.IBaseFunction
	if sep == -1 {
		if f, ok := this.functions[*src.Obj.Name]; ok {
			baseFunction = f
		} else {
			return nil, ierror.RaiseMsg("Function " + *src.Obj.Name + " not found")
		}
	} else {
		if f, err := this.libraryLoader.LoadFunction(*src.Obj.Name); err == nil {
			baseFunction = f.(function.IBaseFunction)
		} else {
			return nil, ierror.Raise(err)
		}

		this.RegisterFunction(baseFunction)

		if withBackup {
			info, err := this.InfoDirectory()
			if err != nil {
				return nil, ierror.Raise(err)
			}
			backupPath := info + "/sources" + strconv.Itoa(this.context.ExecutorId()) + ".bak"
			file, err := os.OpenFile(backupPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, ierror.Raise(err)
			}
			defer file.Close()
			if _, err := file.WriteString(*src.Obj.Name + "\n"); err != nil {
				return nil, ierror.Raise(err)
			}
		}
	}

	if !fast {
		if len(src.Params) > 0 {
			logger.Info("Loading user variables")
			if err := this.LoadParameters(src); err != nil {
				return nil, ierror.Raise(err)
			}
		}
		logger.Info("Function loaded")
	}

	return baseFunction, nil
}

func (this *IExecutorData) LoadLibraryFunctions(path string) error {
	return ierror.RaiseMsg("Not implemented yet")
}

func (this *IExecutorData) ReloadLibraries() error {
	info, err := this.InfoDirectory()
	if err != nil {
		return ierror.Raise(err)
	}
	backupPath := info + "/sources" + strconv.Itoa(this.context.ExecutorId()) + ".bak"
	if _, err = os.Stat(backupPath); errors.Is(err, os.ErrExist) {
		logger.Info("Function backup found, loading")
		file, err := os.Open(backupPath)
		if err != nil {
			return ierror.Raise(err)
		}
		defer file.Close()
		source := rpc.NewISource()
		source.Obj = rpc.NewIEncoded()
		loaded := map[string]bool{}
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()
			source.Obj.Name = &line
			if _, ok := loaded[line]; !ok && len(line) > 0 {
				_, err = this.loadLibrary(source, false, true)
				if err != nil {
					logger.Error(err)
				}
				loaded[line] = true
			}
		}
	}
	return nil
}

func (this *IExecutorData) RegisterFunction(f function.IBaseFunction) {
	types := f.(api.ITypeBase).Types()
	for _, tp := range types {
		this.RegisterType(tp)
	}
	this.functions[reflect.TypeOf(f).Elem().Name()] = f
}

func (this *IExecutorData) RegisterType(tp api.IContextType) {
	tp.LoadType()
	this.baseTypes[tp.Name()] = tp
}

func (this *IExecutorData) GetType(name string) api.IContextType {
	return this.baseTypes[name]
}

func (this *IExecutorData) LoadContextType() api.IContextType {
	var last api.IContextType
	for _, tp := range this.context.arrayTypes {
		this.RegisterType(tp)
		last = tp
	}
	return last
}

func (this *IExecutorData) LoadParameters(src *rpc.ISource) error {
	for key, value := range src.Params {
		buffer := itransport.NewIMemoryBufferWrapper(value, int64(len(value)), itransport.OBSERVE)
		proto := iprotocol.NewIObjectProtocol(buffer)
		if v, err := proto.ReadObject(); err != nil {
			return ierror.Raise(err)
		} else {
			this.context.Vars()[key] = v
		}
	}
	return nil
}
