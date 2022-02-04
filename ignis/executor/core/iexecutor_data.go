package core

import (
	"ignis/executor/api"
	"ignis/executor/api/function"
	"ignis/executor/core/storage"
	"ignis/rpc"
)

type IExecutorData struct {
	cores          int
	partitions     any
	variables      map[string]any
	library_loader ILibraryLoader
	properties     IPropertyParser
	partitionTools IPartitionTools
	context        api.IContext
}

func NewIExecutorData() *IExecutorData {
	return &IExecutorData{}
}

func (this *IExecutorData) GetPartitions() any {
	return nil
}

func (this *IExecutorData) GetAndDeletePartitions() any {
	return nil
}

func CheckPartitions[T any](group any) *storage.IPartitionGroup[T] {
	return nil
}

func (this *IExecutorData) SetPartitions(group any) {

}

func (this *IExecutorData) HasPartitions() bool {
	return false
}

func (this *IExecutorData) DeletePartitions() {

}

func (this *IExecutorData) SetVariable(key string, value *interface{}) *interface{} {
	return nil
}

func (this *IExecutorData) GetVariable(key string) *interface{} {
	return nil
}

func (this *IExecutorData) HasVariable(key string) bool {
	return false
}

func (this *IExecutorData) RemoveVariable(key string) {

}

func (this *IExecutorData) ClearVariables() {

}

func (this *IExecutorData) InfoDirectory() string {
	return "nil"
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

func (this *IExecutorData) SetCores(n int) {
	this.cores = n
}

func (this *IExecutorData) GetCores() int {
	return this.cores
}

func (this *IExecutorData) LoadLibrary(src *rpc.ISource) (function.IBaseFunction, error) {
	return nil, nil //TODO
}

func (this *IExecutorData) LoadLibraryNoBackup(src *rpc.ISource) (function.IBaseFunction, error) {
	return nil, nil //TODO
}
