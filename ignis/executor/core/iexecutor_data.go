package core

import (
	"ignis/executor/api"
	"ignis/executor/core/storage"
)

type IExecutorData struct {
	cores          int
	partitions     *storage.IPartitionGroup
	variables      map[string]interface{}
	properties     IPropertyParser
	partitionTools IPartitionTools
	context        api.IContext
}

func NewIExecutorData() *IExecutorData {
	return &IExecutorData{
	}
}

func (this *IExecutorData) GetPartitions() *storage.IPartitionGroup {
	return nil
}

func (this *IExecutorData) GetAndDeletePartitions() *storage.IPartitionGroup {
	return nil
}

func (this *IExecutorData) SetPartitions(*storage.IPartitionGroup) {

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

func (this *IExecutorData) GetContext() *api.IContext {
	return &this.context
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
