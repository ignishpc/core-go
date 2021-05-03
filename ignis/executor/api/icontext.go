package api

type IContext struct {
	properties map[string]string
	variables  map[string]interface{}
}

func NewIContext() *IContext {
	return &IContext{
	}
}

func (this *IContext) Cores() int {
	return 0
}

func (this *IContext) Executors() int {
	return 0
}

func (this *IContext) ExecutorId() int {
	return 0
}

func (this *IContext) ThreadId() int {
	return 0
}

func (this *IContext) Props() map[string]string {
	return this.properties
}

func (this *IContext) Vars() map[string]interface{} {
	return this.variables
}
