package ierror

import (
	"fmt"
	"runtime"
	"strings"
)

type IExecutorError struct {
	message string
	stack   string
	cause   error
}

func RaiseMsg(message string) *IExecutorError {
	return &IExecutorError{
		message,
		stack(),
		nil,
	}
}

func RaiseMsgCause(message string, cause error) *IExecutorError {
	return &IExecutorError{
		message,
		stack(),
		cause,
	}
}
func (this *IExecutorError) GetMessage() string {
	return this.message
}

func (this *IExecutorError) Error() string {
	var sb strings.Builder
	sb.WriteString("GoExecutorError: ")
	sb.WriteString(this.message)
	sb.WriteString("\n")
	sb.WriteString(this.stack)
	if this.cause != nil {
		sb.WriteString("Caused by: ")
		sb.WriteString(this.cause.Error())
	}
	return sb.String()
}

func stack() string {
	rpc := make([]uintptr, 10)
	runtime.Callers(3, rpc)
	frames := runtime.CallersFrames(rpc)
	frame, more := frames.Next()
	var sb strings.Builder
	for ; more; frame, more = frames.Next() {
		sb.WriteString(fmt.Sprintf("\tat %s (%s:%d)\n", frame.Function, frame.File, frame.Line))
	}
	if more {
		sb.WriteString("\t... more\n")
	}

	return sb.String()
}
