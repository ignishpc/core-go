package derror

import (
	"strings"
)

type IDriverError struct {
	msg   string
	cause *string
}

func (this *IDriverError) HasCause() bool {
	return this.cause != nil
}

func (this *IDriverError) GetCause() string {
	return *this.cause
}

func (this *IDriverError) Error() string {
	var sb strings.Builder
	sb.WriteString("GoDriverError: ")
	sb.WriteString(this.msg)
	sb.WriteString("\n")
	if this.HasCause() {
		sb.WriteString("Caused by: ")
		sb.WriteString(this.GetCause())
	}
	return sb.String()
}

func NewIDriverError(msg string) *IDriverError {
	return &IDriverError{
		msg: msg,
	}
}

func NewIDriverErrorCause(msg string, cause error) *IDriverError {
	cmsg := cause.Error()
	return &IDriverError{
		msg:   msg,
		cause: &cmsg,
	}
}

func NewGenericIDriverError(cause error) *IDriverError {
	cmsg := cause.Error()
	return &IDriverError{
		msg:   "driver fails",
		cause: &cmsg,
	}
}
