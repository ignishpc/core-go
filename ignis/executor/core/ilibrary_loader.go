package core

import (
	"ignis/executor/core/ierror"
	"os"
	"plugin"
	"strings"
)

type ILibraryLoader struct {
	executorData *IExecutorData
}

func NewILibraryLoader(executorData *IExecutorData) *ILibraryLoader {
	return &ILibraryLoader{
		executorData,
	}
}

func (this *ILibraryLoader) LoadFunction(name string) (any, error) {
	if strings.LastIndex(name, "func") == 0 {
		if aux, err := this.loadLambda(name); err != nil {
			return nil, err
		} else {
			name = aux
		}
	}
	sep := strings.Index(name, ":")
	if sep == -1 {
		return nil, ierror.RaiseMsg(name + " is not a valid Go class")
	}

	path := name[:sep]
	class_name := name[sep+1:]

	if _, err := os.Stat(path); err == nil {
		return nil, ierror.RaiseMsgCause(path+" was not found", err)
	}

	library, err := plugin.Open(path)
	if err != nil {
		return nil, ierror.RaiseMsgCause(path+" could not be loaded", err)
	}

	v, err := library.Lookup("New" + class_name)
	if err != nil {
		return nil, ierror.RaiseMsgCause(class_name+"must have a constructor function New"+
			class_name+"() *"+class_name+" {...} in you library", err)
	}

	vf, ok := v.(func() any)
	if !ok {
		return nil, ierror.RaiseMsg("New" + class_name + " must have no arguments")
	}
	return vf(), nil
}

func (this *ILibraryLoader) LoadLibrary(path string) ([]string, error) {
	if strings.Contains(path, "\n") {
		if aux, err := this.loadSource(path); err != nil {
			return nil, err
		} else {
			path = aux
		}
	}

	if _, err := os.Stat(path); err == nil {
		return nil, ierror.RaiseMsgCause(path+" was not found", err)
	}

	library, err := plugin.Open(path)
	if err != nil {
		return nil, ierror.RaiseMsgCause(path+" could not be loaded", err)
	}

	v, err := library.Lookup("IgnisLibrary")
	if err != nil {
		return nil, ierror.RaiseMsgCause("IgnisLibrary is required to register a python file as ignis library", err)
	}

	ignis_library, ok := v.([]string)
	if !ok {
		return nil, ierror.RaiseMsg("IgnisLibrary must be []string")
	}
	return ignis_library, nil
}

func (this *ILibraryLoader) compile(str string) (string, error) {
	return "", ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *ILibraryLoader) loadLambda(str string) (string, error) {
	return "", ierror.RaiseMsg("Not implemented yet") //TODO
}

func (this *ILibraryLoader) loadSource(str string) (string, error) {
	return "", ierror.RaiseMsg("Not implemented yet") //TODO
}
