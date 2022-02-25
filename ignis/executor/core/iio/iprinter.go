package iio

import (
	"fmt"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"io"
	"reflect"
	"strings"
)

type IPrinter interface {
	Print(stream io.Writer, obj any, level int) error
	SetPrinter(id string, value IPrinter)
	GetPrinter(rt string) IPrinter
}

type IPrinterF func(stream io.Writer, obj any, level int) error

type IPrinterType struct {
	print    IPrinterF
	printers map[string]IPrinter
	ptr      IPrinter
	array    IPrinter
}

func (this *IPrinterType) Print(stream io.Writer, obj any, level int) error {
	return this.print(stream, obj, level)
}

func (this *IPrinterType) SetPrinter(id string, value IPrinter) {
	if value != nil {
		switch id {
		case "*":
			this.ptr = value
		case "[]":
			this.array = value
		default:
			this.printers[id] = value
		}
	} else {
		delete(this.printers, id)
	}
}

func (this *IPrinterType) GetPrinter(id string) IPrinter {
	switch id[0] {
	case '*':
		if this.ptr != nil {
			return this.ptr
		}
	case '[':
		if this.array != nil {
			return this.array
		}
	}
	if len(this.printers) > 0 {
		if printer, present := this.printers[id]; present {
			return printer
		}
		i := strings.IndexByte(id, '[')
		if i > 0 {
			printer, present := this.printers[id[0:i]]
			if present {
				return printer
			}
		}
	}
	return this
}

var globalPrinter IPrinter

func Print[T any](stream io.Writer, obj T) error {
	return GetPrinter(GetName(obj)).Print(stream, obj, 0)
}

func SetPrinter(id string, value IPrinter) {
	globalPrinter.SetPrinter(id, value)
}

func GetPrinter(id string) IPrinter {
	return globalPrinter.GetPrinter(id)
}

func NewIPrinterType(f IPrinterF) IPrinter {
	return &IPrinterType{print: f, printers: map[string]IPrinter{}}
}

type IPointerPrinterType[T any] struct {
	IPrinterType
}

func (this *IPointerPrinterType[T]) Print(stream io.Writer, obj any, level int) error {
	return this.print(stream, *(obj.(*T)), level)
}

type IArrayPrinterType[T any] struct {
	IPrinterType
	valPrinter IPrinterType
}

func (this *IArrayPrinterType[T]) Print(stream io.Writer, obj any, level int) error {
	array := obj.([]T)
	if len(array) == 0 {
		return nil
	}

	tab := []byte(strings.Repeat(" ", level))
	sep := []byte("\n")
	for _, e := range array {
		if _, err := stream.Write(tab); err != nil {
			return ierror.Raise(err)
		}
		if err := this.valPrinter.print(stream, e, level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write(sep); err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

type IMapPrinterType[K comparable, V any] struct {
	IPrinterType
	keyPrinter IPrinterType
	valPrinter IPrinterType
}

func (this *IMapPrinterType[K, V]) Print(stream io.Writer, obj any, level int) error {
	m := obj.(map[K]V)
	if len(m) == 0 {
		return nil
	}

	init := []byte(strings.Repeat(" ", level) + "(")
	sep := []byte(", ")
	end := []byte(")")
	for k, v := range m {
		if _, err := stream.Write(init); err != nil {
			return ierror.Raise(err)
		}
		if err := this.keyPrinter.print(stream, k, level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write(sep); err != nil {
			return ierror.Raise(err)
		}
		if err := this.valPrinter.Print(stream, v, level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write(end); err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

type IPairPrinterType[T1 any, T2 any] struct {
	IPrinterType
	firstPrinter  IPrinterType
	secondPrinter IPrinterType
}

func (this *IPairPrinterType[T1, T2]) Print(stream io.Writer, obj any, level int) error {
	pair := obj.(ipair.IPair[T1, T2])

	if _, err := stream.Write([]byte("(")); err != nil {
		return ierror.Raise(err)
	}
	if err := this.firstPrinter.print(stream, pair.First, level+1); err != nil {
		return ierror.Raise(err)
	}
	if _, err := stream.Write([]byte(", ")); err != nil {
		return ierror.Raise(err)
	}
	if err := this.secondPrinter.Print(stream, pair.Second, level+1); err != nil {
		return ierror.Raise(err)
	}
	if _, err := stream.Write([]byte(")")); err != nil {
		return ierror.Raise(err)
	}
	return nil
}

func init() {
	globalPrinter = NewIPrinterType(func(stream io.Writer, obj any, level int) error {
		_, err := fmt.Fprintf(stream, "%+v", obj)
		return err
	})
	SetPrinter(TypeGenericName[*any](), NewIPrinterType(func(stream io.Writer, obj any, level int) error {
		elem := reflect.ValueOf(obj).Elem()
		return GetPrinter(elem.Type().String()).Print(stream, elem.Interface(), level)
	}))
	SetPrinter(TypeGenericName[[]any](), NewIPrinterType(func(stream io.Writer, obj any, level int) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())

		tpElem := rt.Elem()
		if sz > 0 && tpElem.Kind() == reflect.Interface {
			tpElem = value.Index(0).Elem().Type()
		}

		printer := GetPrinter(tpElem.String())

		tab := []byte(strings.Repeat(" ", level))
		sep := []byte("\n")
		for i := 0; i < int(sz); i++ {
			if _, err := stream.Write(tab); err != nil {
				return ierror.Raise(err)
			}
			if err := printer.Print(stream, value.Index(i).Interface(), level+1); err != nil {
				return ierror.Raise(err)
			}
			if _, err := stream.Write(sep); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetPrinter(TypeGenericName[[]byte](), NewIPrinterType(func(stream io.Writer, obj any, level int) error {
		if _, err := fmt.Fprintf(stream, "%v", obj); err != nil {
			return ierror.Raise(err)
		}
		return nil
	}))
	SetPrinter(TypeGenericName[map[any]any](), NewIPrinterType(func(stream io.Writer, obj any, level int) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()
		sz := int64(value.Len())

		keyRt := rt.Key()
		valRt := rt.Elem()

		if sz > 0 {
			if keyRt.Kind() == reflect.Interface {
				it := value.MapRange()
				it.Next()
				keyRt = it.Key().Elem().Type()
			}
			if valRt.Kind() == reflect.Interface {
				it := value.MapRange()
				it.Next()
				valRt = it.Key().Elem().Type()
			}
		}

		keyPrinter := GetPrinter(keyRt.String())
		valPrinter := GetPrinter(keyRt.String())
		init := []byte(strings.Repeat(" ", level) + "(")
		sep := []byte(", ")
		end := []byte(")")
		it := value.MapRange()
		for it.Next() {
			if _, err := stream.Write(init); err != nil {
				return ierror.Raise(err)
			}
			if err := keyPrinter.Print(stream, it.Key().Interface(), level+1); err != nil {
				return ierror.Raise(err)
			}
			if _, err := stream.Write(sep); err != nil {
				return ierror.Raise(err)
			}
			if err := valPrinter.Print(stream, it.Value().Interface(), level+1); err != nil {
				return ierror.Raise(err)
			}
			if _, err := stream.Write(end); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetPrinter(TypeGenericName[ipair.IPair[any, any]](), NewIPrinterType(func(stream io.Writer, obj any, level int) error {
		value := reflect.ValueOf(obj)
		rt := value.Type()

		firstRt := rt.Field(0).Type
		secondRt := rt.Field(1).Type
		if firstRt.Kind() == reflect.Interface {
			firstRt = value.Field(0).Elem().Type()
		}
		if secondRt.Kind() == reflect.Interface {
			secondRt = value.Field(1).Elem().Type()
		}

		firstPrinter := GetPrinter(firstRt.String())
		secondPrinter := GetPrinter(secondRt.String())

		if _, err := stream.Write([]byte("(")); err != nil {
			return ierror.Raise(err)
		}
		if err := firstPrinter.Print(stream, value.Field(0).Interface(), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write([]byte(", ")); err != nil {
			return ierror.Raise(err)
		}
		if err := secondPrinter.Print(stream, value.Field(1).Interface(), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write([]byte(")")); err != nil {
			return ierror.Raise(err)
		}

		return nil
	}))
}
