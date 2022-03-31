package iio

import (
	"fmt"
	"ignis/executor/api/ipair"
	"ignis/executor/core/ierror"
	"ignis/executor/core/utils"
	"io"
	"reflect"
	"strings"
	"unsafe"
)

type IPrinter interface {
	Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error
}

type IPrinterF func(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error

var printers = make(map[string]IPrinter)

func Print[T any](stream io.Writer, obj T) error {
	printer, rtp := GetPrinterObj(obj)
	return printer.Print(stream, rtp, unsafe.Pointer(&obj), 0)
}

func SetPrinter(id string, value IPrinter) {
	id = NameFix(id)
	if value != nil {
		printers[id] = value
	} else {
		delete(printers, id)
	}
}

func GetPrinter(id string) IPrinter {
	id = NameFix(id)
	if printer, present := printers[id]; present {
		return printer
	}
	return nil
}

func getPrinterAux(id string) IPrinter {
	id = NameFix(id)
	if printer, present := printers[id]; present {
		return printer
	}
	name := id
	for true {
		i := strings.LastIndexByte(name, '[')
		if i < 0 {
			break
		}
		name = name[:i]
		printer, present := printers[name]
		if present {
			return printer
		}
	}
	return &IPrinterType{func(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
		_, err := fmt.Fprintf(stream, "%+v", obj)
		return err
	}}
}

func GetPrinterObj[T any](obj T) (IPrinter, reflect.Type) {
	tp := reflect.TypeOf(&obj).Elem()
	if tp.Kind() == reflect.Pointer {
		tpe := tp.Elem()
		if tpe.Kind() == reflect.Pointer || tpe == anyType { // Pointer to pointer or pointer to any
			printer, atp := GetPrinterObj(reflect.ValueOf(obj).Elem().Interface())
			if tpe == anyType {
				return &IPointerPrinterType{atp, printer}, tp
			}
			aprinter := printer.(*IAnyPrinterType)
			return &IPointerPrinterType{aprinter.rtp, aprinter.valPrinter}, tp
		}
		printer := getPrinterAux(tpe.String())
		return &IAnyPrinterType{tpe, printer}, tp

	} else if tp == anyType {
		realTp := reflect.TypeOf(obj)
		if realTp == nil {
			//Collection created with any
		} else if realTp.Kind() == reflect.Pointer { //any is a pointer
			val := reflect.ValueOf(obj).Elem()
			ival := val.Interface()
			if ival != nil { //any stores something
				printer, atp := GetPrinterObj(ival)
				aPrinter := printer.(*IAnyPrinterType)
				var pPrinter IPrinter
				if val.Kind() != reflect.Interface { //any stores a pointer to any
					pPrinter = &IPointerPrinterType{aPrinter.rtp, aPrinter.valPrinter}
				} else {
					pPrinter = &IPointerPrinterType{atp, aPrinter}
				}
				return &IAnyPrinterType{realTp, pPrinter}, tp
			}
		} else {
			printer := getPrinterAux(realTp.String())
			return &IAnyPrinterType{realTp, printer}, tp
		}
	}
	printer := getPrinterAux(tp.String())
	return printer, tp
}

type IPrinterType struct {
	printer IPrinterF
}

func (this *IPrinterType) Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
	return this.printer(stream, rtp, obj, level)
}

type IAnyPrinterType struct {
	rtp        reflect.Type
	valPrinter IPrinter
}

func (this *IAnyPrinterType) Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
	return this.valPrinter.Print(stream, this.rtp, utils.GetAnyData((*any)(obj)), level)
}

type IPointerPrinterType struct {
	rtp        reflect.Type
	valPrinter IPrinter
}

func (this *IPointerPrinterType) Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
	return this.valPrinter.Print(stream, this.rtp, unsafe.Pointer(*(**any)(obj)), level)
}

func NewIPrinter(f IPrinterF) IPrinter {
	return &IPrinterType{f}
}

type IArrayPrinterType[T any] struct {
	rtp        reflect.Type
	valPrinter IPrinter
}

func (this *IArrayPrinterType[T]) Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
	array := (*[]T)(obj)
	if len(*array) == 0 {
		return nil
	}

	tab := []byte(strings.Repeat(" ", level))
	sep := []byte("\n")
	for _, e := range *array {
		if _, err := stream.Write(tab); err != nil {
			return ierror.Raise(err)
		}
		if err := this.valPrinter.Print(stream, this.rtp, unsafe.Pointer(&e), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write(sep); err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

type IMapPrinterType[K comparable, V any] struct {
	keyPrinter IPrinter
	keyRtp     reflect.Type
	valPrinter IPrinter
	valRtp     reflect.Type
}

func (this *IMapPrinterType[K, V]) Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
	m := (*map[K]V)(obj)
	if len(*m) == 0 {
		return nil
	}

	init := []byte(strings.Repeat(" ", level) + "(")
	sep := []byte(", ")
	end := []byte(")")
	for k, v := range *m {
		if _, err := stream.Write(init); err != nil {
			return ierror.Raise(err)
		}
		if err := this.keyPrinter.Print(stream, this.keyRtp, unsafe.Pointer(&k), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write(sep); err != nil {
			return ierror.Raise(err)
		}
		if err := this.valPrinter.Print(stream, this.valRtp, unsafe.Pointer(&v), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write(end); err != nil {
			return ierror.Raise(err)
		}
	}

	return nil
}

type IPairPrinterType[T any] struct {
	firstPrinter  IPrinter
	firstRtp      reflect.Type
	secondPrinter IPrinter
	secondRtp     reflect.Type
}

func (this *IPairPrinterType[T]) Print(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
	p := any((*T)(obj)).(ipair.IAbstractPair)

	init := []byte(strings.Repeat(" ", level) + "(")
	sep := []byte(", ")
	end := []byte(")")

	if _, err := stream.Write(init); err != nil {
		return ierror.Raise(err)
	}
	if err := this.firstPrinter.Print(stream, this.firstRtp, p.GetFirstPointer(), level+1); err != nil {
		return ierror.Raise(err)
	}
	if _, err := stream.Write(sep); err != nil {
		return ierror.Raise(err)
	}
	if err := this.secondPrinter.Print(stream, this.secondRtp, p.GetSecondPointer(), level+1); err != nil {
		return ierror.Raise(err)
	}
	if _, err := stream.Write(end); err != nil {
		return ierror.Raise(err)
	}

	return nil
}

func init() {
	SetPrinter(TypeGenericName[[]any](), NewIPrinter(func(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
		value := reflect.NewAt(rtp, obj).Elem()
		rt := value.Type()
		sz := int64(value.Len())

		var printer IPrinter
		var vtp reflect.Type

		if sz == 0 {
			printer, vtp = GetPrinterObj(reflect.New(rt.Elem()).Elem().Interface())
		} else {
			printer, vtp = GetPrinterObj(value.Index(0).Interface())
		}

		tab := []byte(strings.Repeat(" ", level))
		sep := []byte("\n")
		for i := 0; i < int(sz); i++ {
			e := value.Index(i).Interface()
			if _, err := stream.Write(tab); err != nil {
				return ierror.Raise(err)
			}
			if err := printer.Print(stream, vtp, unsafe.Pointer(&e), level+1); err != nil {
				return ierror.Raise(err)
			}
			if _, err := stream.Write(sep); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetPrinter(TypeGenericName[map[any]any](), NewIPrinter(func(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
		value := reflect.NewAt(rtp, obj).Elem()
		rt := value.Type()
		sz := int64(value.Len())

		var keyPrinter IPrinter
		var ktp reflect.Type
		var valuePrinter IPrinter
		var vtp reflect.Type

		if sz == 0 {
			keyPrinter, ktp = GetPrinterObj(reflect.New(rt.Key()).Elem().Interface())
			valuePrinter, vtp = GetPrinterObj(reflect.New(rt.Elem()).Elem().Interface())
		} else {
			it := value.MapRange()
			it.Next()
			keyPrinter, ktp = GetPrinterObj(it.Key().Interface())
			valuePrinter, vtp = GetPrinterObj(it.Value().Interface())
		}

		init := []byte(strings.Repeat(" ", level) + "(")
		sep := []byte(", ")
		end := []byte(")")
		it := value.MapRange()
		for it.Next() {
			if _, err := stream.Write(init); err != nil {
				return ierror.Raise(err)
			}
			e := it.Key().Interface()
			if err := keyPrinter.Print(stream, ktp, unsafe.Pointer(&e), level+1); err != nil {
				return ierror.Raise(err)
			}
			if _, err := stream.Write(sep); err != nil {
				return ierror.Raise(err)
			}
			e = it.Value().Interface()
			if err := valuePrinter.Print(stream, vtp, unsafe.Pointer(&e), level+1); err != nil {
				return ierror.Raise(err)
			}
			if _, err := stream.Write(end); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}))
	SetPrinter(TypeGenericName[ipair.IPair[any, any]](), NewIPrinter(func(stream io.Writer, rtp reflect.Type, obj unsafe.Pointer, level int) error {
		p := reflect.NewAt(rtp, obj).Elem()

		firstPrinter, ftp := GetPrinterObj(p.Field(0).Interface())
		secondPrinter, stp := GetPrinterObj(p.Field(1).Interface())

		if _, err := stream.Write([]byte("(")); err != nil {
			return ierror.Raise(err)
		}
		e := p.Field(0).Interface()
		if err := firstPrinter.Print(stream, ftp, unsafe.Pointer(&e), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write([]byte(", ")); err != nil {
			return ierror.Raise(err)
		}
		e = p.Field(1).Interface()
		if err := secondPrinter.Print(stream, stp, unsafe.Pointer(&e), level+1); err != nil {
			return ierror.Raise(err)
		}
		if _, err := stream.Write([]byte(")")); err != nil {
			return ierror.Raise(err)
		}

		return nil
	}))
}
