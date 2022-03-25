package core

import (
	"context"
	"ignis/executor/api"
	"ignis/executor/core/ierror"
	"ignis/executor/core/iio"
	. "ignis/executor/core/impi"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/itransport"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
	"unsafe"
)

type IMpi struct {
	propertyParser *IPropertyParser
	partitionTools *IPartitionTools
	context        api.IContext
}

func NewIMpi(propertyParser *IPropertyParser, partitionTools *IPartitionTools, context api.IContext) *IMpi {
	return &IMpi{
		propertyParser,
		partitionTools,
		context,
	}
}

func Gather[T any](this *IMpi, part storage.IPartition[T], root int) error {
	if this.Executors() == 1 {
		return nil
	}
	return gatherImpl[T](this, this.Native(), part, root, true)
}

func Bcast[T any](this *IMpi, part storage.IPartition[T], root int) error {
	if this.Executors() == 1 {
		return nil
	}
	if part.Type() == storage.IMemoryPartitionType {
		if list, ok := part.Inner().(*storage.IListImpl[T]); ok && iio.IsContiguous[T]() {
			sz := C_int(len(list.Array().([]T)))
			if err := MPI_Bcast(P(&sz), 1, MPI_INT, C_int(root), this.Native()); err != nil {
				return ierror.Raise(err)
			}
			if !this.IsRoot(root) {
				list.Resize(int(sz), true)
			}
			array := list.Array().([]T)
			bytes := C_int(int(sz) * int(iio.TypeObj[T]().Size()))
			if err := MPI_Bcast(PS(&array), bytes, MPI_BYTE, C_int(root), this.Native()); err != nil {
				return ierror.Raise(err)
			}
		} else {
			buffer := itransport.NewIMemoryBufferWithSize(part.Bytes())
			native, err := this.propertyParser.NativeSerialization()
			if err != nil {
				return ierror.Raise(err)
			}
			cmp, err := this.propertyParser.MsgCompression()
			if err != nil {
				return ierror.Raise(err)
			}
			var sz C_int
			if this.IsRoot(root) {
				if err = part.WriteWithNative(buffer, cmp, native); err != nil {
					return ierror.Raise(err)
				}
				sz = C_int(buffer.WriteEnd())
				buffer.ResetBuffer()
			}
			if err := MPI_Bcast(P(&sz), 1, MPI_INT, C_int(root), this.Native()); err != nil {
				return ierror.Raise(err)
			}
			ptr, err := buffer.GetWritePtr(int64(sz))
			if err != nil {
				return ierror.Raise(err)
			}
			if err := MPI_Bcast(P(&ptr[0]), sz, MPI_BYTE, C_int(root), this.Native()); err != nil {
				return ierror.Raise(err)
			}
			if !this.IsRoot(root) {
				if err = buffer.WroteBytes(int64(sz)); err != nil {
					return ierror.Raise(err)
				}
				if err = part.Clear(); err != nil {
					return ierror.Raise(err)
				}
				if err = part.Read(buffer); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return nil
	} else if part.Type() == storage.IRawMemoryPartitionType {
		return ierror.RaiseMsg("Not implemented yet")
	} else {
		return ierror.RaiseMsg("Not implemented yet")
	}
}

func DriverGather[T any](this *IMpi, group C_MPI_Comm, partGroup *storage.IPartitionGroup[T]) error {
	var driver bool
	var exec0 bool
	var groupSize int
	{
		var aux C_int
		if err := MPI_Comm_rank(group, &aux); err != nil {
			return ierror.Raise(err)
		}
		driver = aux == 0
		exec0 = aux == 1
		if err := MPI_Comm_size(group, &aux); err != nil {
			return ierror.Raise(err)
		}
		groupSize = int(aux)
	}
	maxPartition := 0
	var protocol C_int8
	var sameProtocol bool
	storageB := []byte{0}
	storageV := []byte{0}
	var storageLength C_int

	if driver {
		protocol = iprotocol.GO_PROTOCOL
	} else {
		maxPartition = partGroup.Size()
		if !partGroup.Empty() {
			storageB = []byte(partGroup.Get(0).Type() + ";")
		}
		storageLength = C_int(len(storageB))
	}

	if err := MPI_Allreduce(MPI_IN_PLACE, P(&maxPartition), 1, MPI_INT, MPI_MAX, group); err != nil {
		return ierror.Raise(err)
	}
	if maxPartition == 0 {
		return nil
	}
	if err := MPI_Allreduce(MPI_IN_PLACE, P(&storageLength), 1, MPI_INT, MPI_MAX, group); err != nil {
		return ierror.Raise(err)
	}
	if err := MPI_Bcast(P(&protocol), 1, MPI_BYTE, 0, group); err != nil {
		return ierror.Raise(err)
	}

	if len(storageB) < int(storageLength) {
		storageB = append(storageB, make([]byte, int(storageLength)-len(storageB))...)
	}
	if driver {
		storageV = make([]byte, int(storageLength)*groupSize)
	}

	if err := MPI_Gather(P(&storageB[0]), storageLength, MPI_BYTE, P(&storageV[0]), storageLength, MPI_BYTE, 0, group); err != nil {
		return ierror.Raise(err)
	}

	for i := 0; i < len(storageV); {
		if storageV[i] != 0 && driver {
			storageB = storageV[i : i+int(storageLength)]
			break
		}
		i += int(storageLength)
	}

	if err := MPI_Bcast(P(&storageB[0]), storageLength, MPI_BYTE, 0, group); err != nil {
		return ierror.Raise(err)
	}
	storageS := string(storageB[0 : len(storageB)-1])

	if driver {
		var aux C_int8
		if err := MPI_Recv(P(&aux), 1, MPI_C_BOOL, 1, 0, group, MPI_STATUS_IGNORE); err != nil {
			return ierror.Raise(err)
		}
		sameProtocol = int(aux) != 0
	} else {
		sameProtocol = int(protocol) == iprotocol.GO_PROTOCOL
		var aux C_int8 = C_int8(utils.Ternary(sameProtocol, 1, 0))
		if exec0 {
			if err := MPI_Send(P(&aux), 1, MPI_C_BOOL, 0, 0, group); err != nil {
				return ierror.Raise(err)
			}
		}
	}

	logger.Info("Comm: driverGather storage: ", storageB, ", operations: ", maxPartition)
	if storageS == storage.IDiskPartitionType && !sameProtocol {
		if !driver && partGroup.Size() > 0 && partGroup.Get(0).Native() {
			for i, p := range partGroup.Iter() {
				newP, err := NewPartitionDef[T](this.partitionTools)
				storage.DisableNative[T](newP)
				if err != nil {
					return ierror.Raise(err)
				}
				if err = p.CopyTo(newP); err != nil {
					return ierror.Raise(err)
				}
				partGroup.Set(i, newP)
			}
		}
	}

	if storageS == storage.IMemoryPartitionType && sameProtocol {
		var aux C_int8
		if exec0 {
			aux = C_int8(utils.Ternary(iio.TypeObj[T]() == iio.TypeObj[any](), 0, 1))
		}
		if err := MPI_Bcast(P(&aux), 1, MPI_C_BOOL, 1, group); err != nil {
			return ierror.Raise(err)
		}
		sameProtocol = aux == 1
	}

	var partSp storage.IPartition[T]
	for i := 0; i < maxPartition; i++ {
		if i < partGroup.Size() {
			partSp = partGroup.Get(i)
		} else if partSp == nil || !partSp.Empty() {
			var err error
			if partSp, err = NewPartitionWithName[T](this.partitionTools, storageS); err != nil {
				return ierror.Raise(err)
			}
			if driver {
				partGroup.Add(partSp)
			}
		}
		if err := gatherImpl(this, group, partSp, 0, sameProtocol); err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

func DriverGather0[T any](this *IMpi, group C_MPI_Comm, partGroup *storage.IPartitionGroup[T]) error {
	var rank C_int
	if err := MPI_Comm_rank(group, &rank); err != nil {
		return ierror.Raise(err)
	}
	var subGroup C_MPI_Comm
	if err := MPI_Comm_split(group, C_int(utils.Ternary(rank < 2, 1, 0)), rank, &subGroup); err != nil {
		return ierror.Raise(err)
	}
	if rank < 2 {
		if err := DriverGather[T](this, subGroup, partGroup); err != nil {
			return ierror.Raise(err)
		}
	}
	return ierror.Raise(MPI_Comm_free(&subGroup))
}

func DriverScatter[T any](this *IMpi, group C_MPI_Comm, partGroup *storage.IPartitionGroup[T], partitions int) error {
	var aux C_int
	if err := MPI_Comm_rank(group, &aux); err != nil {
		return ierror.Raise(err)
	}
	driver := aux == 0
	exec0 := aux == 1
	if err := MPI_Comm_size(group, &aux); err != nil {
		return ierror.Raise(err)
	}
	execs := int(aux) - 1
	var sameProtocol bool
	sz := int64(0)
	src := P_NIL()
	partsv := []C_int{}
	szv := []C_int{0}
	displs := []C_int{0}
	buffer := itransport.NewIMemoryBuffer()

	protocol := C_int8(iprotocol.GO_PROTOCOL)
	if err := MPI_Bcast(P(&protocol), 1, MPI_BYTE, 0, group); err != nil {
		return ierror.Raise(err)
	}

	if driver {
		var aux C_int8
		if err := MPI_Recv(P(&aux), 1, MPI_C_BOOL, 1, 0, group, MPI_STATUS_IGNORE); err != nil {
			return ierror.Raise(err)
		}
		sameProtocol = int(aux) != 0
	} else {
		sameProtocol = int(protocol) == iprotocol.GO_PROTOCOL
		var aux C_int8 = C_int8(utils.Ternary(sameProtocol, 1, 0))
		if exec0 {
			if err := MPI_Send(P(&aux), 1, MPI_C_BOOL, 0, 0, group); err != nil {
				return ierror.Raise(err)
			}
		}
	}

	if sameProtocol {
		var aux C_int8
		if exec0 {
			aux = C_int8(utils.Ternary(iio.TypeObj[T]() == iio.TypeObj[any](), 0, 1))
		}
		if err := MPI_Bcast(P(&aux), 1, MPI_C_BOOL, 1, group); err != nil {
			return ierror.Raise(err)
		}
		sameProtocol = aux == 1
	}

	logger.Info("Comm: driverScatter partitions: ", partitions)

	execsParts := partitions / execs
	if partitions%execs > 0 {
		execsParts++
	}

	if driver {
		sz = partGroup.Get(0).Size()

		execsElems := int(sz / int64(partitions))
		remainder := int(sz % int64(partitions))
		for i := 0; i < execsParts; i++ {
			partsv = append(partsv, 0)
		}
		for i := 0; i < remainder; i++ {
			partsv = append(partsv, C_int((execsElems+1)*int(iio.TypeObj[T]().Size())))
		}
		for i := 0; i < partitions-remainder; i++ {
			partsv = append(partsv, C_int(execsElems*int(iio.TypeObj[T]().Size())))
		}
		for i := 0; i < (len(partsv) - execsParts*(execs+1)); i++ {
			partsv = append(partsv, 0)
		}

		offset := C_int(0)
		if iio.IsContiguous[T]() && sameProtocol {
			for i := execsParts; i < len(partsv); i++ {
				offset += partsv[i]
				if (i+1)%execsParts == 0 {
					displs = append(displs, szv[len(szv)-1]+displs[len(displs)-1])
					szv = append(szv, offset-displs[len(displs)-1])
				}
			}
			array := partGroup.Get(0).Inner().(storage.IList).Array().([]T)
			src = PS(&array)
		} else {
			cmp, err := this.propertyParser.MsgCompression()
			if err != nil {
				return ierror.Raise(err)
			}
			native, err := this.propertyParser.NativeSerialization()
			if err != nil {
				return ierror.Raise(err)
			}
			zlib, err := itransport.NewIZlibTransportWithLevel(buffer, int(cmp))
			if err != nil {
				return ierror.Raise(err)
			}
			proto := iprotocol.NewIObjectProtocol(zlib)
			wrote := int64(0)
			array := partGroup.Get(0).Inner().(storage.IList).Array().([]T)
			for i := execsParts; i < len(partsv); i++ {
				displ := partsv[i] / C_int(iio.TypeObj[T]().Size())
				if err = proto.WriteObjectWithNative(array[offset:offset+displ], sameProtocol && native); err != nil {
					return ierror.Raise(err)
				}
				offset += C_int(displ)
				if err = zlib.Flush(context.Background()); err != nil {
					return ierror.Raise(err)
				}
				if err = zlib.Reset(); err != nil {
					return ierror.Raise(err)
				}
				partsv[i] = C_int(buffer.WriteEnd() - wrote)
				wrote += int64(partsv[i])
				if (i+1)%execsParts == 0 {
					displs = append(displs, szv[len(szv)-1]+displs[len(displs)-1])
					szv = append(szv, C_int(buffer.WriteEnd())-displs[len(displs)-1])
				}
			}
			src = P(&buffer.GetBuffer()[0])
		}
		sz = 0
	} else {
		partsv = make([]C_int, execsParts)
	}

	if err := MPI_Scatter(P(&partsv[0]), C_int(execsParts), MPI_INT, utils.Ternary(driver, MPI_IN_PLACE, P(&partsv[0])),
		C_int(execsParts), MPI_INT, 0, group); err != nil {
		return ierror.Raise(err)
	}

	if !driver {
		for _, l := range partsv {
			sz += int64(l)
		}

		prt, err := buffer.GetWritePtr(sz)
		if err != nil {
			return ierror.Raise(err)
		}
		src = P(&prt[0])
	}

	if err := MPI_Scatterv(src, &szv[0], &displs[0], MPI_BYTE, utils.Ternary(driver, MPI_IN_PLACE, src), C_int(sz),
		MPI_BYTE, 0, group); err != nil {
		return ierror.Raise(err)
	}

	if !driver {
		if iio.IsContiguous[T]() && sameProtocol {
			offset := 0
			for _, l := range partsv {
				if l == 0 {
					break
				}
				part, err := NewMemoryPartitionDef[T](this.partitionTools)
				if err != nil {
					return ierror.Raise(err)
				}
				list := part.Inner().(storage.IList)
				list.Resize(int(l)/int(iio.TypeObj[T]().Size()), true)
				array := list.Array().([]T)
				Memcpy(PS(&array), unsafe.Add(src, offset), int(l))
				offset += int(l)
				partGroup.Add(part)
			}
		} else {
			offset := 0
			for _, l := range partsv {
				if l == 0 {
					break
				}
				view := itransport.NewIMemoryBufferWrapper(buffer.GetBuffer()[offset:], int64(offset+int(l)), itransport.OBSERVE)
				part, err := NewPartitionDef[T](this.partitionTools)
				if err != nil {
					return ierror.Raise(err)
				}
				if err = part.Read(view); err != nil {
					return ierror.Raise(err)
				}
				offset += int(l)
				partGroup.Add(part)
			}
		}
	}
	return nil
}

type MsgOpt struct {
	sameProtocol bool
	sameStorage  bool
}

func (this *IMpi) GetMsgOpt(group C_MPI_Comm, ptype string, send bool, other int, tag int) (*MsgOpt, error) {
	var id C_int
	if err := MPI_Comm_rank(group, &id); err != nil {
		return nil, ierror.Raise(err)
	}
	source := utils.Ternary(send, int(id), other)
	dest := utils.Ternary(send, other, int(id))
	opt := &MsgOpt{}

	if int(id) == source {
		protocol := C_int8(iprotocol.GO_PROTOCOL)
		storage := []byte(ptype)
		length := C_int(len(storage))
		var aux C_int8
		if err := MPI_Send(P(&protocol), 1, MPI_BYTE, C_int(dest), C_int(tag), group); err != nil {
			return nil, ierror.Raise(err)
		}
		if err := MPI_Recv(P(&aux), 1, MPI_C_BOOL, C_int(dest), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
			return nil, ierror.Raise(err)
		}
		opt.sameProtocol = aux != 0
		if err := MPI_Send(P(&length), 1, MPI_INT, C_int(dest), C_int(tag), group); err != nil {
			return nil, ierror.Raise(err)
		}
		if err := MPI_Send(P(&storage[0]), length, MPI_BYTE, C_int(dest), C_int(tag), group); err != nil {
			return nil, ierror.Raise(err)
		}
		if err := MPI_Recv(P(&aux), 1, MPI_C_BOOL, C_int(dest), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
			return nil, ierror.Raise(err)
		}
		opt.sameStorage = aux != 0
	} else {
		var protocol C_int8
		var storage []byte
		var length C_int
		var aux C_int8
		if err := MPI_Recv(P(&protocol), 1, MPI_BYTE, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
			return nil, ierror.Raise(err)
		}
		opt.sameProtocol = protocol == iprotocol.GO_PROTOCOL
		aux = C_int8(utils.Ternary(opt.sameProtocol, 1, 0))
		if err := MPI_Send(P(&aux), 1, MPI_C_BOOL, C_int(source), C_int(tag), group); err != nil {
			return nil, ierror.Raise(err)
		}
		if err := MPI_Recv(P(&length), 1, MPI_INT, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
			return nil, ierror.Raise(err)
		}
		storage = make([]byte, int(length))
		if err := MPI_Recv(P(&storage[0]), length, MPI_BYTE, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
			return nil, ierror.Raise(err)
		}
		opt.sameStorage = ptype == string(storage)
		aux = C_int8(utils.Ternary(opt.sameStorage, 1, 0))
		if err := MPI_Send(P(&aux), 1, MPI_C_BOOL, C_int(source), C_int(tag), group); err != nil {
			return nil, ierror.Raise(err)
		}
	}

	return opt, nil
}

func SendGroup[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], dest int, tag int) error {
	opt, err := this.GetMsgOpt(group, part.Type(), true, dest, tag)
	if err != nil {
		return ierror.Raise(err)
	}
	var rank C_int
	if err := MPI_Comm_rank(group, &rank); err != nil {
		return ierror.Raise(err)
	}
	return sendRecvGroupImpl[T](this, group, part, int(rank), dest, tag, opt)
}

func SendGroupOpt[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], dest int, tag int, opt *MsgOpt) error {
	var rank C_int
	if err := MPI_Comm_rank(group, &rank); err != nil {
		return ierror.Raise(err)
	}
	return sendRecvGroupImpl[T](this, group, part, int(rank), dest, tag, opt)
}

func Send[T any](this *IMpi, part storage.IPartition[T], dest int, tag int) error {
	return sendRcv(this, part, this.Rank(), dest, tag)
}

func RecvGroup[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], source int, tag int) error {
	opt, err := this.GetMsgOpt(group, part.Type(), false, source, tag)
	if err != nil {
		return ierror.Raise(err)
	}
	var rank C_int
	if err := MPI_Comm_rank(group, &rank); err != nil {
		return ierror.Raise(err)
	}
	return sendRecvGroupImpl[T](this, group, part, source, int(rank), tag, opt)
}

func RecvGroupOpt[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], source int, tag int, opt *MsgOpt) error {
	var rank C_int
	if err := MPI_Comm_rank(group, &rank); err != nil {
		return ierror.Raise(err)
	}
	return sendRecvGroupImpl[T](this, group, part, source, int(rank), tag, opt)
}

func Recv[T any](this *IMpi, part storage.IPartition[T], source int, tag int) error {
	return sendRcv(this, part, source, this.Rank(), tag)
}

func SendRcv[T any](this *IMpi, sendp storage.IPartition[T], rcvp storage.IPartition[T], other int, tag int) error {
	//possible optimization, implement as sendRcv so that both processes serialize and deserialize at the same time.
	if this.Rank() > other {
		if err := Send(this, sendp, other, tag); err != nil {
			return ierror.Raise(err)
		}
		if err := Recv(this, rcvp, other, tag); err != nil {
			return ierror.Raise(err)
		}
	} else {
		if err := Recv(this, rcvp, other, tag); err != nil {
			return ierror.Raise(err)
		}
		if err := Send(this, sendp, other, tag); err != nil {
			return ierror.Raise(err)
		}
	}
	return nil
}

func gatherImpl[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], root int, sameProtocol bool) error {
	var rank int
	var executors int
	{
		var aux C_int
		if err := MPI_Comm_rank(group, &aux); err != nil {
			return ierror.Raise(err)
		}
		rank = int(aux)
		if err := MPI_Comm_size(group, &aux); err != nil {
			return ierror.Raise(err)
		}
		executors = int(aux)
	}
	if part.Type() == storage.IMemoryPartitionType {
		if list, ok := part.Inner().(*storage.IListImpl[T]); ok && iio.IsContiguous[T]() && sameProtocol {
			elemSz := int(iio.TypeObj[T]().Size())
			sz := C_int(int(part.Size()) * elemSz)
			szv := []C_int{0}
			displs := []C_int{0}
			if rank == root {
				szv = make([]C_int, executors)
			}
			if err := MPI_Gather(P(&sz), 1, MPI_INT, P(&szv[0]), 1, MPI_INT, C_int(root), group); err != nil {
				return ierror.Raise(err)
			}
			if rank == root {
				displs = this.displs(szv)
				list.Resize(int(displs[len(displs)-1])/elemSz, false)
				array := list.Array().([]T)
				//Use same buffer to rcv elements
				move[T](array, int(szv[rank])/elemSz, int(displs[rank])/elemSz)
				if err := MPI_Gatherv(MPI_IN_PLACE, C_int(sz), MPI_BYTE, PS(&array), &szv[0], &displs[0], MPI_BYTE, C_int(root), group); err != nil {
					return ierror.Raise(err)
				}
			} else {
				array := list.Array().([]T)
				if err := MPI_Gatherv(PS(&array), C_int(sz), MPI_BYTE, P_NIL(), nil, nil, MPI_BYTE, C_int(root), group); err != nil {
					return ierror.Raise(err)
				}
			}
		} else {
			buffer := itransport.NewIMemoryBuffer()
			sz := C_int(0)
			szv := []C_int{0}
			displs := []C_int{0}
			cmp, err := this.propertyParser.MsgCompression()
			if err != nil {
				return ierror.Raise(err)
			}
			native, err := this.propertyParser.NativeSerialization()
			if err != nil {
				return ierror.Raise(err)
			}
			if rank != root {
				if err = part.WriteWithNative(buffer, cmp, native); err != nil {
					return ierror.Raise(err)
				}
				sz = C_int(buffer.WriteEnd())
				buffer.ResetBuffer()
			}
			if rank == root {
				szv = make([]C_int, executors)
			}
			if err = MPI_Gather(P(&sz), 1, MPI_INT, P(&szv[0]), 1, MPI_INT, C_int(root), group); err != nil {
				return ierror.Raise(err)
			}
			if rank == root {
				displs = this.displs(szv)
				if _, err = buffer.GetWritePtr(int64(displs[len(displs)-1])); err != nil {
					return ierror.Raise(err)
				}
			}
			ptr, err := buffer.GetWritePtr(0)
			if err != nil {
				return ierror.Raise(err)
			}
			if err = MPI_Gatherv(P(&ptr[0]), C_int(sz), MPI_BYTE, P(&ptr[0]), &szv[0], &displs[0], MPI_BYTE, C_int(root), group); err != nil {
				return ierror.Raise(err)
			}
			if rank == root {
				rcv, err := NewMemoryPartitionDef[T](this.partitionTools)
				if err != nil {
					return ierror.Raise(err)
				}
				for i := 0; i < executors; i++ {
					if i != rank {
						view := itransport.NewIMemoryBufferWrapper(buffer.GetBuffer()[displs[i]:], int64(szv[i]), itransport.OBSERVE)
						if err = rcv.Read(view); err != nil {
							return ierror.Raise(err)
						}
					} else {
						//Avoid serialization own elements
						if err = part.MoveTo(rcv); err != nil {
							return ierror.Raise(err)
						}
					}
				}
				if err := part.Clear(); err != nil {
					return ierror.Raise(err)
				}
				if err := rcv.MoveTo(part); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return nil
	} else {
		return ierror.RaiseMsg("Not implemented yet")
	}
}

func sendRcv[T any](this *IMpi, part storage.IPartition[T], source int, dest int, tag int) error {
	return sendRecvImpl(this, this.Native(), part, source, dest, tag, true)
}

func sendRecvGroupImpl[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], source int, dest int, tag int, opt *MsgOpt) error {
	var id int
	{
		var aux C_int
		if err := MPI_Comm_rank(group, &aux); err != nil {
			return ierror.Raise(err)
		}
		id = int(aux)
	}
	if opt.sameProtocol && opt.sameStorage && part.Type() == storage.IMemoryPartitionType {
		_, isAny := part.Inner().(*storage.IListImpl[any])
		if int(id) == source {
			aux := C_int8(utils.Ternary(isAny, 1, 0))
			if err := MPI_Send(P(&aux), 1, MPI_C_BOOL, C_int(dest), C_int(tag), group); err != nil {
				return ierror.Raise(err)
			}
			if err := MPI_Recv(P(&aux), 1, MPI_C_BOOL, C_int(dest), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
				return ierror.Raise(err)
			}
			opt.sameStorage = aux != 0
		} else {
			var aux C_int8
			if err := MPI_Recv(P(&aux), 1, MPI_C_BOOL, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
				return ierror.Raise(err)
			}
			isAnyOther := aux != 0
			opt.sameStorage = isAnyOther == isAny
			aux = C_int8(utils.Ternary(opt.sameStorage, 1, 0))
			if err := MPI_Send(P(&aux), 1, MPI_C_BOOL, C_int(source), C_int(tag), group); err != nil {
				return ierror.Raise(err)
			}
		}
	}
	if opt.sameStorage {
		return sendRecvImpl(this, group, part, source, dest, tag, opt.sameStorage)
	} else {
		buffer := itransport.NewIMemoryBuffer()
		sz := C_int(0)
		if id == source {
			cmp, err := this.propertyParser.MsgCompression()
			if err != nil {
				return ierror.Raise(err)
			}
			native, err := this.propertyParser.NativeSerialization()
			if err != nil {
				return ierror.Raise(err)
			}
			if err = part.WriteWithNative(buffer, cmp, native); err != nil {
				return ierror.Raise(err)
			}
			sz = C_int(buffer.WriteEnd())
			buffer.ResetBuffer()
			if err = MPI_Send(P(&sz), 1, MPI_INT, C_int(dest), C_int(tag), group); err != nil {
				return ierror.Raise(err)
			}
			ptr, err := buffer.GetWritePtr(int64(sz))
			if err != nil {
				return ierror.Raise(err)
			}
			if err = MPI_Send(P(&ptr[0]), sz, MPI_BYTE, C_int(dest), C_int(tag), group); err != nil {
				return ierror.Raise(err)
			}
		} else {
			if err := MPI_Recv(P(&sz), 1, MPI_INT, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
				return ierror.Raise(err)
			}
			ptr, err := buffer.GetWritePtr(int64(sz))
			if err != nil {
				return ierror.Raise(err)
			}
			if err = MPI_Recv(P(&ptr[0]), sz, MPI_BYTE, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
				return ierror.Raise(err)
			}
			if err := buffer.WroteBytes(int64(sz)); err != nil {
				return ierror.Raise(err)
			}
			if err := part.Read(buffer); err != nil {
				return ierror.Raise(err)
			}
		}
		return nil
	}
}

func sendRecvImpl[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], source int, dest int, tag int, sameProtocol bool) error {
	var id int
	{
		var aux C_int
		if err := MPI_Comm_rank(group, &aux); err != nil {
			return ierror.Raise(err)
		}
		id = int(aux)
	}
	if part.Type() == storage.IMemoryPartitionType {
		if list, ok := part.Inner().(*storage.IListImpl[T]); ok && iio.IsContiguous[T]() && sameProtocol {
			sz := C_int(part.Size())
			elemSz := int(iio.TypeObj[T]().Size())
			if id == source {
				if err := MPI_Send(P(&sz), 1, MPI_INT, C_int(dest), C_int(tag), group); err != nil {
					return ierror.Raise(err)
				}
				array := list.Array().([]T)
				if err := MPI_Send(PS(&array), C_int(int(sz)*elemSz), MPI_BYTE, C_int(dest),
					C_int(tag), group); err != nil {
					return ierror.Raise(err)
				}
			} else {
				init := sz
				if err := MPI_Recv(P(&sz), 1, MPI_INT, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
					return ierror.Raise(err)
				}
				list.Resize(int(init)+int(sz), false)
				array := list.Array().([]T)
				if err := MPI_Recv(PS(&array), C_int(int(sz)*elemSz), MPI_BYTE, C_int(source), C_int(tag), group,
					MPI_STATUS_IGNORE); err != nil {
					return ierror.Raise(err)
				}
			}
		} else {
			buffer := itransport.NewIMemoryBuffer()
			sz := C_int(0)
			if id == source {
				cmp, err := this.propertyParser.MsgCompression()
				if err != nil {
					return ierror.Raise(err)
				}
				native, err := this.propertyParser.NativeSerialization()
				if err != nil {
					return ierror.Raise(err)
				}
				if err = part.WriteWithNative(buffer, cmp, native); err != nil {
					return ierror.Raise(err)
				}
				sz = C_int(buffer.WriteEnd())
				buffer.ResetBuffer()
				if err = MPI_Send(P(&sz), 1, MPI_INT, C_int(dest), C_int(tag), group); err != nil {
					return ierror.Raise(err)
				}
				ptr, err := buffer.GetWritePtr(int64(sz))
				if err != nil {
					return ierror.Raise(err)
				}
				if err = MPI_Send(P(&ptr[0]), sz, MPI_BYTE, C_int(dest), C_int(tag), group); err != nil {
					return ierror.Raise(err)
				}
			} else {
				if err := MPI_Recv(P(&sz), 1, MPI_INT, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
					return ierror.Raise(err)
				}
				ptr, err := buffer.GetWritePtr(int64(sz))
				if err != nil {
					return ierror.Raise(err)
				}
				if err = MPI_Recv(P(&ptr[0]), sz, MPI_BYTE, C_int(source), C_int(tag), group, MPI_STATUS_IGNORE); err != nil {
					return ierror.Raise(err)
				}
				if err := buffer.WroteBytes(int64(sz)); err != nil {
					return ierror.Raise(err)
				}
				if err := part.Read(buffer); err != nil {
					return ierror.Raise(err)
				}
			}
		}
		return nil
	} else {
		return ierror.RaiseMsg("Not implemented yet")
	}
}

func (this *IMpi) Barrier() error {
	return MPI_Barrier(this.Native())
}

func (this *IMpi) IsRoot(root int) bool {
	return this.Rank() == root
}

func (this *IMpi) Rank() int {
	var rank C_int
	MPI_Comm_rank(this.Native(), &rank)
	return int(rank)
}

func (this *IMpi) Executors() int {
	var n C_int
	MPI_Comm_size(this.Native(), &n)
	return int(n)
}

func (this *IMpi) Native() C_MPI_Comm {
	return this.context.MpiGroup()
}

func (this *IMpi) displs(sz []C_int) []C_int {
	d := make([]C_int, 0, len(sz)+1) //Last value is not used by mpi but is used as total size
	d = append(d, 0)
	for i := 0; i < len(sz); i++ {
		d = append(d, sz[i]+d[i])
	}
	return d
}

func move[T any](array []T, n int, displ int) {
	if displ > 0 {
		copy(array[displ:displ+n], array[0:n])
	}
}
