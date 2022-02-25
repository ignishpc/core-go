package impl

import "C"
import (
	"context"
	"ignis/executor/api/ipair"
	"ignis/executor/core"
	"ignis/executor/core/ierror"
	"ignis/executor/core/impi"
	"ignis/executor/core/iprotocol"
	"ignis/executor/core/ithreads"
	"ignis/executor/core/itransport"
	"ignis/executor/core/logger"
	"ignis/executor/core/storage"
	"ignis/executor/core/utils"
)

type ICommImpl struct {
	IBaseImpl
	groups map[string]impi.C_MPI_Comm
}

func NewICommImpl(executorData *core.IExecutorData) *ICommImpl {
	return &ICommImpl{
		IBaseImpl: IBaseImpl{executorData},
	}
}

func (this *ICommImpl) OpenGroup() (r string, err error) {
	logger.Info("Comm: creating group")
	portName := make([]impi.C_char, impi.MPI_MAX_PORT_NAME)

	if err := impi.MPI_Open_port(impi.MPI_INFO_NULL, &portName[0]); err != nil {
		return "", ierror.Raise(err)
	}
	r = impi.C_ArrayToString(portName)
	core.SetVariable[string](this.executorData, "server", r)
	return
}

func (this *ICommImpl) CloseGroup() error {
	logger.Info("Comm: closing group server")
	if this.executorData.HasVariable("server") {
		portName := impi.C_ArrayFromString(core.GetVariable[string](this.executorData, "server"))
		this.executorData.RemoveVariable("server")
		return ierror.Raise(impi.MPI_Close_port(&portName[0]))
	}
	return nil
}

func (this *ICommImpl) JoinToGroup(id string, leader bool) error {
	comm, err := this.joinToGroupImpl(id, leader)
	if err != nil {
		return ierror.Raise(err)
	}
	this.executorData.SetMpiGroup(comm)
	return nil
}

func (this *ICommImpl) JoinToGroupName(id string, leader bool, name string) error {
	comm, err := this.joinToGroupImpl(id, leader)
	if err != nil {
		return ierror.Raise(err)
	}
	this.groups[name] = comm
	return nil
}

func (this *ICommImpl) HasGroup(name string) (bool, error) {
	_, ok := this.groups[name]
	return ok, nil
}

func (this *ICommImpl) DestroyGroup(name string) error {
	if len(name) == 0 {
		comm := this.executorData.Mpi().Native()
		if comm != impi.MPI_COMM_WORLD {
			return ierror.Raise(impi.MPI_Comm_free(&comm))
		}
		return nil

	} else if comm, ok := this.groups[name]; ok {
		delete(this.groups, name)
		return ierror.Raise(impi.MPI_Comm_free(&comm))
	}
	return nil
}

func (this *ICommImpl) DestroyGroups() (err error) {
	for name, comm := range this.groups {
		if err2 := impi.MPI_Comm_free(&comm); err2 != nil {
			err = err2
		}
		delete(this.groups, name)
	}
	if err2 := this.DestroyGroup(""); err2 != nil {
		err = err2
	}
	return
}

func (this *ICommImpl) GetProtocol() (int8, error) {
	return iprotocol.IGNIS_PROTOCOL, nil
}

func GetPartitions[T any](this *ICommImpl, protocol int8, minPartitions int64) ([][]byte, error) {
	var partitions [][]byte
	group, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return nil, ierror.Raise(err)
	}
	cmp, err := this.executorData.GetProperties().MsgCompression()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	native, err := this.executorData.GetProperties().NativeSerialization()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	aux, err := this.GetProtocol()
	if err != nil {
		return nil, ierror.Raise(err)
	}
	native = native && aux == protocol
	buffer := itransport.NewIMemoryBuffer()
	if int64(group.Size()) > minPartitions {
		for _, part := range group.Iter() {
			buffer.Reset()
			if err := part.WriteWithNative(buffer, cmp, native); err != nil {
				return nil, ierror.Raise(err)
			}
			partitions = append(partitions, buffer.Bytes())
		}
	} else if group.Size() == 1 && this.executorData.GetPartitionTools().IsMemoryGroup(group) {
		men := group.Get(0)
		list := men.Inner().(storage.IList)
		zlib, err := itransport.NewIZlibTransportWithLevel(buffer, int(cmp))
		if err != nil {
			return nil, ierror.Raise(err)
		}
		proto := iprotocol.NewIObjectProtocol(zlib)
		partition_elems := men.Size() / minPartitions
		remainder := men.Size() % minPartitions
		offset := 0
		for p := 0; p < group.Size(); p++ {
			sz := int(partition_elems)
			if int64(p) < remainder {
				sz++
			}
			if err := proto.WriteObjectWithNative(list.Array().([]T)[offset:offset+sz], native); err != nil {
				return nil, ierror.Raise(err)
			}
			offset += sz
			if err := zlib.Flush(context.Background()); err != nil {
				return nil, ierror.Raise(err)
			}
			if err := zlib.Reset(); err != nil {
				return nil, ierror.Raise(err)
			}
			partitions = append(partitions, buffer.Bytes())
			buffer.Reset()
		}
	} else if group.Size() > 0 {
		elemens := int64(0)
		for _, part := range group.Iter() {
			elemens += part.Size()
		}
		part := storage.NewIMemoryPartition[T](1024 * 1024)
		partition_elems := elemens / minPartitions
		remainder := elemens % minPartitions
		i := 0
		ew := int64(0)
		er := int64(0)
		it, err := group.Get(0).ReadIterator()
		if err != nil {
			return nil, ierror.Raise(err)
		}
		for p := 0; p < group.Size(); p++ {
			part.Clear()
			writer, err := part.WriteIterator()
			if err != nil {
				return nil, ierror.Raise(err)
			}
			ew = partition_elems
			if int64(p) < remainder {
				ew++
			}
			for ew > 0 {
				if er == 0 {
					er = group.Get(i).Size()
					it, err = group.Get(i).ReadIterator()
					if err != nil {
						return nil, ierror.Raise(err)
					}
					i++
				}
				var n int64
				if ew < er {
					n = ew
				} else {
					n = er
				}
				for j := int64(0); j < n; j++ {
					elem, err := it.Next()
					if err != nil {
						return nil, ierror.Raise(err)
					}
					if err := writer.Write(elem); err != nil {
						return nil, ierror.Raise(err)
					}
				}
				ew -= n
				er -= n
			}
			if err := part.WriteWithNative(buffer, cmp, native); err != nil {
				return nil, ierror.Raise(err)
			}
			partitions = append(partitions, buffer.Bytes())
			buffer.Reset()
		}
	} else {
		part := storage.NewIMemoryPartitionWithNative[T](1024*1024, native)
		if err := part.WriteWithNative(buffer, cmp, native); err != nil {
			return nil, ierror.Raise(err)
		}
		for i := int64(0); i < minPartitions; i++ {
			partitions = append(partitions, buffer.Bytes())
		}
	}

	return partitions, nil
}

func SetPartitions[T any](this *ICommImpl, partitions [][]byte) error {
	group, err := core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(), len(partitions))
	if err != nil {
		return ierror.Raise(err)
	}
	for i := 0; i < len(partitions); i++ {
		if err := group.Get(i).Read(itransport.NewIMemoryBufferBytes(partitions[i])); err != nil {
			return ierror.Raise(err)
		}
	}
	core.SetPartitions[T](this.executorData, group)
	return nil
}

func DriverGather[T any](this *ICommImpl, group string) error {
	comm, err := this.getGroup(group)
	if err != nil {
		return ierror.Raise(err)
	}
	if this.executorData.Mpi().Rank() == 0 {
		parts, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		core.SetPartitions[T](this.executorData, parts)
	}
	parts, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	return core.DriverGather[T](this.executorData.Mpi(), comm, parts)
}

func DriverGather0[T any](this *ICommImpl, group string) error {
	comm, err := this.getGroup(group)
	if err != nil {
		return ierror.Raise(err)
	}
	if this.executorData.Mpi().Rank() == 0 {
		parts, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		core.SetPartitions[T](this.executorData, parts)
	}
	parts, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	return core.DriverGather0[T](this.executorData.Mpi(), comm, parts)
}

func DriverScatter[T any](this *ICommImpl, group string, partitions int64) error {
	comm, err := this.getGroup(group)
	if err != nil {
		return ierror.Raise(err)
	}
	if this.executorData.Mpi().Rank() != 0 {
		parts, err := core.NewPartitionGroupDef[T](this.executorData.GetPartitionTools())
		if err != nil {
			return ierror.Raise(err)
		}
		core.SetPartitions[T](this.executorData, parts)
	}
	parts, err := core.GetPartitions[T](this.executorData)
	if err != nil {
		return ierror.Raise(err)
	}
	return core.DriverScatter[T](this.executorData.Mpi(), comm, parts, int(partitions))
}

func ImportData[T any](this *ICommImpl, group string, source bool, threads int64) error {
	import_comm, err := this.getGroup(group)
	if err != nil {
		return ierror.Raise(err)
	}
	var me int
	var executors int64
	{
		var aux impi.C_int
		impi.MPI_Comm_rank(import_comm, &aux)
		me = int(aux)
		impi.MPI_Comm_size(import_comm, &aux)
		executors = int64(aux)
	}
	ranges, queue, err := this.importDataAux(import_comm, source)
	if err != nil {
		return ierror.Raise(err)
	}
	offset := ranges[me].First
	var shared *storage.IPartitionGroup[T]
	if source {
		logger.Info("General: importData sending partitions")
		shared, err = core.GetAndDeletePartitions[T](this.executorData)
		if err != nil {
			return ierror.Raise(err)
		}
	} else {
		logger.Info("General: importData receiving partitions")
		shared, err = core.NewPartitionGroupWithSize[T](this.executorData.GetPartitionTools(),
			int(ranges[me].Second-ranges[me].First))
		if err != nil {
			return ierror.Raise(err)
		}
	}

	threads_comm, err := this.executorData.Duplicate(import_comm, int(threads))
	if err != nil {
		return ierror.Raise(err)
	}
	tp := shared.Get(0).Type()
	var opt *core.MsgOpt
	if err := ithreads.New().Parallel(func(id int, sync ithreads.ISync) error {
		comm := threads_comm[id]
		for i := 0; i < len(queue); i++ {
			other := queue[i]
			if other == executors {
				continue
			}
			init := utils.Min(ranges[me].First, ranges[other].First)
			end := utils.Min(ranges[me].Second, ranges[other].Second)
			if end-init < 1 {
				continue
			}
			if err = sync.Master(func() error {
				opt, err = this.executorData.Mpi().GetMsgOpt(comm, tp, source, int(other), 0)
				return err
			}); err != nil {
				return ierror.Raise(err)
			}
			sync.Barrier()
			for j := int64(0); j < end-init; j++ {
				if source {
					if err = core.SendGroup[T](this.executorData.Mpi(), comm, shared.Get(int(init-offset+j)), int(other), 0, opt); err != nil {
						return ierror.Raise(err)
					}
				} else {
					if err = core.RecvGroup[T](this.executorData.Mpi(), comm, shared.Get(int(init-offset+j)), int(other), 0, opt); err != nil {
						return ierror.Raise(err)
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}

	for i := 1; i < len(threads_comm); i++ {
		impi.MPI_Comm_free(&threads_comm[i])
	}

	if source {
		this.executorData.DeletePartitions()
	} else {
		core.SetPartitions[T](this.executorData, shared)
	}

	return nil
}

func (this *ICommImpl) importDataAux(group impi.C_MPI_Comm, source bool) ([]ipair.IPair[int64, int64], []int64, error) {
	rank := int64(0)
	localRank := int64(this.executorData.Mpi().Rank())
	executors := int64(0)
	{
		var aux impi.C_int
		impi.MPI_Comm_rank(group, &aux)
		rank = int64(aux)
		impi.MPI_Comm_size(group, &aux)
		executors = int64(aux)
	}
	localExecutors := int64(this.executorData.Mpi().Executors())
	remoteExecutors := executors - localExecutors
	localRoot := rank - localRank
	remoteRoot := utils.Ternary(localRoot == 0, localExecutors, 0)
	sourceRoot := utils.Ternary(source, localRoot, remoteRoot)
	targetExecutors := utils.Ternary(source, remoteExecutors, localExecutors)

	var cNumPartitions impi.C_int64
	if source {
		cNumPartitions = impi.C_int64(this.executorData.GetPartitionsAny().Size())
	}
	executorsCount := make([]impi.C_int64, executors)
	if err := impi.MPI_Allgather(impi.P(&cNumPartitions), 1, impi.MPI_LONG_LONG_INT, impi.P(&executorsCount[0]), 1, impi.MPI_LONG_DOUBLE_INT, group); err != nil {
		return nil, nil, ierror.Raise(err)
	}
	numPartitions := int64(0)
	for i := int64(0); i < executors; i++ {
		numPartitions += int64(executorsCount[i])
	}
	logger.Info("General: importData ", numPartitions, "partitions")
	var sourceRanges []ipair.IPair[int64, int64]
	offset := int64(0)
	var init int64
	end := sourceRoot + executors - targetExecutors

	for i := sourceRoot; i < end; i++ {
		sourceRanges = append(sourceRanges, ipair.IPair[int64, int64]{offset, offset + int64(executorsCount[i])})
		offset += int64(executorsCount[i])
	}

	block := numPartitions / targetExecutors
	remainder := numPartitions % targetExecutors
	var targetRanges []ipair.IPair[int64, int64]
	for i := int64(0); i < targetExecutors; i++ {
		if i < remainder {
			init = (block + 1) * i
			end = init + block + 1
		} else {
			init = (block+1)*remainder + block*(i-remainder)
			end = init + block
		}
		targetRanges = append(targetRanges, ipair.IPair[int64, int64]{init, end})
	}

	var ranges []ipair.IPair[int64, int64]
	if sourceRoot == 0 {
		for _, r := range sourceRanges {
			ranges = append(ranges, r)
		}
		for _, r := range targetRanges {
			ranges = append(ranges, r)
		}
	} else {
		for _, r := range targetRanges {
			ranges = append(ranges, r)
		}
		for _, r := range sourceRanges {
			ranges = append(ranges, r)
		}
	}

	var globalQueue []int64
	m := utils.Ternary(executors%2 == 0, executors, executors+1)
	id := int64(0)
	id2 := m*m - 2
	for i := int64(0); i < m-1; i++ {
		if rank == id%(m-1) {
			globalQueue = append(globalQueue, m-1)
		}
		if rank == m-1 {
			globalQueue = append(globalQueue, id%(m-1))
		}
		id++
		for j := int64(0); j < m/2; j++ {
			if rank == id%(m-1) {
				globalQueue = append(globalQueue, id2%(m-1))
			}
			if rank == id2%(m-1) {
				globalQueue = append(globalQueue, id%(m-1))
			}
			id++
			id2--
		}
	}

	var queue []int64
	for _, other := range globalQueue {
		if localRoot > 0 {
			if other < localRoot {
				queue = append(queue, other)
			}
		} else {
			if other >= remoteRoot {
				queue = append(queue, other)
			}
		}
	}

	return ranges, queue, nil
}

func (this *ICommImpl) joinToGroupImpl(id string, leader bool) (impi.C_MPI_Comm, error) {
	c_id := impi.C_ArrayFromString(id)
	root := this.executorData.HasVariable("server")
	comm := this.executorData.Mpi().Native()
	var comm1 impi.C_MPI_Comm
	var intercomm impi.C_MPI_Comm

	if leader {
		if root {
			if err := impi.MPI_Comm_accept(&c_id[0], impi.MPI_INFO_NULL, 0, comm, &intercomm); err != nil {
				return impi.MPI_COMM_NULL, ierror.Raise(err)
			}
		} else {
			if err := impi.MPI_Comm_accept(nil, impi.MPI_INFO_NULL, 0, comm, &intercomm); err != nil {
				return impi.MPI_COMM_NULL, ierror.Raise(err)
			}
		}
	} else {
		if err := impi.MPI_Comm_connect(&c_id[0], impi.MPI_INFO_NULL, 0, comm, &intercomm); err != nil {
			return impi.MPI_COMM_NULL, ierror.Raise(err)
		}
	}
	if leader {
		if err := impi.MPI_Intercomm_merge(intercomm, 0, &comm1); err != nil {
			return impi.MPI_COMM_NULL, ierror.Raise(err)
		}
	} else {
		if err := impi.MPI_Intercomm_merge(intercomm, 1, &comm1); err != nil {
			return impi.MPI_COMM_NULL, ierror.Raise(err)
		}
	}
	if err := impi.MPI_Comm_free(&intercomm); err != nil {
		return impi.MPI_COMM_NULL, ierror.Raise(err)
	}
	return comm1, nil
}

func (this *ICommImpl) getGroup(id string) (impi.C_MPI_Comm, error) {
	if comm, ok := this.groups[id]; ok {
		return comm, nil
	}
	return impi.MPI_COMM_NULL, ierror.RaiseMsg("Group " + id + " not found")
}
