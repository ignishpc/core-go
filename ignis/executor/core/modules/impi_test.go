package modules

import (
	"github.com/stretchr/testify/require"
	"ignis/executor/core"
	"ignis/executor/core/impi"
	"ignis/executor/core/modules/impl"
	"ignis/executor/core/storage"
	"math/rand"
	"strconv"
	"testing"
)

func MPI_init() {
	var aux impi.C_int
	if err := impi.MPI_Initialized(&aux); err != nil {
		panic(err)
	}
	if aux == 0 {
		if err := impi.MPI_Init_thread(nil, nil, impi.MPI_THREAD_MULTIPLE, nil); err != nil {
			panic(err)
		}
	}
	if err := impi.MPI_Comm_size(impi.MPI_COMM_WORLD, &aux); err != nil {
		panic(err)
	}
	skip = aux == 1
}

func init() {
	MPI_init()
	storage.CreateList[int64]()
	addMpiTest[int64](
		"IMpiInt64Test",
		func() storage.IPartition[int64] {
			return storage.NewIMemoryPartition[int64](100, false)
		},
		func(n int, seed int) []int64 {
			array := make([]int64, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	)
	addMpiTest[string](
		"IMpiStringTest",
		func() storage.IPartition[string] {
			return storage.NewIMemoryPartition[string](100, false)
		},
		func(n int, seed int) []string {
			array := make([]string, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = strconv.Itoa(int(rand.Int63() % int64(n)))
			}
			return array
		},
	)
	addMpiTest[any](
		"IMpiAnyTest",
		func() storage.IPartition[any] {
			return storage.NewIMemoryPartition[any](100, false)
		},
		func(n int, seed int) []any {
			array := make([]any, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	)
}

type IMpiTestAbs interface {
	GetName() string
	TestGather0(t *testing.T)
	TestGather1(t *testing.T)
	TestBcast(t *testing.T)
	TestSendRcv(t *testing.T)
	TestSendRcvGroupToMemory(t *testing.T)
	TestSendRcvGroupToRawMemory(t *testing.T)
	TestSendRcvGroupToDisk(t *testing.T)
	TestDriverGather(t *testing.T)
	TestDriverScatter(t *testing.T)
	TestSyncExchange(t *testing.T)
	TestSyncExchangeCores(t *testing.T)
	TestAsyncExchange(t *testing.T)
	TestAsyncExchangeCores(t *testing.T)
	TestAutoExchange(t *testing.T)
}

func addMpiTest[T any](name string, create func() storage.IPartition[T], elemens func(n int, seed int) []T) {
	test := &IMpiTest[T]{
		name,
		create,
		elemens,
		core.NewIExecutorData(),
	}
	props := test.executorData.GetContext().Props()
	props["ignis.transport.compression"] = "6"
	props["ignis.partition.compression"] = "6"
	props["ignis.partition.serialization"] = "ignis"
	props["ignis.partition.type"] = create().Type()

	mpiTestSuit = append(mpiTestSuit, test)
}

var mpiTestSuit = make([]IMpiTestAbs, 0)
var skip bool

func executeTest(t *testing.T, f func(IMpiTestAbs, *testing.T)) {
	if len(mpiTestSuit) == 0 || skip {
		t.Skip()
	}
	for _, test := range mpiTestSuit {
		t.Log(test.GetName())
		f(test, t)
	}
}

func TestGather0(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestGather0)
}

func TestGather1(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestGather1)
}

func TestBcast(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestBcast)
}

func TestSendRcv(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestSendRcv)
}

func TestSendRcvGroupToMemory(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestSendRcvGroupToMemory)
}

/* TODO
func TestSendRcvGroupToRawMemory(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestSendRcvGroupToRawMemory)
}

func TestSendRcvGroupToDisk(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestSendRcvGroupToDisk)
}*/

func TestDriverGather(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestDriverGather)
}

func TestDriverScatter(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestDriverScatter)
}

func TestSyncExchange(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestSyncExchange)
}

func TestSyncExchangeCores(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestSyncExchangeCores)
}

func TestAsyncExchange(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestAsyncExchange)
}

func TestAsyncExchangeCores(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestAsyncExchangeCores)
}

func TestAutoExchange(t *testing.T) {
	executeTest(t, (IMpiTestAbs).TestAutoExchange)
}

type IMpiTest[T any] struct {
	name         string
	create       func() storage.IPartition[T]
	elemens      func(n int, seed int) []T
	executorData *core.IExecutorData
}

func (this *IMpiTest[T]) GetName() string {
	return this.name
}

func (this *IMpiTest[T]) createWithName(t *testing.T, partitionType string) storage.IPartition[T] {
	part, err := core.NewPartitionWithName[T](this.executorData.GetPartitionTools(), partitionType)
	require.Nil(t, err)
	return part
}

func (this *IMpiTest[T]) TestGather0(t *testing.T) {
	this.gatherTest(t, 0)
}

func (this *IMpiTest[T]) TestGather1(t *testing.T) {
	this.gatherTest(t, 1)
}

func (this *IMpiTest[T]) TestBcast(t *testing.T) {
	n := 100
	rank := this.executorData.Mpi().Rank()
	elems := this.elemens(n, 0)
	part := this.create()
	if rank == 0 {
		this.insert(t, elems, part)
	} else {
		/*Ensures that the partition will be cleaned*/
		this.insert(t, []T{elems[len(elems)-1]}, part)
	}

	require.Nil(t, core.Bcast(this.executorData.Mpi(), part, 0))
	result := this.get(t, part)
	require.Equal(t, elems, result)
	require.Nil(t, this.executorData.Mpi().Barrier())
}

func (this *IMpiTest[T]) TestSendRcv(t *testing.T) {
	n := 100
	rank := this.executorData.Mpi().Rank()
	size := this.executorData.Mpi().Executors()
	elems := this.elemens(n*size, 0)
	part := this.create()
	if rank%2 == 0 {
		if rank+1 < size {
			require.Nil(t, core.Recv(this.executorData.Mpi(), part, rank+1, 0))
			result := this.get(t, part)
			require.Equal(t, elems, result)
		}
	} else {
		this.insert(t, elems, part)
		require.Nil(t, core.Send(this.executorData.Mpi(), part, rank-1, 0))
	}
	require.Nil(t, this.executorData.Mpi().Barrier())
}

func (this *IMpiTest[T]) TestSendRcvGroupToMemory(t *testing.T) {
	this.sendRcvGroupTest(t, "Memory")
}

func (this *IMpiTest[T]) TestSendRcvGroupToRawMemory(t *testing.T) {
	this.sendRcvGroupTest(t, "RawMemory")
}

func (this *IMpiTest[T]) TestSendRcvGroupToDisk(t *testing.T) {
	this.sendRcvGroupTest(t, "Disk")
}

func (this *IMpiTest[T]) TestDriverGather(t *testing.T) {
	n := 100
	driver := 0
	rank := this.executorData.Mpi().Rank()
	size := this.executorData.Mpi().Executors()
	elems := this.elemens(n*(size-1), 0)
	group := storage.NewIPartitionGroup[T]()
	if rank != driver {
		localElems := elems[n*(rank-1) : n*rank]
		part := this.create()
		this.insert(t, localElems, part)
		group.Add(part)
	}

	require.Nil(t, core.DriverGather(this.executorData.Mpi(), this.executorData.Mpi().Native(), group))

	if rank == driver {
		result := this.get(t, group.Get(0))
		require.Equal(t, elems, result)
	}
	require.Nil(t, this.executorData.Mpi().Barrier())
}

func (this *IMpiTest[T]) TestDriverScatter(t *testing.T) {
	n := 100
	driver := 0
	rank := this.executorData.Mpi().Rank()
	size := this.executorData.Mpi().Executors()
	elems := this.elemens(n*(size-1), 0)
	group := storage.NewIPartitionGroup[T]()
	if rank == driver {
		part := storage.NewIMemoryPartitionArray(elems) //Scatter is always from user memory array
		group.Add(part)
	}

	require.Nil(t, core.DriverScatter(this.executorData.Mpi(), this.executorData.Mpi().Native(), group, (size-1)*2))

	if rank != driver {
		localElems := elems[n*(rank-1) : n*rank]
		result := make([]T, 0)
		for _, part := range group.Iter() {
			result = append(result, this.get(t, part)...)
		}
		require.Equal(t, localElems, result)
	}
	require.Nil(t, this.executorData.Mpi().Barrier())
}

func (this *IMpiTest[T]) TestSyncExchange(t *testing.T) {
	this.executorData.GetContext().Props()["ignis.modules.exchange.type"] = "sync"
	this.executorData.GetContext().Props()["ignis.transport.cores"] = "0"
	//this.exchange(t)TODO
}

func (this *IMpiTest[T]) TestSyncExchangeCores(t *testing.T) {
	this.executorData.GetContext().Props()["ignis.modules.exchange.type"] = "sync"
	this.executorData.GetContext().Props()["ignis.transport.cores"] = "1"
	//this.exchange(t)TODO
}

func (this *IMpiTest[T]) TestAsyncExchange(t *testing.T) {
	this.executorData.GetContext().Props()["ignis.modules.exchange.type"] = "async"
	this.executorData.GetContext().Props()["ignis.transport.cores"] = "0"
	//this.exchange(t)TODO
}

func (this *IMpiTest[T]) TestAsyncExchangeCores(t *testing.T) {
	this.executorData.GetContext().Props()["ignis.modules.exchange.type"] = "async"
	this.executorData.GetContext().Props()["ignis.transport.cores"] = "1"
	//this.exchange(t)TODO
}

func (this *IMpiTest[T]) TestAutoExchange(t *testing.T) {
	this.executorData.GetContext().Props()["ignis.modules.exchange.type"] = "auto"
	this.executorData.GetContext().Props()["ignis.transport.cores"] = "0"
	//this.exchange(t)TODO
}

func (this *IMpiTest[T]) insert(t *testing.T, array []T, part storage.IPartition[T]) {
	writer, err := part.WriteIterator()
	require.Nil(t, err)
	for _, e := range array {
		require.Nil(t, writer.Write(e))
	}
}

func (this *IMpiTest[T]) get(t *testing.T, part storage.IPartition[T]) []T {
	reader, err := part.ReadIterator()
	require.Nil(t, err)
	array := make([]T, 0)
	for reader.HasNext() {
		elem, err := reader.Next()
		require.Nil(t, err)
		array = append(array, elem)
	}
	return array
}

func (this *IMpiTest[T]) gatherTest(t *testing.T, root int) {
	n := 100
	rank := this.executorData.Mpi().Rank()
	size := this.executorData.Mpi().Executors()
	elems := this.elemens(n*size, 0)
	localElems := elems[n*rank : n*(rank+1)]
	part := this.create()
	this.insert(t, localElems, part)
	require.Nil(t, core.Gather(this.executorData.Mpi(), part, root))
	if rank == root {
		result := this.get(t, part)
		require.Equal(t, elems, result)
	}
	require.Nil(t, this.executorData.Mpi().Barrier())
}

func (this *IMpiTest[T]) sendRcvGroupTest(t *testing.T, partitionType string) {
	n := 100
	rank := this.executorData.Mpi().Rank()
	size := this.executorData.Mpi().Executors()
	elems := this.elemens(n*size, 0)
	if rank%2 == 0 {
		if rank+1 < size {
			part := this.createWithName(t, partitionType)
			require.Nil(t, core.RecvGroup(this.executorData.Mpi(), this.executorData.Mpi().Native(), part, rank+1, 0))
			result := this.get(t, part)
			require.Equal(t, elems, result)
		}
	} else {
		part := this.create()
		this.insert(t, elems, part)
		require.Nil(t, core.SendGroup(this.executorData.Mpi(), this.executorData.Mpi().Native(), part, rank-1, 0))
	}
	require.Nil(t, this.executorData.Mpi().Barrier())
}

func (this *IMpiTest[T]) exchange(t *testing.T) {
	n := 100
	np := 4
	executors := this.executorData.Mpi().Executors()
	rank := this.executorData.Mpi().Rank()
	elems := this.elemens(n*executors*np, 0)
	parts := make([]storage.IPartition[T], 0)
	localParts := storage.NewIPartitionGroup[T]()

	for i := 0; i < executors*np; i++ {
		part := this.create()
		parts = append(parts, part)
		view := elems[n*i : n+(i+1)]
		this.insert(t, view, part)
		if i%executors == rank {
			other, err := part.Clone()
			require.Nil(t, err)
			localParts.Add(other.(storage.IPartition[T]))
		} else {
			localParts.Add(this.create())
		}
	}

	result := storage.NewIPartitionGroup[T]()
	refParts := parts[rank*np : (rank+1)*np]
	require.Nil(t, impl.Exchange(impl.NewIPipeImpl(this.executorData).Base(), localParts, result))

	for i := 0; i < np; i++ {
		a := this.get(t, result.Get(i))
		b := this.get(t, refParts[i])
		require.Equal(t, a, b)
	}

	require.Nil(t, this.executorData.Mpi().Barrier())
}
