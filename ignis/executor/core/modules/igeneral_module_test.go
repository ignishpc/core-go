package modules

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"ignis/executor/api"
	"ignis/executor/api/base"
	"ignis/executor/api/function"
	"ignis/executor/api/ipair"
	"ignis/executor/api/iterator"
	"ignis/executor/core"
	"ignis/executor/core/utils"
	"sort"
	"testing"
)

type IGeneralModuleTest struct {
	general      *IGeneralModule
	executorData *core.IExecutorData
}

var generalModuleTest *IGeneralModuleTest

func init() {
	executorData := core.NewIExecutorData()
	general := NewIGeneralModule(executorData)
	generalModuleTest = &IGeneralModuleTest{general, executorData}

	sepUpDefault(executorData)
	props := executorData.GetContext().Props()
	props["ignis.modules.sort.samples"] = "0.1"
	props["ignis.modules.sort.resampling"] = "False"
}

type IntSequence struct {
	function.IOnlyCall
	base.IExecuteTo[int]
}

func (this *IntSequence) Call(ctx api.IContext) ([][]int, error) {
	seq := make([]int, 100)
	for i := 0; i < 100; i++ {
		seq[i] = i
	}
	return [][]int{seq}, nil
}

func TestExecuteToInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&IntSequence{})
	executeToTest(generalModuleTest, t, "IntSequence", "Memory")
}

type MapInt struct {
	function.IOnlyCall
	base.IMap[int64, string]
}

func (this *MapInt) Call(e int64, ctx api.IContext) (string, error) {
	return fmt.Sprint(e), nil
}

func TestMapInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&MapInt{})
	mapTest[int64](generalModuleTest, t, "MapInt", 2, "Memory", &IElemensInt{})
}

type FilterInt struct {
	function.IOnlyCall
	base.IFilter[int64]
}

func (this *FilterInt) Call(e int64, ctx api.IContext) (bool, error) {
	return e%2 == 0, nil
}

func TestFilterInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&FilterInt{})
	filterTest[int64](generalModuleTest, t, "FilterInt", 2, "Memory", &IElemensInt{})
}

type FlatmapString struct {
	function.IOnlyCall
	base.IFlatmap[string, string]
}

func (this *FlatmapString) Call(e string, ctx api.IContext) ([]string, error) {
	return []string{e, e}, nil
}

func TestFlatmapString(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&FlatmapString{})
	flatmapTest[string](generalModuleTest, t, "FlatmapString", 2, "Memory", &IElemensString{})
}

type KeyByString struct {
	function.IOnlyCall
	base.IKeyBy[string, int]
}

func (this *KeyByString) Call(e string, ctx api.IContext) (int, error) {
	return len(e), nil
}

func TestKeyByStringInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&KeyByString{})
	keyByTest(generalModuleTest, t, "KeyByString", 2, "Memory")
}

type MapPartitionsInt struct {
	function.IOnlyCall
	base.IMapPartitions[int64, string]
}

func (this *MapPartitionsInt) Call(it iterator.IReadIterator[int64], ctx api.IContext) ([]string, error) {
	array := make([]string, 0)
	for it.HasNext() {
		if elem, err := it.Next(); err == nil {
			array = append(array, fmt.Sprint(elem))
		} else {
			return nil, err
		}
	}
	return array, nil
}

func TestMapPartitionsInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&MapPartitionsInt{})
	mapPartitionsTest[int64](generalModuleTest, t, "MapPartitionsInt", 2, "Memory", &IElemensInt{})
}

type MapPartitionWithIndexInt struct {
	function.IOnlyCall
	base.IMapPartitionsWithIndex[int64, string]
}

func (this *MapPartitionWithIndexInt) Call(p int64, it iterator.IReadIterator[int64], ctx api.IContext) ([]string, error) {
	array := make([]string, 0)
	for it.HasNext() {
		if elem, err := it.Next(); err == nil {
			array = append(array, fmt.Sprint(elem))
		} else {
			return nil, err
		}
	}
	return array, nil
}

func TestMapPartitionWithIndexInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&MapPartitionWithIndexInt{})
	mapPartitionsWithIndexTest[int64](generalModuleTest, t, "MapPartitionWithIndexInt", 2, "Memory", &IElemensInt{})
}

type MapExecutorInt struct {
	function.IOnlyCall
	base.IMapExecutor[int64]
}

func (this *MapExecutorInt) Call(parts [][]int64, ctx api.IContext) error {
	for _, part := range parts {
		for i, _ := range part {
			part[i]++
		}
	}
	return nil
}

func TestMapExecutorInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&MapExecutorInt{})
	mapExecutorTest[int64](generalModuleTest, t, "MapExecutorInt", "Memory", &IElemensInt{})
}

type MapExecutorToString struct {
	function.IOnlyCall
	base.IMapExecutorTo[int64, string]
}

func (this *MapExecutorToString) Call(parts [][]int64, ctx api.IContext) ([][]string, error) {
	array := make([][]string, len(parts))
	for i, part := range parts {
		for _, elem := range part {
			array[i] = append(array[i], fmt.Sprint(elem))
		}
	}
	return array, nil
}

func TestMapExecutorToString(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&MapExecutorToString{})
	mapExecutorToTest[int64](generalModuleTest, t, "MapExecutorToString", "Memory", &IElemensInt{})
}

type GroupByIntString struct {
	function.IOnlyCall
	base.IGroupBy[string, int64]
}

func (this *GroupByIntString) Call(e string, ctx api.IContext) (int64, error) {
	return int64(len(e)), nil
}

func TestGroupByIntString(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&GroupByIntString{})
	groupByTest(generalModuleTest, t, "GroupByIntString", 2, "Memory")
}

type SortInt struct {
	function.IOnlyCall
	base.ISortBy[int64]
}

func (this *SortInt) Call(a int64, b int64, ctx api.IContext) (bool, error) {
	return a < b, nil
}

func TestSortInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&SortInt{})
	sortTest[int64](generalModuleTest, t, "SortInt", 2, "Memory", false, &IElemensInt{})
}

type SortString struct {
	function.IOnlyCall
	base.ISortBy[string]
}

func (this *SortString) Call(a string, b string, ctx api.IContext) (bool, error) {
	return a < b, nil
}

func TestSortString(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&SortString{})
	sortTest[string](generalModuleTest, t, "SortString", 2, "Memory", false, &IElemensString{})
}

func TestResamplingSortInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&SortInt{})
	sortTest[int64](generalModuleTest, t, "SortInt", 2, "Memory", true, &IElemensInt{})
}

func TestDistinctInt(t *testing.T) {
	distinctTest[int64](generalModuleTest, t, 2, "Memory", &IElemensInt{})
}

func TestJoinStringInt(t *testing.T) {
	joinTest[string, int64](generalModuleTest, t, 2, "Memory", &IElemensPair[string, int64]{&IElemensString{}, &IElemensInt{}})
}

func TestUnionInt(t *testing.T) {
	unionTest[int64](generalModuleTest, t, 2, "Memory", true, &IElemensInt{})
}

func TestUnionUnorderedString(t *testing.T) {
	unionTest[int64](generalModuleTest, t, 2, "Memory", false, &IElemensInt{})
}

type FlatMapValuesInt struct {
	function.IOnlyCall
	base.IFlatMapValues[int64, int64, string]
}

func (this *FlatMapValuesInt) Call(e int64, ctx api.IContext) ([]string, error) {
	return []string{fmt.Sprint(e)}, nil
}

func TestFlatMapValuesInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&FlatMapValuesInt{})
	flatMapValuesTest[int64](generalModuleTest, t, "FlatMapValuesInt", 2, "Memory", &IElemensInt{})
}

type MapValuesInt struct {
	function.IOnlyCall
	base.IMapValues[int64, int64, string]
}

func (this *MapValuesInt) Call(e int64, ctx api.IContext) (string, error) {
	return fmt.Sprint(e), nil
}

func TestMapValuesInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&MapValuesInt{})
	mapValuesTest[int64](generalModuleTest, t, "MapValuesInt", 2, "Memory", &IElemensInt{})
}

func TestGroupByKeyIntString(t *testing.T) {
	groupByKeyTest(generalModuleTest, t, 2, "Memory")
}

type ReduceString struct {
	function.IOnlyCall
	base.IReduceByKey[int64, string]
}

func (this *ReduceString) Call(v1 string, v2 string, ctx api.IContext) (string, error) {
	return v1 + v2, nil
}

func TestReduceByKeyIntString(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&ReduceString{})
	reduceByKeyTest[int64, string](generalModuleTest, t, "ReduceString", 2, "Memory", &IElemensPair[int64, string]{&IElemensInt{}, &IElemensString{}})
}

type ZeroString struct {
	function.IOnlyCall
	base.IZero[string]
}

func (this *ZeroString) Call(ctx api.IContext) (string, error) {
	return "", nil
}

type ReduceIntToString struct {
	function.IOnlyCall
	base.IAggregateByKey[int64, string, int64]
}

func (this *ReduceIntToString) Call(v1 string, v2 int64, ctx api.IContext) (string, error) {
	return v1 + fmt.Sprint(v2), nil
}

func TestAggregateByKeyIntInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&ZeroString{})
	generalModuleTest.executorData.RegisterFunction(&ReduceIntToString{})
	generalModuleTest.executorData.RegisterFunction(&ReduceString{})
	aggregateByKeyTest[int64, int64](generalModuleTest, t, "ZeroString", "ReduceIntToString", "ReduceString", 2, "Memory", &IElemensPair[int64, int64]{&IElemensInt{}, &IElemensInt{}})
}

type ZeroInt struct {
	function.IOnlyCall
	base.IZero[int64]
}

func (this *ZeroInt) Call(ctx api.IContext) (int64, error) {
	return 0, nil
}

type ReduceInt struct {
	function.IOnlyCall
	base.IFoldByKey[int64, int64]
}

func (this *ReduceInt) Call(v1 int64, v2 int64, ctx api.IContext) (int64, error) {
	return v1 + v2, nil
}

func TestFoldByKeyIntInt(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&ZeroInt{})
	generalModuleTest.executorData.RegisterFunction(&ReduceInt{})
	foldByKeyTest[int64, int64](generalModuleTest, t, "ZeroInt", "ReduceInt", 2, "Memory", &IElemensPair[int64, int64]{&IElemensInt{}, &IElemensInt{}})
}

func TestSortByKeyIntString(t *testing.T) {
	sortByKeyTest[int64, string](generalModuleTest, t, 2, "Memory", &IElemensPair[int64, string]{&IElemensInt{}, &IElemensString{}})
}

/* TODO
func TestRepartitionOrderedInt(t *testing.T) {
	repartitionTest[int](generalModuleTest, t, 2, "Memory", true, true, &IElemensInt{})
}

func TestRepartitionUnorderedString(t *testing.T) {
	repartitionTest[string](generalModuleTest, t, 2, "Memory", false, true, &IElemensString{})
}

func TestRepartitionLocalInt(t *testing.T) {
	repartitionTest[int](generalModuleTest, t, 2, "Memory", false, false, &IElemensInt{})
}

type PartitionByStr struct {
	function.IOnlyCall
	base.IPartitionBy[string]
	hasher utils.Hasher
}

func (this *PartitionByStr) Call(e string, ctx api.IContext) (int64, error) {
	return int64(utils.Hash(e, this.hasher)), nil
}

func TestPartitionByString(t *testing.T) {
	generalModuleTest.executorData.RegisterFunction(&PartitionByStr{hasher: utils.GetHasher(reflect.TypeOf(""))})
	partitionByTest[string](generalModuleTest, t, "PartitionByStr", 2, "Memory", &IElemensString{})
}

func TestPartitionByRandomInt(t *testing.T) {
	partitionByRandomTest[int](generalModuleTest, t, 2, "Memory", &IElemensInt{})
}

func TestPartitionByHashString(t *testing.T) {
	partitionByHashTest[string](generalModuleTest, t, 2, "Memory", &IElemensString{})
}*/

/* Implementations */
func executeToTest(this *IGeneralModuleTest, t *testing.T, name string, partitionType string) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType

	require.Nil(t, this.general.ExecuteTo(nil, newSource(name)))
	result := getFromPartitions[int](t, this.executorData)

	require.Equal(t, 100, len(result))
	for i := 0; i < 100; i++ {
		require.Equal(t, i, result[i])
	}
}

func mapTest[T any](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores, 0)
	loadToPartitions[T](t, this.executorData, elems, cores*2)
	require.Nil(t, this.general.Map_(nil, newSource(name)))
	result := getFromPartitions[string](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, fmt.Sprint(elems[i]), result[i])
	}
}

func filterTest[T utils.Integer](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores, 0)
	loadToPartitions[T](t, this.executorData, elems, cores*2)
	require.Nil(t, this.general.Filter(nil, newSource(name)))
	result := getFromPartitions[T](t, this.executorData)

	j := 0
	for i := 0; i < len(elems); i++ {
		if elems[i]%2 == 0 {
			require.Equal(t, elems[i], result[j])
			j++
		}
	}
}

func flatmapTest[T any](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores, 0)
	loadToPartitions[T](t, this.executorData, elems, cores*2)
	require.Nil(t, this.general.Flatmap(nil, newSource(name)))
	result := getFromPartitions[string](t, this.executorData)

	require.Equal(t, len(elems)*2, len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, fmt.Sprint(elems[i]), result[2*i])
		require.Equal(t, fmt.Sprint(elems[i]), result[2*i+1])
	}
}

func keyByTest(this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := (&IElemensString{}).create(100*cores*2, 0)
	loadToPartitions(t, this.executorData, elems, cores*2)

	require.Nil(t, this.general.KeyBy(nil, newSource(name)))
	result := getFromPartitions[ipair.IPair[int, string]](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, len(elems[i]), result[i].First)
		require.Equal(t, elems[i], result[i].Second)
	}
}

func mapPartitionsTest[T any](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores*2, 0)
	loadToPartitions(t, this.executorData, elems, cores*2)

	require.Nil(t, this.general.MapPartitions(nil, newSource(name)))
	result := getFromPartitions[string](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, fmt.Sprint(elems[i]), result[i])
	}
}

func mapPartitionsWithIndexTest[T any](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores*2, 0)
	loadToPartitions(t, this.executorData, elems, cores*2)

	require.Nil(t, this.general.MapPartitionsWithIndex(nil, newSource(name), false))
	result := getFromPartitions[string](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, fmt.Sprint(elems[i]), result[i])
	}
}

func mapExecutorTest[T utils.Integer | utils.Float](this *IGeneralModuleTest, t *testing.T, name string, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	elems := gen.create(100*2*2, 0)
	loadToPartitions(t, this.executorData, elems, 5)

	require.Nil(t, this.general.MapExecutor(nil, newSource(name)))
	result := getFromPartitions[T](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, elems[i]+1, result[i])
	}
}

func mapExecutorToTest[T any](this *IGeneralModuleTest, t *testing.T, name string, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	elems := gen.create(100*2*2, 0)
	loadToPartitions(t, this.executorData, elems, 5)

	require.Nil(t, this.general.MapExecutorTo(nil, newSource(name)))
	result := getFromPartitions[string](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, fmt.Sprint(elems[i]), result[i])
	}
}

func groupByTest(this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(cores)
	elems := (&IElemensString{}).create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	this.executorData.RegisterType(base.NewTypeCA[int64, string]())
	require.Nil(t, this.general.GroupBy(nil, newSource(name), int64(cores*2)))
	result := getFromPartitions[ipair.IPair[int64, []string]](t, this.executorData)

	counts := make(map[int64]int64)

	for _, elem := range elems {
		counts[int64(len(elem))]++
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[ipair.IPair[int64, []string]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[ipair.IPair[int64, []string]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		for i := 0; i < len(result); i++ {
			require.Equal(t, counts[result[i].First], int64(len(result[i].Second)))
		}
	}
}

func sortTest[T utils.Ordered](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, rs bool, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.GetContext().Props()["ignis.modules.sort.resampling"] = utils.Ternary(rs, "true", "false")

	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(2)
	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	loadToPartitions(t, this.executorData, localElems, cores*2)
	require.Nil(t, this.general.SortBy(nil, newSource(name), true))
	result := getFromPartitions[T](t, this.executorData)

	for i := 1; i < len(result); i++ {
		require.GreaterOrEqual(t, result[i], result[i-1])
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems), len(result))
		sort.Slice(elems, func(i, j int) bool {
			return elems[i] < elems[j]
		})
		for i := 0; i < len(elems); i++ {
			require.GreaterOrEqual(t, elems[i], result[i])
		}
	}
}

func distinctTest[T comparable](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores*2, 0)
	loadToPartitions(t, this.executorData, elems, cores*2)
	this.executorData.RegisterType(base.NewTypeC[T]())
	require.Nil(t, this.general.Distinct(nil, 4))

	result := getFromPartitions[T](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		distinct := make(map[T]bool)
		for i := 0; i < len(elems); i++ {
			distinct[elems[i]] = true
		}
		require.Equal(t, len(distinct), len(result))

		rdist := make(map[T]bool)
		for i := 0; i < len(result); i++ {
			rdist[result[i]] = true
		}
		require.Equal(t, len(rdist), len(result))
		require.EqualValues(t, distinct, rdist)
	}

}

func joinTest[K comparable, V any](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, gen IElements[ipair.IPair[K, V]]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	np := this.executorData.GetContext().Executors()
	elems := gen.create(100*cores*2*np, 0)
	elems2 := gen.create(100*cores*2*np, 1)
	localElems := rankVector(this.executorData, elems)
	localElems2 := rankVector(this.executorData, elems2)

	loadToPartitions(t, this.executorData, localElems2, cores*2)
	core.SetVariable(this.executorData, "other", this.executorData.GetPartitionsAny())
	loadToPartitions(t, this.executorData, localElems, cores*2)

	this.executorData.RegisterType(base.NewTypeCA[K, V]())
	this.executorData.RegisterType(base.NewTypeCA[K, ipair.IPair[V, V]]())
	require.Nil(t, this.general.Join(nil, "other", int64(cores*2)))
	result := getFromPartitions[ipair.IPair[K, ipair.IPair[V, V]]](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)
	group, err := core.GetPartitions[ipair.IPair[K, ipair.IPair[V, V]]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))
	result = getFromPartitions[ipair.IPair[K, ipair.IPair[V, V]]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		m1 := make(map[K][]V)
		for _, entry := range elems {
			if _, present := m1[entry.First]; !present {
				m1[entry.First] = make([]V, 0)
			}
			m1[entry.First] = append(m1[entry.First], entry.Second)
		}

		expected := make([]ipair.IPair[K, ipair.IPair[V, V]], 0)
		for _, entry := range elems2 {
			if values, present := m1[entry.First]; present {
				for _, value := range values {
					expected = append(expected, *ipair.New(entry.First, *ipair.New(value, entry.Second)))
				}
			}
		}

		require.Equal(t, len(expected), len(result))
		require.ElementsMatch(t, expected, result)
	}
}

func unionTest[T any](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, preserveOrder bool, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	np := this.executorData.GetContext().Executors()
	elems := gen.create(100*cores*2*np, 0)
	elems2 := gen.create(100*cores*2*np, 1)
	localElems := rankVector(this.executorData, elems)
	localElems2 := rankVector(this.executorData, elems2)

	loadToPartitions(t, this.executorData, localElems2, cores*2)
	core.SetVariable(this.executorData, "other", this.executorData.GetPartitionsAny())
	loadToPartitions(t, this.executorData, localElems, cores*2)

	this.executorData.RegisterType(base.NewTypeA[T]())
	require.Nil(t, this.general.Union_(nil, "other", preserveOrder))
	result := getFromPartitions[T](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)
	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))
	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems)+len(elems2), len(result))

		expected := append(append(make([]T, 0), elems...), elems2...)

		if preserveOrder {
			require.Equal(t, expected, result)
		} else {
			require.ElementsMatch(t, expected, result)
		}
	}
}

func flatMapValuesTest[T any](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := (&IElemensPair[int64, T]{&IElemensInt{}, gen}).create(100*cores*2, 0)
	loadToPartitions(t, this.executorData, elems, cores*2)
	require.Nil(t, this.general.FlatMapValues(nil, newSource(name)))
	result := getFromPartitions[ipair.IPair[int64, string]](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, elems[i].First, result[i].First)
		require.Equal(t, fmt.Sprint(elems[i].Second), result[i].Second)
	}
}

func mapValuesTest[T any](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	elems := (&IElemensPair[int64, T]{&IElemensInt{}, gen}).create(100*cores*2, 0)
	loadToPartitions(t, this.executorData, elems, cores*2)
	require.Nil(t, this.general.MapValues(nil, newSource(name)))
	result := getFromPartitions[ipair.IPair[int64, string]](t, this.executorData)

	require.Equal(t, len(elems), len(result))
	for i := 0; i < len(elems); i++ {
		require.Equal(t, elems[i].First, result[i].First)
		require.Equal(t, fmt.Sprint(elems[i].Second), result[i].Second)
	}
}

func groupByKeyTest(this *IGeneralModuleTest, t *testing.T, cores int, partitionType string) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(cores)
	elems := (&IElemensPair[int64, string]{&IElemensInt{}, &IElemensString{}}).create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	this.executorData.RegisterType(base.NewTypeCC[int64, string]())
	require.Nil(t, this.general.GroupByKey(nil, int64(cores*2)))
	result := getFromPartitions[ipair.IPair[int64, []string]](t, this.executorData)

	counts := make(map[int64]int)

	for _, elem := range elems {
		counts[elem.First]++
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[ipair.IPair[int64, []string]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[ipair.IPair[int64, []string]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		for i := 0; i < len(result); i++ {
			require.Equal(t, counts[result[i].First], len(result[i].Second))
		}
	}
}

func reduceByKeyTest[K comparable, V utils.Ordered](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[ipair.IPair[K, V]]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	this.executorData.RegisterType(base.NewTypeCC[K, V]())
	require.Nil(t, this.general.ReduceByKey(nil, newSource(name), int64(cores*2), true))
	result := getFromPartitions[ipair.IPair[K, V]](t, this.executorData)

	acums := make(map[K]V)
	for _, elem := range elems {
		if _, present := acums[elem.First]; !present {
			acums[elem.First] = elem.Second
		} else {
			acums[elem.First] += elem.Second
		}
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[ipair.IPair[K, V]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[ipair.IPair[K, V]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		for i := 0; i < len(result); i++ {
			require.Equal(t, normalize(acums[result[i].First]), normalize(result[i].Second))
		}
	}
}

func aggregateByKeyTest[K comparable, V utils.Ordered](this *IGeneralModuleTest, t *testing.T, zero string, seq string, comb string, cores int, partitionType string, gen IElements[ipair.IPair[K, V]]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	require.Nil(t, this.general.AggregateByKey4(nil, newSource(zero), newSource(seq), newSource(comb), int64(cores*2)))
	result := getFromPartitions[ipair.IPair[K, string]](t, this.executorData)

	acums := make(map[K]string)
	for _, elem := range elems {
		if _, present := acums[elem.First]; !present {
			acums[elem.First] = ""
		} else {
			acums[elem.First] += fmt.Sprint(elem.Second)
		}
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[ipair.IPair[K, string]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[ipair.IPair[K, string]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		for i := 0; i < len(result); i++ {
			require.Equal(t, normalize(acums[result[i].First]), normalize(result[i].Second))
		}
	}
}

func foldByKeyTest[K comparable, V utils.Ordered](this *IGeneralModuleTest, t *testing.T, zero string, name string, cores int, partitionType string, gen IElements[ipair.IPair[K, V]]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(cores)
	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	require.Nil(t, this.general.FoldByKey(nil, newSource(zero), newSource(name), int64(cores*2), true))
	result := getFromPartitions[ipair.IPair[K, V]](t, this.executorData)

	acums := make(map[K]V)
	for _, elem := range elems {
		if _, present := acums[elem.First]; !present {
			acums[elem.First] = elem.Second
		} else {
			acums[elem.First] += elem.Second
		}
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[ipair.IPair[K, V]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[ipair.IPair[K, V]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		for i := 0; i < len(result); i++ {
			require.Equal(t, normalize(acums[result[i].First]), normalize(result[i].Second))
		}
	}
}

func sortByKeyTest[K utils.Ordered, V any](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, gen IElements[ipair.IPair[K, V]]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	np := this.executorData.GetContext().Executors()
	this.executorData.SetCores(cores)

	elems := gen.create(100*cores*2*np, 0)
	elemsKey := make([]K, 0)
	for _, elem := range elems {
		elemsKey = append(elemsKey, elem.First)
	}
	localElems := rankVector(this.executorData, elems)

	loadToPartitions(t, this.executorData, localElems, cores*2)
	this.executorData.RegisterType(base.NewTypeCA[K, V]())
	require.Nil(t, this.general.SortByKey(nil, true))
	result := getFromPartitions[ipair.IPair[K, V]](t, this.executorData)

	for i := 1; i < len(result); i++ {
		require.GreaterOrEqual(t, result[i].First, result[i-1].First)
	}

	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[ipair.IPair[K, V]](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[ipair.IPair[K, V]](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems), len(result))
		sort.Slice(elems, func(i, j int) bool {
			return elems[i].First < elems[j].First
		})
		for i := 0; i < len(elems); i++ {
			require.Equal(t, elems[i], result[i])
		}
	}
}

func repartitionTest[T comparable](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, preserveOrdering bool, global bool, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	np := this.executorData.GetContext().Executors()
	rank := this.executorData.Mpi().Rank()

	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	if rank == np-1 {
		localElems = append(localElems, localElems[:100]...)
	}
	elems = append(elems, elems[:100]...)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	require.Nil(t, this.general.Repartition(nil, int64(2+np+1), preserveOrdering, global))

	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	for _, part := range group.Iter() {
		require.Greater(t, 50, part.Size())
	}

	result := getFromPartitions[T](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)
	group, err = core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))
	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems), len(result))
		if preserveOrdering || !global {
			require.Equal(t, elems, result)
		} else {
			expected := make(map[T]int64)
			for _, e := range elems {
				expected[e]++
			}
			for _, e := range result {
				expected[e]--
			}
			for _, count := range expected {
				require.Equal(t, 0, count)
			}
		}
	}
}

func partitionByTest[T comparable](this *IGeneralModuleTest, t *testing.T, name string, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	np := this.executorData.GetContext().Executors()
	rank := this.executorData.Mpi().Rank()

	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	if rank == np-1 {
		localElems = append(localElems, localElems[:100]...)
	}
	elems = append(elems, elems[:100]...)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	require.Nil(t, this.general.PartitionBy(nil, newSource(name), int64(2+np+1)))

	result := getFromPartitions[T](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)
	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))
	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems), len(result))
		expected := make(map[T]int64)
		for _, e := range elems {
			expected[e]++
		}
		for _, e := range result {
			expected[e]--
		}
		for _, count := range expected {
			require.Equal(t, 0, count)
		}
	}
}

func partitionByRandomTest[T comparable](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	np := this.executorData.GetContext().Executors()
	rank := this.executorData.Mpi().Rank()

	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	if rank == np-1 {
		localElems = append(localElems, localElems[:100]...)
	}
	elems = append(elems, elems[:100]...)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	require.Nil(t, this.general.PartitionByRandom(nil, int64(2+np+1)))

	result := getFromPartitions[T](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)
	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))
	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems), len(result))
		expected := make(map[T]int64)
		for _, e := range elems {
			expected[e]++
		}
		for _, e := range result {
			expected[e]--
		}
		for _, count := range expected {
			require.Equal(t, 0, count)
		}
	}
}

func partitionByHashTest[T comparable](this *IGeneralModuleTest, t *testing.T, cores int, partitionType string, gen IElements[T]) {
	this.executorData.GetContext().Props()["ignis.partition.type"] = partitionType
	this.executorData.SetCores(cores)
	np := this.executorData.GetContext().Executors()
	rank := this.executorData.Mpi().Rank()

	elems := gen.create(100*cores*2*np, 0)
	localElems := rankVector(this.executorData, elems)
	if rank == np-1 {
		localElems = append(localElems, localElems[:100]...)
	}
	elems = append(elems, elems[:100]...)
	loadToPartitions(t, this.executorData, localElems, cores*2)

	require.Nil(t, this.general.PartitionByHash(nil, int64(2+np+1)))

	result := getFromPartitions[T](t, this.executorData)

	loadToPartitions(t, this.executorData, result, 1)
	group, err := core.GetPartitions[T](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))
	result = getFromPartitions[T](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, len(elems), len(result))
		expected := make(map[T]int64)
		for _, e := range elems {
			expected[e]++
		}
		for _, e := range result {
			expected[e]--
		}
		for _, count := range expected {
			require.Equal(t, 0, count)
		}
	}
}
