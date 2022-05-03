package modules

import (
	"context"
	"github.com/stretchr/testify/require"
	"ignis/executor/api/base"
	"ignis/executor/core"
	"os"
	"testing"
)

type IIOModuleTest struct {
	io           *IIOModule
	executorData *core.IExecutorData
}

var ioModuleTest *IIOModuleTest

func init() {
	executorData := core.NewIExecutorData()
	general := NewIIOModule(executorData)
	ioModuleTest = &IIOModuleTest{general, executorData}

	sepUpDefault(executorData)
	props := executorData.GetContext().Props()
	props["ignis.partition.minimal"] = "10MB"
	props["ignis.partition.type"] = "Memory"
	props["ignis.modules.io.overwrite"] = "true"
	props["ignis.modules.io.cores"] = "1"
	props["ignis.modules.io.compression"] = "0"
}

func TestTextFile1(t *testing.T) {
	textFileTest(ioModuleTest, t, 1, 1)
}

func TestTextFileN(t *testing.T) {
	textFileTest(ioModuleTest, t, 8, 2)
}

func TestPlainFile1(t *testing.T) {
	plainFileTest(ioModuleTest, t, 1, 1)
}

func TestPlainFileN(t *testing.T) {
	plainFileTest(ioModuleTest, t, 8, 2)
}

func TestSaveAsTextFile(t *testing.T) {
	saveAsTextFileTest(ioModuleTest, t, 8, 2)
}

func TestPartitionTextFile(t *testing.T) {
	partitionTextFileTest(ioModuleTest, t, 8)
}

func textFileTest(this *IIOModuleTest, t *testing.T, n int, cores int) {
	this.executorData.SetCores(cores)
	path := "./tmpfile.txt"
	executors := this.executorData.GetContext().Executors()
	file, err := os.Create(path)
	require.Nil(t, err)
	lines := (&IElemensString{}).create(10000, 0)

	for _, line := range lines {
		_, err := file.WriteString(line + "\n")
		require.Nil(t, err)
	}
	require.Nil(t, file.Close())

	require.Nil(t, this.io.TextFile2(context.Background(), path, int64(n)))

	require.GreaterOrEqual(t, this.executorData.GetPartitionsAny().Size(), n/executors)

	result := getFromPartitions[string](t, this.executorData)
	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[string](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[string](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, lines, result)
	}
}

func plainFileTest(this *IIOModuleTest, t *testing.T, n int, cores int) {
	this.executorData.SetCores(cores)
	path := "./plainfile.txt"
	executors := this.executorData.GetContext().Executors()
	file, err := os.Create(path)
	require.Nil(t, err)
	lines := (&IElemensString{}).create(10000, 0)

	for _, line := range lines {
		_, err := file.WriteString(line + "@")
		require.Nil(t, err)
	}
	require.Nil(t, file.Close())

	require.Nil(t, this.io.PlainFile3(context.Background(), path, int64(n), '@'))

	require.GreaterOrEqual(t, this.executorData.GetPartitionsAny().Size(), n/executors)

	result := getFromPartitions[string](t, this.executorData)
	loadToPartitions(t, this.executorData, result, 1)

	group, err := core.GetPartitions[string](this.executorData)
	require.Nil(t, err)
	require.Nil(t, core.Gather(this.executorData.Mpi(), group.Get(0), 0))

	result = getFromPartitions[string](t, this.executorData)

	if this.executorData.Mpi().IsRoot(0) {
		require.Equal(t, lines, result)
	}
}

func saveAsTextFileTest(this *IIOModuleTest, t *testing.T, n int, cores int) {
	this.executorData.SetCores(cores)
	path := "./tmpsave.txt"
	id := this.executorData.GetContext().ExecutorId()

	lines := (&IElemensString{}).create(10000, 0)

	loadToPartitions(t, this.executorData, lines, n)
	
	this.executorData.RegisterType(base.NewTypeC[string]())
	require.Nil(t, this.io.SaveAsTextFile(context.Background(), path, int64(cores*id)))
}

func partitionTextFileTest(this *IIOModuleTest, t *testing.T, n int) {
	this.executorData.SetCores(1)
	path := "./savetest.txt"
	rank := this.executorData.GetContext().ExecutorId()

	lines := (&IElemensString{}).create(10000, 0)

	loadToPartitions(t, this.executorData, lines, n)
	_ = os.RemoveAll(path)

	this.executorData.RegisterType(base.NewTypeC[string]())
	require.Nil(t, this.io.SaveAsTextFile(context.Background(), path, int64(rank*n)))
	require.Nil(t, this.io.PartitionTextFile(context.Background(), path, int64(rank*n), int64(n)))

	result := getFromPartitions[string](t, this.executorData)

	require.Equal(t, lines, result)
}
