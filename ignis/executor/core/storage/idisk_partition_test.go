package storage

import (
	"github.com/stretchr/testify/require"
	"ignis/executor/core/iio"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

var ID = 0

func init() {
	iio.AddBasicType[int64]()
	addPartitionTest(&IPartitionTest[int64]{
		"IDiskPartitionInt64Test",
		func() IPartition[int64] {
			part, err := NewIDiskPartition[int64]("/tmp/diskpartitionTest"+strconv.Itoa(ID), 6, false, false, false)
			ID++
			if err != nil {
				panic(err)
			}
			return part
		},
		func(n int, seed int) []int64 {
			array := make([]int64, n)
			rand.Seed(int64(seed))
			for i := 0; i < n; i++ {
				array[i] = rand.Int63() % int64(n)
			}
			return array
		},
	})
}

type IDiskPartitionTestAbs interface {
	TestPersist(t *testing.T)
	TestRename(t *testing.T)
}

func disktest(p IPartitionTestAbs) bool {
	return strings.HasPrefix(p.GetName(), "IDisk")
}

func TestRename(t *testing.T) {
	executeTest(t, func(p IPartitionTestAbs, t *testing.T) {
		if part, ok := p.(IDiskPartitionTestAbs); ok && disktest(p) {
			part.TestRename(t)
		}
	})
}

func TestPersist(t *testing.T) {
	executeTest(t, func(p IPartitionTestAbs, t *testing.T) {
		if part, ok := p.(IDiskPartitionTestAbs); ok && disktest(p) {
			part.TestPersist(t)
		}
	})
}

func (this *IPartitionTest[T]) TestRename(t *testing.T) {
	part := this.create().(*IDiskPartition[T])
	elems := this.elemens(100, 0)
	this.writeIterator(t, elems, part)
	path := part.GetPath()
	newPath := path + "-TestRename"
	_ = os.Remove(newPath)
	require.Nil(t, part.Rename(newPath))
	_, err := os.Stat(path)
	require.NotNil(t, err)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}

func (this *IPartitionTest[T]) TestPersist(t *testing.T) {
	part := this.create().(*IDiskPartition[T])
	elems := this.elemens(100, 0)
	this.writeIterator(t, elems, part)
	part.Persist(true)
	require.Nil(t, part.Sync())
	path := part.GetPath()
	part = nil
	runtime.GC()
	runtime.GC()
	_, err := os.Stat(path)
	require.Nil(t, err)
	part, err = NewIDiskPartition[T](path, 6, false, false, true)
	require.Nil(t, err)
	require.Equal(t, len(elems), int(part.Size()))
	result := this.readIterator(t, part)
	require.Equal(t, elems, result)
}
