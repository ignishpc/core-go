package iio

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
	"ignis/executor/api/ipair"
	"ignis/executor/core/itransport"
	"ignis/executor/core/utils"
	"testing"
)

func testData[T any](t *testing.T, obj T, expected any) {
	t.Run(utils.TypeName[T](), func(t *testing.T) {
		buffer := itransport.NewIMemoryBuffer()
		proto := thrift.NewTCompactProtocolConf(buffer, &thrift.TConfiguration{})
		require.Nil(t, Write[T](proto, obj))
		require.Nil(t, Write[any](proto, obj))
		val, err := Read[any](proto)
		require.Nil(t, err)
		require.Equal(t, expected, val)
		val, err = Read[any](proto)
		require.Nil(t, err)
		require.Equal(t, expected, val)
	})
}

func TestIO(t *testing.T) {
	testData[int64](t, 32, int64(32))
	testData[string](t, "1", "1")
	s := "1"
	testData[*string](t, &s, s)
	ps := &s
	testData[**string](t, &ps, s)
	a := any(int64(1))
	testData[*any](t, &a, a)
	testData[[]int64](t, []int64{1}, []int64{1})
	testData[[]any](t, []any{}, []any{})
	testData[[]byte](t, []byte{0}, []byte{0})
	testData[map[string]int64](t, map[string]int64{"a": 1}, map[string]int64{"a": 1})
	testData[map[any]any](t, map[any]any{}, map[any]any{})
	testData[ipair.IPair[string, any]](t, ipair.IPair[string, any]{"1", int64(1)}, ipair.IPair[any, any]{"1", int64(1)})
	testData[[]ipair.IPair[string, any]](t, []ipair.IPair[string, any]{ipair.IPair[string, any]{"1", int64(1)}},
		[]ipair.IPair[any, any]{ipair.IPair[any, any]{"1", int64(1)}})
	SetIntAsDefault(true)
	testData[int](t, 32, 32)
	SetIntAsDefault(false)

	AddBasicType[int64]()
	testData[[]int64](t, []int64{1}, []int64{1})
	AddBasicType[ipair.IPair[int64, int64]]()
	p := ipair.IPair[int64, int64]{int64(1), int64(1)}
	testData[ipair.IPair[int64, int64]](t, p, p)
	testData[[]ipair.IPair[int64, int64]](t, []ipair.IPair[int64, int64]{p}, []ipair.IPair[int64, int64]{p})
	AddBasicType[ipair.IPair[int64, ipair.IPair[int64, int64]]]()
	pp := ipair.IPair[int64, ipair.IPair[int64, int64]]{1, p}
	testData[[]ipair.IPair[int64, ipair.IPair[int64, int64]]](t, []ipair.IPair[int64, ipair.IPair[int64, int64]]{pp}, []ipair.IPair[int64, ipair.IPair[int64, int64]]{pp})
	AddKeyType[string, string]()
	testData[map[string]string](t, map[string]string{"": ""}, map[string]string{"": ""})
}
