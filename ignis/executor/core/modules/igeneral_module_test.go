package modules

import (
	"github.com/apache/thrift/lib/go/thrift"
	"ignis/executor/core/itransport"
	"testing"
)

func test(t *testing.T) {
	buffer := thrift.NewTMemoryBuffer()
	zlib, err := itransport.NewIZlibTransport(buffer)
	if err != nil {
		panic(err)
	}

}
