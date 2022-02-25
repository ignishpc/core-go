package core

import (
	"ignis/executor/api"
	. "ignis/executor/core/impi"
	"ignis/executor/core/storage"
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
	return nil
}

func Bcast[T any](this *IMpi, part storage.IPartition[T], root int) error {
	return nil
}

func DriverGather[T any](this *IMpi, group C_MPI_Comm, partGroup *storage.IPartitionGroup[T]) error {
	return nil
}

func DriverGather0[T any](this *IMpi, group C_MPI_Comm, partGroup *storage.IPartitionGroup[T]) error {
	return nil
}

func DriverScatter[T any](this *IMpi, group C_MPI_Comm, partGroup *storage.IPartitionGroup[T], partitions int) error {
	return nil
}

type MsgOpt struct {
	sameProtocol bool
	sameStorage  bool
}

func (this *IMpi) GetMsgOpt(group C_MPI_Comm, ptype string, send bool, other int, tag int) (*MsgOpt, error) {
	return &MsgOpt{}, nil
}

func SendGroup[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], dest int, tag int, opt *MsgOpt) error {
	return nil
}

func Send[T any](this *IMpi, part storage.IPartition[T], dest int, tag int) error {
	return nil
}

func RecvGroup[T any](this *IMpi, group C_MPI_Comm, part storage.IPartition[T], source int, tag int, opt *MsgOpt) error {
	return nil
}

func Recv[T any](this *IMpi, part storage.IPartition[T], source int, tag int) error {
	return nil
}

func SendRcv[T any](this *IMpi, sendp storage.IPartition[T], rcvp storage.IPartition[T], other int, tag int) error {
	return nil
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
