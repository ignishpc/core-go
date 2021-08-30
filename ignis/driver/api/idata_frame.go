package api

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/rpc/driver"
)

const (
	NO_CACHE   int8 = 0
	PRESERVE   int8 = 1
	MEMORY     int8 = 2
	RAW_MEMORY int8 = 3
	DISK       int8 = 4
)

type IDataFrame struct {
	worker *IWorker
	id     *driver.IDataFrameId
}

func (this *IDataFrame) SetName(name string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().SetName(context.Background(), this.id, name)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) Persist(cacheLevel int8) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().Persist(context.Background(), this.id, cacheLevel)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) Cache(cacheLevel int8) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().Cache(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) Unpersist(cacheLevel int8) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().Unpersist(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) Uncache(cacheLevel int8) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().Unpersist(context.Background(), this.id)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

/* TODO
func (this *IDataFrame) Repartition(numPartitions int64) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Repartition(context.Background(), this.id, numPartitions)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}*/

func (this *IDataFrame) Partitions() (int64, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return -1, err
	}
	defer client.Free()
	n, err2 := client.Services().GetDataframeService().Partitions(context.Background(), this.id)
	if err2 != nil {
		return -1, derror.NewGenericIDriverError(err2)
	}
	return n, nil
}

func (this *IDataFrame) SaveAsObjectFileDefault(path string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().SaveAsObjectFile(context.Background(), this.id, path, 6)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) SaveAsTextFile(path string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().SaveAsTextFile(context.Background(), this.id, path)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) SaveAsJsonFileDefault(path string) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().SaveAsJsonFile(context.Background(), this.id, path, true)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) SaveAsJsonFile(path string, pretty bool) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().SaveAsJsonFile(context.Background(), this.id, path, pretty)
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) Map(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Map_(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) Filter(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Filter(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) Flatmap(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Flatmap(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) KeyBy(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().KeyBy(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) MapPartitionsDefault(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapPartitions(context.Background(), this.id, src.rpc(), true)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) MapPartitions(src *ISource, preservesPartitioning bool) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapPartitions(context.Background(), this.id, src.rpc(), preservesPartitioning)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) MapPartitionsWithIndexDefault(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapPartitionsWithIndex(context.Background(), this.id, src.rpc(), true)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) MapPartitionsWithIndex(src *ISource, preservesPartitioning bool) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapPartitionsWithIndex(context.Background(), this.id, src.rpc(), preservesPartitioning)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) MapExecutor(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapExecutor(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) MapExecutorTo(src *ISource) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapExecutorTo(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) GroupByDefault(src *ISource, numPartitions int64) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().GroupBy(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) GroupBy(src *ISource, numPartitions int64) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().GroupBy2(context.Background(), this.id, src.rpc(), numPartitions)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) SortDefault(ascending bool) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Sort(context.Background(), this.id, ascending)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) Sort(ascending bool, numPartitions int64) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Sort2(context.Background(), this.id, ascending, numPartitions)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) SortByDefault(src *ISource, ascending bool) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().SortBy(context.Background(), this.id, src.rpc(), ascending)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) SortBy(src *ISource, ascending bool, numPartitions int64) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().SortBy3(context.Background(), this.id, src.rpc(), ascending, numPartitions)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) Reduce(src *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Reduce(context.Background(), this.id, src.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect1(id)
}

func (this *IDataFrame) TreeReduce(src *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().TreeReduce(context.Background(), this.id, src.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect1(id)
}

func (this *IDataFrame) Collect() (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Collect(context.Background(), this.id, NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Aggregate(zero, seqOp, combOp *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Aggregate(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect1(id)
}

func (this *IDataFrame) TreeAggregate(zero, seqOp, combOp *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().TreeAggregate(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect1(id)
}

func (this *IDataFrame) Fold(zero, src *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Fold(context.Background(), this.id, zero.rpc(), src.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect1(id)
}

func (this *IDataFrame) TreeFold(zero, src *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().TreeFold(context.Background(), this.id, zero.rpc(), src.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect1(id)
}

func (this *IDataFrame) Take(num int64) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Take(context.Background(), this.id, num, NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Foreach(src *ISource) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().Foreach_(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) ForeachPartition(src *ISource) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().ForeachPartition(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) ForeachExecutor(src *ISource) error {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return err
	}
	defer client.Free()
	err2 := client.Services().GetDataframeService().ForeachExecutor(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	return nil
}

func (this *IDataFrame) TopDefault(num int64) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Top(context.Background(), this.id, num, NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Top(num int64, cmp *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Top4(context.Background(), this.id, num, cmp.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) TakeOrderedDefault(num int64) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().TakeOrdered(context.Background(), this.id, num, NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) TakeOrdered(num int64, cmp *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().TakeOrdered4(context.Background(), this.id, num, cmp.rpc(), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Sample(withReplacement bool, fraction float64, seed int) (*IDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Sample(context.Background(), this.id, withReplacement, fraction, int32(seed))
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IDataFrame{
		this.worker,
		id,
	}, nil
}

func (this *IDataFrame) TakeSample(withReplacement bool, num int64, seed int) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().TakeSample(context.Background(), this.id, withReplacement, num, int32(seed), NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Count() (int64, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return -1, err
	}
	defer client.Free()
	n, err2 := client.Services().GetDataframeService().Count(context.Background(), this.id)
	if err2 != nil {
		return -1, derror.NewGenericIDriverError(err2)
	}
	return n, nil
}

func (this *IDataFrame) Max(cmp *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id int64
	var err2 error
	if cmp == nil {
		id, err2 = client.Services().GetDataframeService().Max3(context.Background(), this.id, cmp.rpc(), NewISource("").rpc())
	} else {
		id, err2 = client.Services().GetDataframeService().Max(context.Background(), this.id, NewISource("").rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Min(cmp *ISource) (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id int64
	var err2 error
	if cmp == nil {
		id, err2 = client.Services().GetDataframeService().Min3(context.Background(), this.id, cmp.rpc(), NewISource("").rpc())
	} else {
		id, err2 = client.Services().GetDataframeService().Min(context.Background(), this.id, NewISource("").rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) ToPair(cmp *ISource) *IPairDataFrame {
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			this.id,
		},
	}
}

type IPairDataFrame struct {
	IDataFrame
}

func (this *IPairDataFrame) FlatMapValues(src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().FlatMapValues(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) MapValues(src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().MapValues(context.Background(), this.id, src.rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) GroupByKeyDefault(src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if src == nil {
		id, err2 = client.Services().GetDataframeService().GroupByKey(context.Background(), this.id)
	} else {
		id, err2 = client.Services().GetDataframeService().GroupByKey2b(context.Background(), this.id, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) GroupByKey(src *ISource, numPartitions int64) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if src == nil {
		id, err2 = client.Services().GetDataframeService().GroupByKey2a(context.Background(), this.id, numPartitions)
	} else {
		id, err2 = client.Services().GetDataframeService().GroupByKey3(context.Background(), this.id, numPartitions, src.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) ReduceByKeyDefault(src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().ReduceByKey(context.Background(), this.id, src.rpc(), true)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) ReduceByKey(src *ISource, numPartitions int64, localReduce bool) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if src == nil {
		id, err2 = client.Services().GetDataframeService().ReduceByKey(context.Background(), this.id, src.rpc(), localReduce)
	} else {
		id, err2 = client.Services().GetDataframeService().ReduceByKey4(context.Background(), this.id, src.rpc(), numPartitions, localReduce)
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) AggregateByKeyDefault(zero, seqOp, combOp *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if combOp == nil {
		id, err2 = client.Services().GetDataframeService().AggregateByKey(context.Background(), this.id, zero.rpc(), seqOp.rpc())
	} else {
		id, err2 = client.Services().GetDataframeService().AggregateByKey4b(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc())
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) AggregateByKey(zero, seqOp, combOp *ISource, numPartitions int64) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if combOp == nil {
		id, err2 = client.Services().GetDataframeService().AggregateByKey4a(context.Background(), this.id, zero.rpc(), seqOp.rpc(), numPartitions)
	} else {
		id, err2 = client.Services().GetDataframeService().AggregateByKey5(context.Background(), this.id, zero.rpc(), seqOp.rpc(), combOp.rpc(), numPartitions)
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) FoldByKeyDefault(zero, src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().FoldByKey(context.Background(), this.id, zero.rpc(), src.rpc(), true)
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) FoldByKey(zero, src *ISource, numPartitions int64, localFold bool) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if src == nil {
		id, err2 = client.Services().GetDataframeService().FoldByKey(context.Background(), this.id, zero.rpc(), src.rpc(), localFold)
	} else {
		id, err2 = client.Services().GetDataframeService().FoldByKey5(context.Background(), this.id, zero.rpc(), src.rpc(), numPartitions, localFold)
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) SortByKeyDefault(ascending bool, src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if src == nil {
		id, err2 = client.Services().GetDataframeService().SortByKey(context.Background(), this.id, ascending)
	} else {
		id, err2 = client.Services().GetDataframeService().SortByKey3b(context.Background(), this.id, src.rpc(), ascending)
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IPairDataFrame) SortByKey(ascending bool, numPartitions int64, src *ISource) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	var id *driver.IDataFrameId
	var err2 error
	if src == nil {
		id, err2 = client.Services().GetDataframeService().SortByKey3a(context.Background(), this.id, ascending, numPartitions)
	} else {
		id, err2 = client.Services().GetDataframeService().SortByKey4(context.Background(), this.id, src.rpc(), ascending, numPartitions)
	}
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IDataFrame) Keys() (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Keys(context.Background(), this.id, NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) Values() (interface{}, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().Values(context.Background(), this.id, NewISource("").rpc())
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return Ignis.callback.DriverContext().Collect(id)
}

func (this *IDataFrame) SampleByKey(withReplacement bool, fraction map[interface{}]float64, seed int) (*IPairDataFrame, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	src := NewISource("").addParam("fraction", fraction)
	id, err2 := client.Services().GetDataframeService().SampleByKey(context.Background(), this.id, withReplacement, src.rpc(), int32(seed))
	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return &IPairDataFrame{
		IDataFrame{
			this.worker,
			id,
		},
	}, nil
}

func (this *IDataFrame) CountByKey() (map[interface{}]int64, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().CountByKey(context.Background(), this.id, NewISource("").rpc())
	obj, err := Ignis.callback.DriverContext().Collect(id)
	if err != nil {
		return nil, err
	}
	maps := obj.(map[interface{}]int64)
	var result map[interface{}]int64
	for key, value := range maps {
		if _, found := result[key]; found {
			result[key] += value
		} else {
			result[key] = value
		}
	}

	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return result, nil
}

func (this *IDataFrame) CountByValue() (map[interface{}]int64, error) {
	client, err := Ignis.pool.GetClient()
	if err != nil {
		return nil, err
	}
	defer client.Free()
	id, err2 := client.Services().GetDataframeService().CountByValue(context.Background(), this.id, NewISource("").rpc())
	obj, err := Ignis.callback.DriverContext().Collect(id)
	if err != nil {
		return nil, err
	}
	maps := obj.(map[interface{}]int64)
	var result map[interface{}]int64
	for key, value := range maps {
		if _, found := result[key]; found {
			result[key] += value
		} else {
			result[key] = value
		}
	}

	if err2 != nil {
		return nil, derror.NewGenericIDriverError(err2)
	}
	return result, nil
}
