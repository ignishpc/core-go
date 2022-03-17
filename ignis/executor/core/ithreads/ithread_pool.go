package ithreads

/*
#include <threads.h>
thread_local int thread_id = 0;
thread_local int threads = 1;
int getThreadId(){return thread_id;}
void setThreadId(int id){thread_id = id;}
int getThreads(){return threads;}
void setThreads(int n){threads = n;}
*/
import "C"
import (
	"errors"
	"fmt"
	"math"
	"sync"
)

func ThreadId() int {
	return int(C.getThreadId())
}

func Threads() int {
	return int(C.getThreads())
}

var defaultCores = 1

func DefaultCores() int {
	return defaultCores
}
func SetDefaultCores(n int) {
	defaultCores = n
}

type IRuntimeContext interface {
	Critical(f func() error) error
	Master(f func() error) error
	Barrier()
	For() IForBuilder
}

type iRuntimeContextImpl struct {
	threads  int
	data     chan [2]int
	error    chan error
	f        func(rctx IRuntimeContext) error
	mutex    sync.Mutex
	barriers [2]sync.WaitGroup
	ibarrier int
}

func Parallel(f func(rctx IRuntimeContext) error) error {
	return ParallelT(defaultCores, f)
}

func ParallelT(threads int, f func(rctx IRuntimeContext) error) (r error) {
	rctx := &iRuntimeContextImpl{
		threads:  threads,
		data:     nil,
		error:    make(chan error),
		f:        f,
		ibarrier: 0,
	}
	rctx.barriers[0].Add(threads)
	rctx.barriers[1].Add(threads)
	for i := 1; i < threads; i++ {
		go worker(i, rctx)
	}
	worker(0, rctx)

	for i := 0; i < threads; i++ {
		if err := <-rctx.error; err != nil {
			r = err
		}
	}
	return
}

func worker(id int, rctx *iRuntimeContextImpl) {
	C.setThreadId(C.int(id))
	C.setThreads(C.int(rctx.threads))
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				rctx.error <- err
			} else {
				rctx.error <- errors.New(fmt.Sprint(err))
			}
		}
	}()

	rctx.error <- rctx.f(rctx)
}

func (this *iRuntimeContextImpl) Critical(f func() error) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return f()
}

func (this *iRuntimeContextImpl) Master(f func() error) error {
	if ThreadId() == 0 {
		return f()
	}
	return nil
}

func (this *iRuntimeContextImpl) Barrier() {
	i := this.ibarrier
	i2 := (this.ibarrier + 1) % 2
	this.barriers[i].Done()
	this.barriers[i].Wait()
	this.ibarrier = i2
	if ThreadId() == 0 {
		this.barriers[i].Add(this.threads)
	}
}

func (this *iRuntimeContextImpl) fail() {
	this.barriers[0].Add(-(this.threads + 1))
	this.barriers[1].Add(-(this.threads + 1))
}

func (this *iRuntimeContextImpl) For() IForBuilder {
	return &iForBuilderImpl{
		rctx:    this,
		static:  true,
		threads: this.threads,
		chunk:   -1,
		start:   0,
	}
}

type IForBuilder interface {
	Static() IForBuilder
	Dynamic() IForBuilder
	Threads(n int) IForBuilder
	Chunk(n int) IForBuilder
	Start(n int) IForBuilder
	Run(n int, f func(i int) error) error
}

type iForBuilderImpl struct {
	rctx    *iRuntimeContextImpl
	static  bool
	threads int
	chunk   int
	start   int
}

func (this *iForBuilderImpl) Static() IForBuilder {
	this.static = true
	return this
}

func (this *iForBuilderImpl) Dynamic() IForBuilder {
	this.static = false
	return this
}

func (this *iForBuilderImpl) Threads(n int) IForBuilder {
	if n > 0 && n <= this.threads {
		this.threads = n
	}
	return this
}

func (this *iForBuilderImpl) Chunk(n int) IForBuilder {
	this.chunk = n
	return this
}

func (this *iForBuilderImpl) Start(n int) IForBuilder {
	this.start = n
	return this
}

func (this *iForBuilderImpl) Run(end int, f func(i int) error) error {
	if this.chunk == -1 {
		if this.static {
			this.chunk = int(math.Ceil(float64(end-this.start) / float64(this.threads)))
		} else {
			this.chunk = 1
		}
	}

	data := this.rctx.data
	threadId := ThreadId()
	if threadId == 0 || this.static {
		data = make(chan [2]int)
		if !this.static {
			this.rctx.data = data
		}

		i := this.start
		id := 0
		for {
			next := i + this.chunk
			if next > end {
				break
			}
			if !this.static || threadId == id {
				data <- [2]int{i, next}
			}
			i = next
			id++
			id = id % this.threads
		}
		close(data)
	}
	this.rctx.Barrier()
	data = this.rctx.data

	for chunk := range data {
		for i := chunk[0]; i < chunk[1]; i++ {
			if err := f(i); err != nil {
				this.rctx.fail()
				return err
			}
		}
	}
	this.rctx.Barrier()
	return nil
}
