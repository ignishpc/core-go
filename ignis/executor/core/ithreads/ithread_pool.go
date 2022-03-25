package ithreads

import "C"
import (
	"errors"
	"fmt"
	"ignis/executor/core/ierror"
	"math"
	"sync"
)

var defaultCores = 1

func DefaultCores() int {
	return defaultCores
}
func SetDefaultCores(n int) {
	defaultCores = n
}

type IRuntimeContext interface {
	Threads() int
	ThreadId() int
	Critical(f func() error) error
	Master(f func() error) error
	Barrier()
	For() IForBuilder
}

type cyclicBarrier struct {
	it      int
	count   int
	parties int
	cond    *sync.Cond
}

func (this *cyclicBarrier) await() {
	this.cond.L.Lock()
	defer this.cond.L.Unlock()

	this.count--
	if this.count == 0 {
		this.cond.Broadcast()
		this.it++
		this.count = this.parties
	} else {
		it := this.it
		for it == this.it {
			this.cond.Wait()
		}
	}
}

type iRuntimeContextData struct {
	threads int
	data    chan [2]int
	loop    bool
	error   chan error
	f       func(rctx IRuntimeContext) error
	mutex   sync.Mutex
	barrier cyclicBarrier
}

type iRuntimeContextImpl struct {
	*iRuntimeContextData
	threadId int
}

func Parallel(f func(rctx IRuntimeContext) error) error {
	return ParallelT(defaultCores, f)
}

func ParallelT(threads int, f func(rctx IRuntimeContext) error) (r error) {
	rctx := &iRuntimeContextData{
		threads: threads,
		data:    nil,
		loop:    false,
		error:   make(chan error, threads),
		f:       f,
		barrier: cyclicBarrier{
			it:      0,
			count:   threads,
			parties: threads,
		},
	}
	rctx.barrier.cond = sync.NewCond(&rctx.mutex)
	for i := 1; i < threads; i++ {
		go worker(&iRuntimeContextImpl{rctx, i})
	}
	worker(&iRuntimeContextImpl{rctx, 0})

	for i := 0; i < threads; i++ {
		if err := <-rctx.error; err != nil {
			r = err
		}
	}
	return
}

func worker(rctx *iRuntimeContextImpl) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				rctx.error <- err
			} else {
				rctx.error <- errors.New(fmt.Sprint(r))
			}
		}
	}()

	rctx.error <- rctx.f(rctx)
}

func (this *iRuntimeContextImpl) Threads() int {
	return this.threads
}

func (this *iRuntimeContextImpl) ThreadId() int {
	return this.threadId
}

func (this *iRuntimeContextImpl) Critical(f func() error) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return f()
}

func (this *iRuntimeContextImpl) Master(f func() error) error {
	if this.ThreadId() == 0 {
		return f()
	}
	return nil
}

func (this *iRuntimeContextImpl) Barrier() {
	this.barrier.await()
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

func (this *iForBuilderImpl) Run(end int, f func(i int) error) (_err error) {
	if this.rctx.loop {
		return ierror.RaiseMsg("parallel loop in parallel loop error")
	}
	if this.chunk == -1 {
		if this.static {
			this.chunk = int(math.Ceil(float64(end-this.start) / float64(this.threads)))
		} else {
			this.chunk = 1
		}
	}

	data := this.rctx.data
	threadId := this.rctx.ThreadId()
	if threadId == 0 || this.static {
		data = make(chan [2]int, (end-this.start)/this.chunk+1)
		if !this.static {
			this.rctx.data = data
		}

		i := this.start
		id := 0
		for {
			next := i + this.chunk
			if next > end {
				if i < end {
					next = end
				} else {
					break
				}
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
	this.rctx.loop = true
	if !this.static {
		data = this.rctx.data
	}

	for chunk := range data {
		for i := chunk[0]; i < chunk[1]; i++ {
			if err := f(i); err != nil {
				this.rctx.Barrier()
				this.rctx.loop = false
				return err
			}
		}
	}
	this.rctx.Barrier()
	this.rctx.loop = false
	return nil
}
