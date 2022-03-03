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

type ThreadsInfo struct {
	static  bool
	threads int
	chunk   int
	before  func() error
	after   func() error
}

func New() *ThreadsInfo {
	return &ThreadsInfo{
		static:  true,
		threads: defaultCores,
		chunk:   0,
	}
}

func (this *ThreadsInfo) Static() *ThreadsInfo {
	this.static = true
	return this
}

func (this *ThreadsInfo) Dynamic() *ThreadsInfo {
	this.static = false
	return this
}

func (this *ThreadsInfo) Threads(n int) *ThreadsInfo {
	this.threads = n
	return this
}

func (this *ThreadsInfo) Chunk(n int) *ThreadsInfo {
	this.chunk = n
	return this
}

func (this *ThreadsInfo) Before(f func() error) *ThreadsInfo {
	this.before = f
	return this
}

func (this *ThreadsInfo) After(f func() error) *ThreadsInfo {
	this.after = f
	return this
}

func (this *ThreadsInfo) RunRange(start int, end int, f func(int, ISync) error) error {
	if this.chunk == 0 {
		this.chunk = int(math.Ceil(float64(end-start) / float64(this.threads)))
	}
	if this.static {
		return staticRun(this, start, end, f)
	} else {
		return dinamicRun(this, start, end, f)
	}
}

func (this *ThreadsInfo) RunN(n int, f func(int, ISync) error) error {
	return this.RunRange(0, n, f)
}

func (this *ThreadsInfo) Parallel(f func(int, ISync) error) error {
	this.static = true
	return this.RunRange(0, this.threads, f)
}

type ISync interface {
	Critical(f func() error) error
	Master(f func() error) error
	Barrier()
}

type iSyncImpl struct {
	mutex    sync.Mutex
	barriers [2]sync.WaitGroup
	bi       int
	party    int
}

func (this *iSyncImpl) Critical(f func() error) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return f()
}

func (this *iSyncImpl) Master(f func() error) error {
	if ThreadId() == 0 {
		return f()
	}
	return nil
}

func (this *iSyncImpl) Barrier() {
	bi2 := (this.bi + 1) % 2
	this.barriers[this.bi].Done()
	this.barriers[this.bi].Wait()
	this.bi = bi2
}

func (this *iSyncImpl) fail() {
	this.barriers[0].Add(-(this.party + 1))
	this.barriers[1].Add(-(this.party + 1))
}

func newISync(party int) ISync {
	s := &iSyncImpl{bi: 0, party: party}
	s.barriers[0].Add(party)
	s.barriers[1].Add(party)
	return s
}

func staticWorker(info *ThreadsInfo, id int, input [][2]int, sync ISync, f func(int, ISync) error, results chan<- error) {
	C.setThreadId(C.int(id))
	C.setThreads(C.int(info.threads))
	defer func() {
		if r := recover(); r != nil {
			results <- nil
		}
	}()

	if info.before != nil {
		if err := info.before(); err != nil {
			results <- err
			sync.(*iSyncImpl).fail()
			return
		}
	}

	for _, chunk := range input {
		for i := chunk[0]; i < chunk[1]; i++ {
			if err := f(i, sync); err != nil {
				results <- err
				sync.(*iSyncImpl).fail()
				return
			}
		}
	}

	if info.after != nil {
		if err := info.after(); err != nil {
			results <- err
			sync.(*iSyncImpl).fail()
			return
		}
	}

	results <- nil
}

func dinamicWorker(info *ThreadsInfo, id int, input <-chan [2]int, sync ISync, f func(int, ISync) error, results chan<- error) {
	C.setThreadId(C.int(id))
	C.setThreads(C.int(info.threads))
	defer func() {
		if r := recover(); r != nil {
			results <- nil
		}
	}()

	if info.before != nil {
		if err := info.before(); err != nil {
			results <- err
			sync.(*iSyncImpl).fail()
			return
		}
	}

	for chunk := range input {
		for i := chunk[0]; i < chunk[1]; i++ {
			if err := f(i, sync); err != nil {
				results <- err
				sync.(*iSyncImpl).fail()
				return
			}
		}
	}

	if info.after != nil {
		if err := info.after(); err != nil {
			results <- err
			sync.(*iSyncImpl).fail()
			return
		}
	}

	results <- nil
}

func staticRun(info *ThreadsInfo, start int, end int, f func(int, ISync) error) error {
	threads := info.threads
	input := make([][][2]int, threads)
	results := make(chan error, threads)

	i := 0
	id := 0
	for {
		next := i + info.chunk
		if next > end {
			break
		}
		input[id] = append(input[id], [2]int{i, next})
		i = next
		id++
		id = id % threads
	}

	sync := newISync(threads)
	for i := 1; i < threads; i++ {
		go staticWorker(info, i, input[i], sync, f, results)
	}
	staticWorker(info, 0, input[0], sync, f, results)

	var lastErr error
	for i := 0; i < threads; i++ {
		if err := <-results; err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func dinamicRun(info *ThreadsInfo, start int, end int, f func(int, ISync) error) error {
	threads := info.threads
	input := make(chan [2]int, threads)
	results := make(chan error, threads)

	sync := newISync(threads)
	for i := 1; i < threads; i++ {
		go dinamicWorker(info, i, input, sync, f, results)
	}

	i := 0
	id := 0
	for {
		next := i + info.chunk
		if next > end {
			break
		}
		input <- [2]int{i, next}
		i = next
		id++
		id = id % threads
	}
	dinamicWorker(info, 0, input, sync, f, results)

	close(input)

	var lastErr error
	for i := 0; i < threads; i++ {
		if err := <-results; err != nil {
			lastErr = err
		}
	}
	return lastErr
}
