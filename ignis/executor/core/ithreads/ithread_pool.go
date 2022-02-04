package ithreads

/*
#include <threads.h>
thread_local int thread_id = 0;
int getThreadId(){return thread_id;}
void setThreadId(int id){thread_id = id;}
*/
import "C"
import "math"

func GetThreadId() int {
	return int(C.getThreadId())
}

var pool_cores = 1

func GetPoolCores() int {
	return pool_cores
}

type IThreadPool struct {
}

func NewIThreadPool() *IThreadPool {
	return &IThreadPool{}
}

func staticWorker(input [][2]int, f func(int) error, results chan<- int) {
	for _, chunk := range input {
		for i := chunk[0]; i < chunk[1]; i++ {
			f(i)
		}
	}
	results <- 0
}

func dinamicWorker(input <-chan [2]int, f func(int) error, results chan<- int) {
	for chunk := range input {
		for i := chunk[0]; i < chunk[1]; i++ {
			f(i)
		}
	}
	results <- 0
}

func (this *IThreadPool) StaticRunChunk(start int, end int, chunk int, f func(int) error) {
	input := make([][][2]int, pool_cores)
	results := make(chan int, pool_cores)

	i := 0
	id := 0
	for {
		next := i + chunk
		if next > end {
			break
		}
		input[id] = append(input[id], [2]int{i, next})
		i = next
		id++
		id = id % pool_cores
	}

	for i := 0; i < pool_cores; i++ {
		go staticWorker(input[i], f, results)
	}

	for i := 0; i < pool_cores; i++ {
		<-results
	}
}

func (this *IThreadPool) DinamicRunChunk(start int, end int, chunk int, f func(int) error) {
	input := make(chan [2]int, pool_cores)
	results := make(chan int, pool_cores)

	for i := 0; i < pool_cores; i++ {
		go dinamicWorker(input, f, results)
	}

	i := 0
	id := 0
	for {
		next := i + chunk
		if next > end {
			break
		}
		input <- [2]int{i, next}
		i = next
		id++
		id = id % pool_cores
	}
	close(input)

	for i := 0; i < pool_cores; i++ {
		<-results
	}
}

func (this *IThreadPool) StaticRun(start int, end int, f func(int) error) {
	chunk := int(math.Ceil(float64(end-start) / float64(pool_cores)))
	this.StaticRunChunk(start, end, chunk, f)
}

func (this *IThreadPool) DinamicRun(start int, end int, f func(int) error) {
	this.DinamicRunChunk(start, end, 1, f)
}
