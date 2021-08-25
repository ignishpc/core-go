package core

type IWorkerFunc func() (interface{}, error)

type iWorkerTask struct {
	in  IWorkerFunc
	out chan IWorkerFunc
}

type IWorkerResult struct {
	lazy chan IWorkerFunc
}

func (this *IWorkerResult) get() (interface{}, error) {
	result := <-this.lazy
	return result()
}

type IWorkerPool struct {
	workers int
	queue   chan *iWorkerTask
}

func worker(queue chan *iWorkerTask) {
	for true {
		task := <-queue
		if task.in == nil {
			task.out <- nil
			break
		}
		result, err := task.in()
		task.out <- func() (interface{}, error) { return result, err }
	}
}

func NewIWorkerPool(workers int) *IWorkerPool {
	queue := make(chan *iWorkerTask)
	for i := 0; i < workers; i++ {
		go worker(queue)
	}
	return &IWorkerPool{
		workers: workers,
		queue:   queue,
	}
}

func (this *IWorkerPool) Destroy() {
	for i := 0; i < this.workers; i++ {
		out := make(chan IWorkerFunc)
		this.queue <- &iWorkerTask{
			nil, out,
		}
		_ = <-out
	}
}

func (this *IWorkerPool) Submit(task IWorkerFunc) *IWorkerResult {
	out := make(chan IWorkerFunc)
	this.queue <- &iWorkerTask{
		in:  task,
		out: out,
	}
	return &IWorkerResult{
		out,
	}
}
