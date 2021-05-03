package core

import (
	"container/list"
	"sync"
)

type IClientPool struct {
	port        int
	compression int
	clients     list.List
	queue       list.List
	mu          sync.Mutex
}

type IClientBound struct {
	pool   *IClientPool
	client *IClient
}

func NewIClientPool(port, compression int) *IClientPool {
	return &IClientPool{
		port,
		compression,
		*list.New(),
		*list.New(),
		sync.Mutex{},
	}
}

func (this *IClientPool) Destroy() {
	this.mu.Lock()
	defer this.mu.Unlock()
	e := this.clients.Front()
	for e != nil {
		e.Value.(*IClient).close()
		e = e.Next()
	}
	this.queue.Init()
	this.clients.Init()
}

func (this *IClientPool) GetClient() (*IClientBound, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.queue.Len() == 0 {
		client, err := NewIClient(this.port, this.compression)
		if err != nil {
			return nil, err
		}
		this.clients.PushBack(client)
		return &IClientBound{this, client}, nil

	} else {
		e := this.queue.Front()
		this.queue.Remove(e)
		return &IClientBound{this, e.Value.(*IClient)}, nil
	}

}

func (this *IClientBound) Services() *IClient {
	return this.client
}

func (this *IClientBound) Free() {
	this.pool.mu.Lock()
	defer this.pool.mu.Unlock()
	this.pool.queue.PushBack(this.client)
	this.client = nil
}
