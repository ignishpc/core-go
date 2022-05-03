package core

import (
	"sync"
)

type IClientPool struct {
	port        int
	compression int
	clients     []*IClient
	queue       []*IClient
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
		make([]*IClient, 0),
		make([]*IClient, 0),
		sync.Mutex{},
	}
}

func (this *IClientPool) Destroy() {
	this.mu.Lock()
	defer this.mu.Unlock()
	for _, c := range this.clients {
		c.close()
	}
	this.clients = make([]*IClient, 0)
	this.queue = make([]*IClient, 0)
}

func (this *IClientPool) GetClient() (*IClientBound, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if len(this.queue) == 0 {
		client, err := NewIClient(this.port, this.compression)
		if err != nil {
			return nil, err
		}
		this.clients = append(this.clients, client)
		return &IClientBound{this, client}, nil

	} else {
		l := len(this.queue)
		c := this.queue[l-1]
		this.queue = this.queue[:l-1]
		return &IClientBound{this, c}, nil
	}

}

func (this *IClientBound) Services() *IClient {
	return this.client
}

func (this *IClientBound) Free() {
	this.pool.mu.Lock()
	defer this.pool.mu.Unlock()
	this.pool.queue = append(this.pool.queue, this.client)
	this.client = nil
}
