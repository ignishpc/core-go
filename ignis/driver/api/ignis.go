package api

import (
	"context"
	"fmt"
	"ignis/driver/api/derror"
	"ignis/driver/core"
	"os"
	"os/exec"
	"sync"
)

type _Ignis struct {
	pool     *core.IClientPool
	callback *core.ICallBack
	cmd      *exec.Cmd
}

var mu sync.Mutex
var Ignis _Ignis

func (this *_Ignis) Start() error {
	mu.Lock()
	defer mu.Unlock()
	if this.cmd != nil {
		return nil
	}
	ctx := context.Background()
	this.cmd = exec.CommandContext(ctx, "ignis-backend")
	this.cmd.Stderr = os.Stderr
	input, err := this.cmd.StdinPipe()
	if err != nil {
		this.cmd = nil
		return derror.NewGenericIDriverError(err)
	}
	_ = input
	output, err := this.cmd.StdoutPipe()
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	err = this.cmd.Start()
	go this.cmd.Wait()
	if err != nil {
		this.cmd = nil
		return derror.NewGenericIDriverError(err)
	}
	var backendPort, backendCompression, callbackPort, callbackCompression int
	if _, err := fmt.Fscanf(output, "%d\n%d\n%d\n%d\n", &backendPort, &backendCompression, &callbackPort, &callbackCompression); err != nil {
		this.Stop()
		return derror.NewGenericIDriverError(err)
	}
	this.callback, err = core.NewICallBack(callbackPort, callbackCompression)
	if err != nil {
		this.Stop()
		return derror.NewGenericIDriverError(err)
	}
	this.pool = core.NewIClientPool(backendPort, backendCompression)
	return nil
}

func (this *_Ignis) Stop() {
	mu.Lock()
	defer mu.Unlock()
	if this.cmd != nil {
		client, err := this.pool.GetClient()
		if err != nil || client.Services().GetBackendService().Stop(context.Background()) != nil {
			_ = this.cmd.Process.Kill()
		}
		this.cmd = nil
		if this.pool != nil {
			this.pool.Destroy()
			this.pool = nil
		}
		if this.callback != nil {
			this.callback.Stop()
			this.callback = nil
		}
	}
}

func (this *_Ignis) driverContext() *core.IDriverContext {
	if this.callback == nil {
		panic("Ignis.start() must be invoked before the other routines")
	}
	return this.callback.DriverContext()
}

func (this *_Ignis) clientPool() *core.IClientPool {
	if this.pool == nil {
		panic("Ignis.start() must be invoked before the other routines")
	}
	return this.pool
}
