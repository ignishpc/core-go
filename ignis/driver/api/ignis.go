package api

import (
	"context"
	"fmt"
	"ignis/driver/api/derror"
	"ignis/driver/core"
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

func (this _Ignis) Start() error {
	mu.Lock()
	defer mu.Unlock()
	if this.cmd == nil {
		return nil
	}
	ctx := context.Background()
	this.cmd = exec.CommandContext(ctx, "ignis-backend")
	input, err := this.cmd.StdinPipe()
	if err != nil {
		return derror.NewGenericIDriverError(err)
	}
	_ = input
	output, err2 := this.cmd.StdoutPipe()
	if err2 != nil {
		return derror.NewGenericIDriverError(err2)
	}
	err3 := this.cmd.Start()
	if err3 != nil {
		return derror.NewGenericIDriverError(err3)
	}
	var backendPort, backendCompression, callbackPort, callbackCompression int
	fmt.Fscanf(output, "%d\n%d\n%d\n%d\n", &backendPort, &backendCompression, &callbackPort, &callbackCompression)
	this.callback, err = core.NewICallBack(callbackPort, callbackCompression)
	if err != nil {
		this.Stop()
		return derror.NewGenericIDriverError(err)
	}
	this.pool = core.NewIClientPool(backendPort, backendCompression)
	return nil
}

func (this _Ignis) Stop() {
	mu.Lock()
	defer mu.Unlock()
	if this.cmd != nil {
		client, err := this.pool.GetClient()
		if err == nil {
			client.Services().GetBackendService().Stop(context.Background())
			this.cmd.Wait()
		} else {
			this.cmd.Process.Kill()
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
