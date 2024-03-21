package api

import (
	"context"
	"ignis/driver/api/derror"
	"ignis/driver/core"
	"ignis/executor/core/ierror"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	this.cmd.Stdout = os.Stdout
	input, err := this.cmd.StdinPipe()
	if err != nil {
		this.cmd = nil
		return derror.NewGenericIDriverError(err)
	}
	_ = input
	err = this.cmd.Start()
	go this.cmd.Wait()
	if err != nil {
		this.cmd = nil
		return derror.NewGenericIDriverError(err)
	}
	var transport_cmp int
	job_sockets := os.Getenv("IGNIS_JOB_SOCKETS")
	if value, found := os.LookupEnv("IGNIS_TRANSPORT_COMPRESSION"); found {
		var err error
		transport_cmp, err = strconv.Atoi(value)
		if err != nil {
			return ierror.RaiseMsg("Executor arguments are not valid")
		}
	} else {
		transport_cmp = 0
	}

	this.callback, err = core.NewICallBack(filepath.Join(job_sockets, "driver.sock"), transport_cmp)
	if err != nil {
		this.Stop()
		return derror.NewGenericIDriverError(err)
	}
	this.pool = core.NewIClientPool(filepath.Join(job_sockets, "driver.sock"), transport_cmp)
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
