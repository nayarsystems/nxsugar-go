package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxsugar-go"
)

func main() {
	// Server sets defaults for all services
	server, err := nxsugar.NewServerFromConfig()
	if err != nil {
		return
	}

	service1, err := server.AddService("service1")
	if err != nil {
		return
	}

	service2, err := server.AddService("service2")
	if err != nil {
		return
	}

	// A method that computes fibonacci on both services
	fib := func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		// Parse params
		v, err := ei.N(task.Params).M("v").Int()
		if err != nil {
			return nil, &nxsugar.JsonRpcErr{nxsugar.ErrInvalidParams, "", nil}
		}
		tout := ei.N(task.Params).M("t").Int64Z()

		// Do work
		if tout > 0 {
			time.Sleep(time.Duration(tout) * time.Second)
		}
		r := []int{}
		for i, j := 0, 1; j < v; i, j = i+j, i {
			r = append(r, i)
		}
		return r, nil
	}

	service1.AddMethod("fib", fib)
	service2.AddMethod("fib", fib)

	// Serve
	server.Serve()
}
