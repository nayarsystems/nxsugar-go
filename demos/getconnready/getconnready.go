package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxsugar-go"
)

func main() {
	// Service
	s := nxsugar.NewService("root:root@localhost", "test.nxsugar.getconnready", &nxsugar.ServiceOpts{4, time.Hour, 12, false})
	s.SetLogLevel("debug")
	s.SetStatsPeriod(time.Second * 5)
	s.ConnState = getConnState

	// A method that computes fibonacci
	s.AddMethod("fib", func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
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
	})

	// Serve
	s.Serve()
}

func getConnState(nc *nxsugar.NexusConn, state nxsugar.NexusConnState) {
	nxsugar.Log(nxsugar.InfoLevel, "connState", "I got connState: (%s) and conn (%#v)", state, nc)
	if state == nxsugar.StateServing {
		err := nc.Ping(time.Second * 20)
		if err == nil {
			nxsugar.Log(nxsugar.InfoLevel, "connState", "I did a ping with conn!")
		} else {
			nxsugar.Log(nxsugar.InfoLevel, "connState", "Ping failed with conn!")
		}
	}
}
