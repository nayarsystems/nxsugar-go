package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxsugar-go"
)

func main() {
	// Service
	s, err := nxsugar.NewServiceFromConfig("example")
	if err != nil {
		return
	}

	// A method that returns the service available methods
	s.AddMethod("methods", nxsugar.ReplyToWrapper(func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		return s.GetMethods(), nil
	}))

	// A method that panics: a nxsugar.ErrInternal error will be returned as a result
	s.AddMethod("panic", func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		panic("¿What if a method panics?")
		return "ok", nil
	})

	// A method that calls Stop()
	s.AddMethod("exit", func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		go func() {
			s.Stop()
		}()
		return "why?", nil
	})

	// A method that calls GracefulStop()
	s.AddMethod("gracefulExit", func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		go func() {
			s.GracefulStop()
		}()
		return "meh!", nil
	})

	// A fibonacci method
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
