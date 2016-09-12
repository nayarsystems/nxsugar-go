package main

import (
	"github.com/nayarsystems/nxsugar-go"
)

func main() {
	// Service
	s, err := nxsugar.NewServiceFromConfig("handler")
	if err != nil {
		return
	}

	// A handler for all methods
	s.SetHandler(func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		if task.Method == "hello" {
			return "bye", nil
		}
		return nil, &nxsugar.JsonRpcErr{nxsugar.ErrMethodNotFound, "", nil}
	})

	// Serve
	s.Serve()
}
