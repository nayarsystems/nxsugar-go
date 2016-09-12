package main

import (
	"time"

	"github.com/nayarsystems/nxsugar-go"
)

func main() {
	// Service
	s := nxsugar.NewService("root:root@localhost", "test.trackid.three", &nxsugar.ServiceOpts{4, time.Hour, 12, false})
	s.SetLogLevel("debug")
	s.AddMethod("m", func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		return "ok three", nil
	})

	// Serve
	s.Serve()
}
