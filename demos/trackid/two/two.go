package main

import (
	"time"

	"github.com/nayarsystems/nxsugar-go"
)

func main() {
	// Service
	s := nxsugar.NewService("root:root@localhost", "test.trackid.two", &nxsugar.ServiceOpts{4, time.Hour, 12, false})
	s.SetLogLevel("debug")
	s.AddMethod("m", func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
		c := task.GetConn()
		_, err := c.TaskPush("test.trackid.three.m", nil, time.Second*30)
		if err != nil {
			return nil, &nxsugar.JsonRpcErr{nxsugar.ErrInternal, err.Error(), nil}
		}
		return "ok two", nil
	})

	// Serve
	s.Serve()
}
