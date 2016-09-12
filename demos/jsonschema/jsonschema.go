package main

import (
	"time"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxsugar-go"
	. "github.com/nayarsystems/nxsugar-go/log"
)

func main() {
	// Service
	s, err := nxsugar.NewServiceFromConfig("jsonschema")
	if err != nil {
		return
	}

	// Config
	config, err := nxsugar.GetConfig()
	if err != nil {
		return
	}

	sleep := ei.N(config).M("services").M("jsonschema").M("sleep").IntZ()

	if sleep < 0 {
		sleep = 0
	}

	s.Log(InfoLevel, "My sleep opt: %d", sleep)

	if err = s.AddMethodSchema("person", &nxsugar.Schema{FromFile: "personSchema.json"},
		func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
			time.Sleep(time.Second * time.Duration(sleep))
			return task.Params, nil
		},
	); err != nil {
		return
	}

	if err = s.AddMethodSchema("secret", &nxsugar.Schema{
		Input:  `{"type":"string","minLength":8}`,
		Result: `{"type":"string","enum":["the correct result"]}`,
		Error:  `{"type": "object", "properties":{"code": {"type": "integer"}, "message": {"type": "string"}, "data": {"type": "object"}}, "required":["code", "message"]}`,
	},
		func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
			time.Sleep(time.Second * time.Duration(sleep))
			return task.Params, nil
		},
	); err != nil {
		return
	}

	if err = s.AddMethodSchema("pactest", &nxsugar.Schema{FromFile: "pactestSchema.json"},
		func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
			return "not implemented", nil
		},
	); err != nil {
		return
	}

	// Serve
	s.Serve()
}
