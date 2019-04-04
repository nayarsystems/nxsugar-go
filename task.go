package nxsugar

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jaracil/ei"
	nexus "github.com/nayarsystems/nxgo/nxcore"
)

type NexusConn struct {
	*nexus.NexusConn
	trackid          string
	isMocked         bool
	mockResponses    []TaskMockResponse
	responseCountRef *uint64
}

type Task struct {
	nexus.Task
	Service       *Service `json:"-"`
	isMocked      bool
	mockResponses []TaskMockResponse
	responseCount uint64
}

type TaskMockResponse struct {
	Result interface{}
	Error  error
}

func NewMockedTask(task *Task, mockedResponses []TaskMockResponse) *Task {
	task.isMocked = true
	task.mockResponses = mockedResponses
	task.responseCount = 0
	return task
}

func (t *Task) GetConn() *NexusConn {
	tid := ei.N(t.Params).M("@metadata").M("trackid").StringZ()
	if tid == "" {
		tid = newTrackId()
	}
	if conn := t.Task.GetConn(); conn == nil {
		return nil
	} else {
		return &NexusConn{conn, tid, t.isMocked, t.mockResponses, &t.responseCount}
	}
}

func (nc *NexusConn) TaskPush(method string, params interface{}, timeout time.Duration, opts ...*nexus.TaskOpts) (interface{}, error) {
	if params == nil {
		params = ei.M{"@metadata": ei.M{"trackid": nc.trackid}}
	} else if pm, err := ei.N(params).MapStr(); err == nil {
		if pm == nil {
			pm = map[string]interface{}{}
		}
		md, err := ei.N(pm).M("@metadata").MapStr()
		if err != nil || md == nil {
			md = map[string]interface{}{}
		}
		tid := ei.N(md).M("trackid").StringZ()
		if tid == "" {
			tid = nc.trackid
		}
		md["trackid"] = tid
		pm["@metadata"] = md
		params = pm
	}
	if nc.isMocked {
		mockResIdx := atomic.AddUint64(nc.responseCountRef, 1)
		if len(nc.mockResponses) < int(mockResIdx) {
			return nil, fmt.Errorf("No more mock responses")
		}
		response := nc.mockResponses[mockResIdx-1]
		return response.Result, response.Error
	}
	return nc.NexusConn.TaskPush(method, params, timeout, opts...)
}
