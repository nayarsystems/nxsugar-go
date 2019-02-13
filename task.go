package nxsugar

import (
	"time"

	"github.com/jaracil/ei"
	nexus "github.com/nayarsystems/nxgo/nxcore"
)

type NexusConn struct {
	*nexus.NexusConn
	trackid string
}

type Task struct {
	nexus.Task
	Service *Service `json:"-"`
}

func (t *Task) GetConn() *NexusConn {
	tid := ei.N(t.Params).M("@metadata").M("trackid").StringZ()
	if tid == "" {
		tid = newTrackId()
	}
	if conn := t.Task.GetConn(); conn == nil {
		return nil
	} else {
		return &NexusConn{conn, tid}
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
	return nc.NexusConn.TaskPush(method, params, timeout, opts...)
}
