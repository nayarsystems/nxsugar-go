package main

import (
	"fmt"
	"net/url"
	"time"

	"github.com/gopherjs/gopherjs/js"
	"github.com/gopherjs/jquery"
	nxcli "github.com/nayarsystems/nxgo"
	nexus "github.com/nayarsystems/nxgo/nxcore"
)

var JQ = jquery.NewJQuery

var ses *nexus.NexusConn
var editor *js.Object

var prefix string
var host string
var method string

var editorContainer jquery.JQuery
var alertsContainer jquery.JQuery
var methodsContainer jquery.JQuery

var prefixInput jquery.JQuery
var hostInput jquery.JQuery

var loadPrefixBtn jquery.JQuery
var callMethodBtn jquery.JQuery

var jsoneditor *js.Object

func main() {
	// Elements
	editorContainer = JQ("#editor_container")
	alertsContainer = JQ("#alerts_container")
	methodsContainer = JQ("#methods_container")

	prefixInput = JQ("#prefix")
	hostInput = JQ("#host")

	loadPrefixBtn = JQ("#load_prefix")
	callMethodBtn = JQ("#call_method")

	// Set defaults
	jsoneditor = js.Global.Get("JSONEditor")
	jsoneditor.Get("defaults").Set("theme", "foundation5")
	jsoneditor.Get("defaults").Set("iconlib", "fontawesome4")

	editorContainer.Hide()
	methodsContainer.Hide()

	// Explore
	loadPrefixBtn.On("click", func(ev *jquery.Event) {
		go func() {
			var err error
			host = hostInput.Val()
			prefix = prefixInput.Val()

			// Reset editor
			alertsContainer.Empty()
			methodsContainer.Hide().Find("ul").Empty()
			editorContainer.Hide()
			if editor != nil {
				editor.Call("destroy")
			}

			// Connect to nexus
			ses, err = nxcli.Dial(host, nil)
			if err != nil {
				alertsContainer.Append(fmt.Sprintf(`<p>Error connecting: %s</p>`, err.Error()))
				return
			}

			// Login
			parsed, err := url.Parse(host)
			if err != nil {
				alertsContainer.Append(fmt.Sprintf(`<p>Invalid nexus url (%s): %s</p>`, host, err.Error()))
				return
			}
			if parsed.User == nil {
				alertsContainer.Append(fmt.Sprintf(`<p>Invalid nexus url (%s): user or pass not found</p>`, host))
				return
			}
			username := parsed.User.Username()
			password, _ := parsed.User.Password()
			if username == "" || password == "" {
				alertsContainer.Append(fmt.Sprintf(`<p>Invalid nexus url (%s): user or pass is empty</p>`, host))
				return
			}
			_, err = ses.Login(username, password)
			if err != nil {
				alertsContainer.Append(fmt.Sprintf(`<p>Can't login to nexus server (%s)</p>`, host))
				return
			}

			// Get schema
			res, err := ses.TaskPush(fmt.Sprintf("%s.@schema", prefix), nil, time.Second*6)
			if err != nil {
				alertsContainer.Append(fmt.Sprintf(`<p>Error getting methods: %s</p>`, err.Error()))
				return
			}

			// Get methods
			resm, ok := res.(map[string]interface{})
			if !ok {
				alertsContainer.Append(`<p>Unexpected result: Expecting map</p>`)
				return
			}

			methodsContainer.Find(".methods_title").SetHtml(fmt.Sprintf("%s microservice methods:", prefix))
			methodsContainer.Show()
			for name, schemas := range resm {
				methodsContainer.Find(".methods_list").Append(JQ(fmt.Sprintf(`<li><a class="small button">%s</a></li>`, name)).On("click", loadMethod(name, schemas)))
			}
		}()
	})

	// Call
	callMethodBtn.On("click", func() {
		go func() {
			if !ses.Closed() {
				if errs := editor.Call("validate"); errs.Length() != 0 {
					msg := "json schema validation: "
					if errs.Length() == 1 {
						msg += fmt.Sprintf("%s: %s", errs.Index(0).Get("path").String()[5:], errs.Index(0).Get("message").String())
					} else {
						for i := 0; i < errs.Length(); i++ {
							msg += fmt.Sprintf("\n- %s: %s", errs.Index(i).Get("path").String()[5:], errs.Index(i).Get("message").String())
						}
					}
					if !js.Global.Call("confirm", fmt.Sprintf("Error: Invalid params:[%s]\n\n¿Do you want to call the method anyway?", msg)).Bool() {
						return
					}
				}
				res, err := ses.TaskPush(fmt.Sprintf("%s.%s", prefix, method), editor.Call("getValue").Interface(), time.Second*15)
				if err != nil {
					js.Global.Call("alert", fmt.Sprintf("Error: %s", err.Error()))
				} else {
					js.Global.Call("alert", fmt.Sprintf("Result: %+v", res))
				}
			} else {
				alertsContainer.Append(`<p>Session is closed</p>`)
			}
		}()
	})
}

func loadMethod(name string, schemas interface{}) func() {
	return func() {
		editorContainer.Hide()
		if editor != nil {
			editor.Call("destroy")
		}
		schemam, _ := schemas.(map[string]interface{})
		schema, ok := schemam["input"]
		if !ok {
			schema = map[string]interface{}{}
		}
		editor = jsoneditor.New(js.Global.Get("document").Call("getElementById", "editor_holder"), map[string]interface{}{
			"schema": schema,
		})
		method = name
		js.Global.Set("editor", editor)
		editor.Call("on", "ready", func() {
			editorContainer.Find(".editor_title").SetHtml(fmt.Sprintf("%s.%s method", prefix, name))
			editorContainer.Show()
		})
	}
}
