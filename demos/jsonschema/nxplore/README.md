nxplore
-------

This is a demo of what can be acheived with the use of schemas on nexus services

## Usage:

Install gopherjs and start serving:

```shell
go get -u github.com/gopherjs/gopherjs
gopherjs serve --http :8000
```

Access `http://localhost:8000` on your browser and navigate to nxplore folder.

If missing dependencies `go get -u` them and refresh the browser page.

Then run jsonschema demo on some nexus server, set the nexus connection string (only ws and wss allowed) and click on ¡Explore!.
