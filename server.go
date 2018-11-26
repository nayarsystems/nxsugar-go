package nxsugar

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/jaracil/ei"
	nxcli "github.com/nayarsystems/nxgo"
	nexus "github.com/nayarsystems/nxgo/nxcore"
	"github.com/xeipuuv/gojsonschema"
)

/*
Server allows to have multiple services running in the same binary.
A `Server` can be created with a call to `NewServer()` or `NewServerFromConfig()`.
Its configuration can be changed with calls to `Set...()`.
After it has been configured, services can be added with calls to `AddService()`, the server configuration will be used as default configuration for the service.
*/
type Server struct {
	Url           string
	User          string
	Pass          string
	Pulls         int
	PullTimeout   time.Duration
	MaxThreads    int
	StatsPeriod   time.Duration
	GracefulExit  time.Duration
	LogLevel      string
	Testing       bool
	Version       string
	ConnState     func(*NexusConn, NexusConnState)
	nc            *nexus.NexusConn
	sharedSchemas map[string]gojsonschema.JSONLoader
	services      map[string]*Service
	wg            *sync.WaitGroup
	connLock      sync.Mutex
}

/*
NewServer returns a server that will connect and authenticate with the provided url.
Default values are used for the server.
*/
func NewServer(url string) *Server {
	parseFlags()
	url, username, password := parseServerUrl(url)
	return &Server{Url: url, User: username, Pass: password, Pulls: 1, PullTimeout: time.Hour, MaxThreads: 4, LogLevel: "info", StatsPeriod: time.Minute * 5, GracefulExit: time.Second * 20, Testing: false, Version: "0.0.0", services: map[string]*Service{}}
}

// GetConn returns the underlying nexus connection of a server
func (s *Server) GetConn() *NexusConn {
	s.connLock.Lock()
	defer s.connLock.Unlock()
	if s.nc == nil {
		return nil
	}
	return &NexusConn{NexusConn: s.nc}
}

/*
SetUrl changes the url that the server and its services will use to connect and authenticate to nexus.
*/
func (s *Server) SetUrl(url string) {
	s.Url = url
}

/*
SetUser changes the user the server and its services will use to authenticate to nexus.
*/
func (s *Server) SetUser(user string) {
	s.User = user
}

/*
SetUser changes the password the server and its services will use to authenticate to nexus.
*/
func (s *Server) SetPass(password string) {
	s.Pass = password
}

/*
SetLogLevel changes the global log level (one of debug, info, warn, error, fatal, panic).
*/
func (s *Server) SetLogLevel(l string) {
	s.LogLevel = l
}

/*
SetStatsPeriod changes the period for printing the stats for the server services.
*/
func (s *Server) SetStatsPeriod(t time.Duration) {
	s.StatsPeriod = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetStatsPeriod(t)
		}
	}
}

/*
SetStatsPeriod changes the timeout for the server services to stop gracefully.
*/
func (s *Server) SetGracefulExit(t time.Duration) {
	s.GracefulExit = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetGracefulExit(t)
		}
	}
}

/*
SetVersion changes the version for the server services.
*/
func (s *Server) SetVersion(major int, minor int, patch int) {
	s.Version = fmt.Sprintf("%d.%d.%d", major, minor, patch)
	if s.services != nil {
		for _, svc := range s.services {
			svc.Version = s.Version
		}
	}
}

/*
SetTesting turns on or off the testing mode for the server services.
*/
func (s *Server) SetTesting(t bool) {
	s.Testing = t
	if s.services != nil {
		for _, svc := range s.services {
			svc.SetTesting(t)
		}
	}
}

/*
IsTesting returns wheter the server services are in testing mode or not.
*/
func (s *Server) IsTesting() bool {
	return s.Testing
}

/*
AddSharedSchema adds a schema with an id that can be referenced by method schemas (this schema can be referenced from others with: `{"$ref":"http://nexus.service/id"}`)
*/
func (s *Server) AddSharedSchema(id string, schema string) error {
	return s.addSharedSchema(id, schema)
}

/*
AddSharedSchemaFromFile adds a schema from file with an id that can be referenced by method schemas (this schema can be referenced from others with: `{"$ref":"http://nexus.service/id"}`)
*/
func (s *Server) AddSharedSchemaFromFile(id string, file string) error {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		err = fmt.Errorf("error adding shared jsonschema (%s) from file (%s): %s", id, file, err.Error())
		LogWithFields(ErrorLevel, "server", ei.M{"type": "shared_file"}, err.Error())
		return err
	}
	return s.addSharedSchema(id, string(contents))
}

func (s *Server) addSharedSchema(id string, schema string) error {
	if s.sharedSchemas == nil {
		s.sharedSchemas = map[string]gojsonschema.JSONLoader{}
	}
	if _, ok := s.sharedSchemas[id]; ok {
		err := fmt.Errorf("error adding shared jsonschema (%s): an schema with given id already exists", id)
		LogWithFields(ErrorLevel, "server", ei.M{"type": "adding_shared"}, err.Error())
		return err
	}
	loader, err := getSchemaLoaderFromJson(schema)
	if err != nil {
		err = fmt.Errorf("error adding shared jsonschema (%s): %s", id, err.Error())
		LogWithFields(ErrorLevel, "server", ei.M{"type": "adding_shared"}, err.Error())
		return err
	}
	s.sharedSchemas[id] = loader
	return nil
}

/*
AddService adds a service to a server created with `NewServer()` by name.
If another service was previously added with the same name it will be replaced.
If opts are passed, its values will be used for the service. If not, default values will be used.
*/
func (s *Server) AddService(name string, path string, opts *ServiceOpts) *Service {
	if s.services == nil {
		s.services = map[string]*Service{}
	}
	svc := &Service{Name: name, Url: s.Url, User: s.User, Pass: s.Pass, Path: path, Pulls: s.Pulls, PullTimeout: s.PullTimeout, MaxThreads: s.MaxThreads, LogLevel: s.LogLevel, StatsPeriod: s.StatsPeriod, GracefulExit: s.GracefulExit, Testing: s.Testing, sharedSchemas: s.sharedSchemas}
	if opts != nil {
		opts = populateOpts(opts)
		svc.Pulls = opts.Pulls
		svc.PullTimeout = opts.PullTimeout
		svc.MaxThreads = opts.MaxThreads
		svc.Testing = opts.Testing
	}
	s.services[name] = svc
	return svc
}

func (s *Server) setState(state NexusConnState) {
	if hook := s.ConnState; hook != nil {
		hook(s.GetConn(), state)
	}
}

/*
Serve connects and authenticates with nexus, starts all the services and waits them to end.
It returns any error with the server or the first error from one of the services that caused the nexus connection to stop.
A SIGINT will cause the server to start a graceful stop, if another SIGINT is received then a hard stop will be done.
*/
func (s *Server) Serve() error {
	parseFlags()
	defer s.setState(StateStopped)
	defer func() { s.nc = nil }()
	s.setState(StateInitializing)

	// Parse url
	_, err := url.Parse(s.Url)
	if err != nil {
		err = fmt.Errorf("invalid nexus url (%s): %s", s.Url, err.Error())
		LogWithFields(ErrorLevel, "server", ei.M{"type": "invalid_url"}, err.Error())
		return err
	}

	// Check services
	if s.services == nil || len(s.services) == 0 {
		err = fmt.Errorf("no services to serve")
		LogWithFields(ErrorLevel, "server", ei.M{"type": "no_services"}, err.Error())
		return err
	}

	s.setState(StateConnecting)
	// Dial
	s.connLock.Lock()
	s.nc, err = nxcli.Dial(s.Url, nxcli.NewDialOptions())
	if err != nil {
		if err == nxcli.ErrVersionIncompatible {
			LogWithFields(WarnLevel, "server", ei.M{"type": "incompatible_version"}, "connecting to an incompatible version of nexus at (%s): client (%s) server (%s)", s.Url, nxcli.Version, s.nc.NexusVersion)
		} else {
			err = fmt.Errorf("can't connect to nexus server (%s): %s", s.Url, err.Error())
			LogWithFields(ErrorLevel, "server", ei.M{"type": "connection_error"}, err.Error())
			s.connLock.Unlock()
			return err
		}
	}

	s.setState(StateLoggingIn)
	// Login
	_, err = s.nc.Login(s.User, s.Pass)
	if err != nil {
		err = fmt.Errorf("can't login to nexus server (%s) as (%s): %s", s.Url, s.User, err.Error())
		LogWithFields(ErrorLevel, "server", ei.M{"type": "login_error"}, err.Error())
		s.connLock.Unlock()
		return err
	}

	// Configure services
	for _, svc := range s.services {
		svc.SetLogLevel(s.LogLevel)
		svc.setConn(s.nc)
	}
	s.connLock.Unlock()

	// Wait for signal
	go func() {
		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		LogWithFields(DebugLevel, "signal", ei.M{"type": "graceful_requested"}, "received SIGINT: stop gracefuly")
		for _, svc := range s.services {
			svc.GracefulStop()
		}
		<-signalChan
		LogWithFields(DebugLevel, "signal", ei.M{"type": "stop_requested"}, "received SIGINT again: stop")
		for _, svc := range s.services {
			svc.Stop()
		}
	}()

	// Serve
	s.wg = &sync.WaitGroup{}
	errCh := make(chan error, 0)
	for _, svc := range s.services {
		s.wg.Add(1)
		go func(serv *Service) {
			if err := serv.Serve(); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
			s.wg.Done()
		}(svc)
	}

	s.setState(StateServing)

	var serveErr error
	s.wg.Wait()
	select {
	case serveErr = <-errCh:
	default:
	}
	return serveErr
}
