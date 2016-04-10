package httpserver

import (
	"encoding/json"
	"fmt"
	log "github.com/golang/glog"
	"github.com/more-free/mesos_scheduler/ha"
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/more-free/mesos_scheduler/scheduler"
	"github.com/more-free/mesos_scheduler/util"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// TODO http.Handle("/", http.FileServer(http.Dir(s.staticDir)))
// this cannot be used within virtual box :
// http://stackoverflow.com/questions/20702221/http-fileserver-caching-files-and-serving-old-versions-after-edit
// TODO use interface not struct

// show in the UI
type TaskView struct {
	Id    string             `json:"id"` // optional. task id.
	Name  string             `json:"name"`
	Type  protocol.CmdType   `json:"type"`
	Cmd   string             `json:"cmd"`
	Host  string             `json:"host"`
	Ports []uint32           `json:"ports"`
	State protocol.TaskState `json:"state"`
}

// with leader election
type HASchedulerServer struct {
	scheduler *scheduler.TriggerScheduler
	elector   ha.LeaderElection
	port      int
	staticDir string // root directory of static files
}

func NewHASchedulerServer(scheduler *scheduler.TriggerScheduler,
	elector ha.LeaderElection, port int, staticDir string) *HASchedulerServer {
	server := &HASchedulerServer{
		scheduler: scheduler,
		elector:   elector,
		port:      port,
		staticDir: staticDir,
	}
	return server
}

func (s *HASchedulerServer) WithPort(newPort int) *HASchedulerServer {
	s.port = newPort
	return s
}

func (s *HASchedulerServer) WithStaticDir(newStaticDir string) *HASchedulerServer {
	s.staticDir = newStaticDir
	return s
}

func (s *HASchedulerServer) Start() {
	go s.scheduler.Start()
	if s.elector != nil {
		go s.elector.ElectLeader() // TODO more on this later
	}
	go s.captureInterrupt()

	// this FileServer function cannot be used within virtual box, see discussion here :
	// http://stackoverflow.com/questions/20702221/http-fileserver-caching-files-and-serving-old-versions-after-edit
	// also note that back-slash is needed to indicate a subtree rooted as /css/, rather than /
	// ex. we can have two valid patterns, "/css/" and "/css", they map to different patterns
	http.Handle("/css/", http.FileServer(http.Dir(s.staticDir)))
	http.Handle("/js/", http.FileServer(http.Dir(s.staticDir)))
	http.Handle("/fonts/", http.FileServer(http.Dir(s.staticDir)))

	http.HandleFunc("/create", s.CreateTrigger)
	http.HandleFunc("/", s.Welcome) // unmatched patterns fall here
	http.ListenAndServe(fmt.Sprintf(":%d", s.port), nil)
}

func (s *HASchedulerServer) captureInterrupt() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	signal.Notify(ch, syscall.SIGTERM)

	select {
	case <-ch:
		log.Infoln("Interruption received. Pre-quit cleanup for http server...")
		// scheduler will capture the signal and do its own cleanup
		if s.elector != nil {
			s.elector.Close()
		}

		// Reset only works for go 1.5 or above
		//signal.Reset(os.Interrupt)
		//signal.Reset(syscall.SIGTERM)

		// for go 1.4 or below
		signal.Stop(ch)
	}
}

func (s *HASchedulerServer) Welcome(w http.ResponseWriter, r *http.Request) {
	rts, err := s.scheduler.GetAllStat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	taskViews := s.toTaskView(rts)
	temp, err := template.ParseFiles(s.staticDir + "/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	temp.Execute(w, taskViews)
}

func (s *HASchedulerServer) ListAll(w http.ResponseWriter, r *http.Request) {
	rts, err := s.scheduler.GetAllStat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	taskViews := s.toTaskView(rts)
	bytes, err := json.Marshal(taskViews)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(bytes)
}

func (s *HASchedulerServer) toTaskView(rts protocol.TaskRunTimeList) []*TaskView {
	tasks := make([]*TaskView, len(rts))
	for i := 0; i < len(tasks); i++ {
		tasks[i] = &TaskView{
			Name:  rts[i].Post.Name,
			Type:  rts[i].Post.CmdType,
			Cmd:   rts[i].Post.Cmd,
			Host:  rts[i].Host,
			Ports: s.extractHostPorts(rts[i].PortMapping),
			State: rts[i].State,
		}
	}
	return tasks
}

func (s *HASchedulerServer) extractHostPorts(portMapping []*protocol.PortMapping) []uint32 {
	hostports := make([]uint32, len(portMapping))
	for i := 0; i < len(hostports); i++ {
		hostports[i] = portMapping[i].HostPort
	}
	return hostports
}

func (s *HASchedulerServer) CreateTrigger(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	post, err := protocol.ToPost(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	post = util.WithDefault(post)
	get, err := s.scheduler.CreateTrigger(post)
	log.Infoln("Created trigger for", post, ", and returned", get, " with error ", err)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := json.Marshal(get)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(res)
}
