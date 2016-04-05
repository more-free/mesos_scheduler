package httpserver

import (
	"encoding/json"
	"fmt"
	log "github.com/golang/glog"
	"github.com/more-free/mesos_scheduler/ha"
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/more-free/mesos_scheduler/scheduler"
	"github.com/more-free/mesos_scheduler/util"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// TODO http.Handle("/", http.FileServer(http.Dir(s.staticDir)))
// this cannot be used within virtual box :
// http://stackoverflow.com/questions/20702221/http-fileserver-caching-files-and-serving-old-versions-after-edit

// with leader election
type HASchedulerServer struct {
	scheduler *scheduler.TriggerScheduler
	elector   ha.LeaderElection
	port      int
	staticDir string
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
	http.Handle("/", http.FileServer(http.Dir(s.staticDir)))
	http.HandleFunc("/list", s.ListAll)
	http.HandleFunc("/create", s.CreateTrigger)
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

func (s *HASchedulerServer) ListAll(w http.ResponseWriter, r *http.Request) {

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
