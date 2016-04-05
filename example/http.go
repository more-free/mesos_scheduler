package main

import (
	log "github.com/golang/glog"
	"github.com/more-free/mesos_scheduler/httpserver"
	schd "github.com/more-free/mesos_scheduler/scheduler"
	"os"
	"time"
)

// to demonstrate http interface
// cd mesos_scheduler/example & go run http.go
// press CTRL-C twice to stop
func main() {
	schedConfig := &schd.SchedulerConfig{
		StartTimeout:   time.Second * 10,
		CleanTimeout:   time.Second * 60,
		HistoryTimeout: time.Second * 60,
		MesosMaster:    "127.0.0.1:5050",
		ZkServers:      []string{"127.0.0.1:2181"},
	}
	scheduler := schd.NewTriggerScheduler(schedConfig)

	dir := os.Getenv("GOPATH") + "/src/github.com/more-free/mesos_scheduler/httpserver/ui"
	log.Infoln(dir)
	server := httpserver.NewHASchedulerServer(
		scheduler, nil, 9998, dir,
	)
	server.Start()
}
