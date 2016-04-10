package main

import (
	log "github.com/golang/glog"
	"github.com/more-free/mesos_scheduler/protocol"
	schd "github.com/more-free/mesos_scheduler/scheduler"
	"time"
)

// go run long_running_task.go
func main() {
	schedConfig := &schd.SchedulerConfig{
		StartTimeout:   time.Second * 10,
		CleanTimeout:   time.Second * 60,
		HistoryTimeout: time.Second * 60,
		MesosMaster:    "127.0.0.1:5050",
		ZkServers:      []string{"127.0.0.1:2181"},
	}

	scheduler := schd.NewTriggerScheduler(schedConfig)
	go scheduler.Start()

	<-time.After(time.Second * 3)
	scheduler.Clear() // Delete all previous tasks. Optional.
	submitLongRunningTasks(scheduler)

	<-time.After(time.Second * 12000)
	log.Infoln(scheduler.GetAllStat())
	scheduler.Stop() // must call this to release resources
}

func submitLongRunningTasks(scheduler *schd.TriggerScheduler) {
	// long_running task example.
	get, err := scheduler.CreateTrigger(&protocol.Post{
		StartTime:    time.Now().Unix() + 1,
		MaxRetry:     -1, // infinite retry
		RepeatPeriod: 0,
		Cpu:          0.1,
		Mem:          8,
		CmdType:      protocol.CMD_DOCKER,
		Image:        "python:3",
		Cmd:          "python3",
		Args:         []string{"-m", "http.server", "9999"},
		PortMapping: []*protocol.PortMapping{
			// hostport to container port mapping.
			// setting hostport to 0 guides the scheduler to dynamically allocate a port,
			// usually from 31000 to 32000
			&protocol.PortMapping{0, 9999, "tcp"},
		},
		Name: "long-running-docker-task-dynamic-port",
	})
	log.Infoln(get, err)

	get, err = scheduler.CreateTrigger(&protocol.Post{
		StartTime:    time.Now().Unix() + 2,
		MaxRetry:     -1,
		RepeatPeriod: 0,
		Cpu:          0.1,
		Mem:          8,
		CmdType:      protocol.CMD_DOCKER,
		Image:        "python:3",
		Cmd:          "python3",
		Args:         []string{"-m", "http.server", "9999"},
		PortMapping: []*protocol.PortMapping{
			// though not recommended, we can allocate a static port mapping here, ranging [31000, 32000]
			// Note that if a unavailable port is specified (either beyond the range or already in use),
			// the scheduler will wait for the port from mesos offer.  It won't return any error which makes
			// the user hard to notice.
			&protocol.PortMapping{31002, 9999, "tcp"},
		},
		Name: "long-running-docker-task-static-port",
	})
	log.Infoln(get, err)
}
