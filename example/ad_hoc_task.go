package main

import (
	log "github.com/golang/glog"
	"github.com/more-free/mesos_scheduler/protocol"
	schd "github.com/more-free/mesos_scheduler/scheduler"
	"time"
)

// go run ad_hoc_task.go
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
	submitAdHocTasks(scheduler)

	<-time.After(time.Second * 20)
	log.Infoln(scheduler.GetAllStat())
	scheduler.Stop() // must call this to release resources
}

func submitAdHocTasks(scheduler *schd.TriggerScheduler) {
	// Shell command example.
	get, err := scheduler.CreateTrigger(&protocol.Post{
		StartTime: time.Now().Unix(),
		Cpu:       0.1,
		Mem:       8,
		Cmd:       "echo hello,world",
		// Args: []string{},  // Args will be ignored for CMD_SHELL
		CmdType: protocol.CMD_SHELL,
		Name:    "echo-task",
	})
	log.Infoln(get, err)

	// Docker command example.
	get, err = scheduler.CreateTrigger(&protocol.Post{
		StartTime: time.Now().Unix() + 1,
		Cpu:       0.1,
		Mem:       8,
		CmdType:   protocol.CMD_DOCKER,
		Image:     "ubuntu",
		Cmd:       "/bin/echo",
		Args:      []string{"hello,docker"},
		Name:      "docker-task",
	})
	log.Infoln(get, err)

	// Fail-Retry example.
	get, err = scheduler.CreateTrigger(&protocol.Post{
		StartTime:    time.Now().Unix() + 2,
		RepeatPeriod: 0,
		MaxRetry:     3, // -1 for infinite retry
		Cpu:          0.1,
		Mem:          8,
		Cmd:          "fake-cmd",
		CmdType:      protocol.CMD_SHELL,
		Name:         "fake-task",
	})
	log.Infoln(get, err)
}
