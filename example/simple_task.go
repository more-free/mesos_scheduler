package main

import (
	log "github.com/golang/glog"
	"github.com/more-free/mesos_scheduler/protocol"
	schd "github.com/more-free/mesos_scheduler/scheduler"
	"time"
)

// TODO suppress CTRL-C signal, make sure Stop() is always called.
func main() {
	schedConfig := &schd.SchedulerConfig{
		StartTimeout:   time.Second * 10,
		CleanTimeout:   time.Second * 60,
		HistoryTimeout: time.Second * 60,
		MesosMaster:    "127.0.0.1:5050",
		ZkServers:      []string{"127.0.0.1:2181"},
	}

	scheduler := schd.NewTriggerScheduler(schedConfig)
	go func() {
		<-time.After(time.Second * 3)
		scheduler.Clear()
		submitTasks(scheduler)

		<-time.After(time.Second * 20)
		err := scheduler.Stop()
		if err != nil {
			log.Infoln(err)
		}
	}()

	err := scheduler.Start()
	log.Infoln("Post-start", err)
}

func submitTasks(scheduler *schd.TriggerScheduler) {
	get, err := scheduler.CreateTrigger(&protocol.Post{
		StartTime:    time.Now().Unix() + 1, // 5 seconds after now
		RepeatPeriod: 0,
		Cpu:          0.1,
		Mem:          8,
		Cmd:          "bash /home/vagrant/some.sh test-task",
		CmdType:      protocol.CMD_SHELL,
		Name:         "test-task",
	})
	log.Infoln(get, err)

	/*
		get, err = scheduler.TriggerCreated(&protocol.Post{
			StartTime:    time.Now().Unix() + 3, // 5 seconds after now
			RepeatPeriod: 0,
			Cpu:          0.1,
			Mem:          8,
			Cmd:          "bash /home/vagrant/some.sh test-task-2",
			CmdType:      protocol.CMD_SHELL,
			Name:         "test-task-2",
		})
		log.Infoln(get, err)

		get, err = scheduler.TriggerCreated(&protocol.Post{
			StartTime:    time.Now().Unix() + 5, // 5 seconds after now
			RepeatPeriod: 0,
			MaxRetry:     3, // -1 for infinity
			Cpu:          0.1,
			Mem:          8,
			Cmd:          "fake-bash /home/vagrant/some.sh test-task-3",
			CmdType:      protocol.CMD_SHELL,
			Name:         "test-task-3",
		})
		log.Infoln(get, err)

		get, err = scheduler.TriggerCreated(&protocol.Post{
			StartTime:    time.Now().Unix() + 7, // 5 seconds after now
			RepeatPeriod: 0,
			Cpu:          0.1,
			Mem:          8,
			CmdType:      protocol.CMD_DOCKER,
			Image:        "ubuntu",
			Cmd:          "/bin/echo",
			Args:         []string{"I-am-test-task-docker"},
			Name:         "test-task-docker",
		})
		log.Infoln(get, err)
	*/

	get, err = scheduler.CreateTrigger(&protocol.Post{
		StartTime:    time.Now().Unix() + 1,
		MaxRetry:     -1,
		RepeatPeriod: 0,
		Cpu:          0.1,
		Mem:          8,
		CmdType:      protocol.CMD_DOCKER,
		Image:        "python:3",
		Cmd:          "python3",
		Args:         []string{"-m", "http.server", "9999"},
		PortMapping: []*protocol.PortMapping{
			&protocol.PortMapping{0, 9999, "tcp"},
		},
		Name: "test-task-docker-long-running",
	})
	log.Infoln(get, err)

	get, err = scheduler.CreateTrigger(&protocol.Post{
		StartTime:    time.Now().Unix() + 1,
		MaxRetry:     -1,
		RepeatPeriod: 0,
		Cpu:          0.1,
		Mem:          8,
		CmdType:      protocol.CMD_DOCKER,
		Image:        "python:3",
		Cmd:          "python3",
		Args:         []string{"-m", "http.server", "9999"},
		PortMapping: []*protocol.PortMapping{
			&protocol.PortMapping{0, 9999, "tcp"},
		},
		Name: "test-task-docker-long-running-2",
	})
	log.Infoln(get, err)
}
