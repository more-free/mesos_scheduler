package main

import (
	"github.com/gogo/protobuf/proto"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/more-free/mesos_scheduler/storage"
	trigger "github.com/more-free/mesos_scheduler/util"
	"net"
	"sync"
	"time"
)

type TriggerScheduler struct {
	storeCli storage.Storage
	tasks    *trigger.PostPriorityQueue
	taskLock *sync.Mutex
}

func NewTriggerScheduler(zkServers []string) *TriggerScheduler {
	return &TriggerScheduler{
		storeCli: storage.NewZkStorage(zkServers, "/trigger-scheduler"),
		tasks:    trigger.NewPostPriorityQueue(),
		taskLock: &sync.Mutex{},
	}
}

func (s *TriggerScheduler) Init() error {
	return s.storeCli.Open()
}

func (s *TriggerScheduler) Cleanup() error {
	return s.storeCli.Close()
}

func (s *TriggerScheduler) TriggerUpdated(updatedTask *protocol.Update) error {
	err := s.storeCli.Update(updatedTask)
	if err != nil {
		return err
	}
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	s.tasks.Update(updatedTask)
	return nil
}

func (s *TriggerScheduler) TriggerCreated(newTask *protocol.Post) (*protocol.Get, error) {
	get, err := s.storeCli.Post(newTask)
	if err != nil {
		return nil, err
	}
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	s.tasks.Push(&protocol.Update{get.TaskId, newTask})
	return get, nil
}

func (s *TriggerScheduler) isTriggerable(t *protocol.Update) bool {
	return t.StartTime <= time.Now().Unix()
}

func (s *TriggerScheduler) loadAll() error {
	updates, err := s.storeCli.GetAll()
	if err != nil {
		return err
	}

	for _, u := range updates {
		s.tasks.Push(u)
	}
	return nil
}

func (s *TriggerScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
	err := s.loadAll()
	if err != nil {
		log.Infoln("Failed to load from zookeeper", err)
	}
}

func (s *TriggerScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
	err := s.loadAll()
	if err != nil {
		log.Infoln("Failed to load from zookeeper", err)
	}
}

func (s *TriggerScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
}

func (s *TriggerScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	mesosTasks := make([]*mesos.TaskInfo, 0)
	for _, offer := range offers {
		pendingTasks := s.prepareTask(offer)
		for _, t := range pendingTasks {
			var task *mesos.TaskInfo
			switch t.CmdType {
			case protocol.CMD_DOCKER:
				task = s.toMesosDockerTask(t)
			case protocol.CMD_SHELL:
				task = s.toMesosShellTask(t)
			}
			task.SlaveId = offer.SlaveId
			mesosTasks = append(mesosTasks, task)
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())
		}
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, mesosTasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		log.Infof("Launched %v tasks with offer %s\n", len(mesosTasks), offer.Id.GetValue())
	}
}

// convert internal task struct (protocol.Post) to mesos.TaskInfo
func (s *TriggerScheduler) toMesosShellTask(task *protocol.Update) *mesos.TaskInfo {
	return &mesos.TaskInfo{
		Name: proto.String(task.Name),
		TaskId: &mesos.TaskID{
			Value: proto.String(task.TaskId),
		},
		Command: &mesos.CommandInfo{
			Shell: proto.Bool(true),
			Value: proto.String(task.Cmd),
			// Arguments field will be ignored for Shell command
		},
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", task.Cpu),
			util.NewScalarResource("mem", task.Mem),
		},
	}
}

func (s *TriggerScheduler) toMesosDockerTask(task *protocol.Update) *mesos.TaskInfo {
	containerType := mesos.ContainerInfo_DOCKER // const, must assign to a var to pass by reference
	// mesos starts this task via :
	// docker run [Container.Docker.Image] [Command.Value] [Command.Arguments]
	// where Command.Value is the name of the executable binary.
	// ex. docker run ubuntu /bin/echo hello
	// if the image already has default entrypoint, Command.Value field can be omitted.
	// see an example here :  https://github.com/derekchiang/Mesos-Bitcoin-Miner/blob/master/scheduler.go#L147
	// it is also possible to specify options for "docker run" command, one just needs to set
	// Container.Docker.Parameters field.
	return &mesos.TaskInfo{
		Name: proto.String(task.Name),
		TaskId: &mesos.TaskID{
			Value: proto.String(task.TaskId),
		},
		Container: &mesos.ContainerInfo{
			Type: &containerType,
			Docker: &mesos.ContainerInfo_DockerInfo{
				Image: proto.String(task.Image),
			},
		},
		Command: &mesos.CommandInfo{
			Shell:     proto.Bool(false),
			Value:     proto.String(task.Cmd),
			Arguments: task.Args,
		},
		Resources: []*mesos.Resource{
			util.NewScalarResource("cpus", task.Cpu),
			util.NewScalarResource("mem", task.Mem),
		},
	}
}

// compute the max number of tasks that can be launched given an offer
func (s *TriggerScheduler) prepareTask(offer *mesos.Offer) []*protocol.Update {
	pendingTasks := make([]*protocol.Update, 0)

	var cpus, mem float64
	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpus += *resource.GetScalar().Value
		case "mem":
			mem += *resource.GetScalar().Value
		}
	}

	s.taskLock.Lock()
	defer s.taskLock.Unlock()

	left := make([]*protocol.Update, 0)
	for s.tasks.Len() > 0 {
		next := s.tasks.Pop()
		if !s.isTriggerable(next) {
			left = append(left, next)
			break
		}

		if cpus >= next.Cpu && mem >= next.Mem {
			cpus -= next.Cpu
			mem -= next.Mem
			pendingTasks = append(pendingTasks, next)
		}

		if cpus == 0.0 || mem == 0.0 {
			break
		}
	}

	// add back unlaunched tasks to s.tasks
	for _, task := range left {
		s.tasks.Push(task)
	}

	return pendingTasks
}

func (s *TriggerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		err := s.storeCli.Delete(&protocol.Delete{*status.GetTaskId().Value})
		log.Infof("Delete finished task : %v %v\n", status.GetTaskId(), err)
	}
}

func (s *TriggerScheduler) OfferRescinded(driver sched.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (s *TriggerScheduler) FrameworkMessage(driver sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (s *TriggerScheduler) SlaveLost(driver sched.SchedulerDriver, id *mesos.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
}

func (s *TriggerScheduler) ExecutorLost(driver sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
}

func (s *TriggerScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}

func main() {
	//master := "10.88.196.178:5050"
	master := "127.0.0.1:5050"
	zkServers := []string{"127.0.0.1:2181"}
	scheduler := NewTriggerScheduler(zkServers)
	err := scheduler.Init()
	defer scheduler.Cleanup()

	scheduler.storeCli.DeleteAll() // TODO remove this line

	if err != nil {
		log.Errorln(err)
		return
	}

	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Morefree Test Framework (Go)"),
	}

	config := sched.DriverConfig{
		Scheduler:  scheduler,
		Framework:  fwinfo,
		Master:     master,
		Credential: (*mesos.Credential)(nil),
		//BindingAddress: parseIP("127.0.0.1"), // must use this line, why ???
	}

	driver, err := sched.NewMesosSchedulerDriver(config)
	if err != nil {
		log.Errorln("Unable to create driver", err)
	}

	go func() {
		time.Sleep(time.Second * 3)
		get, err := scheduler.TriggerCreated(&protocol.Post{
			StartTime:    time.Now().Unix() + 1, // 5 seconds after now
			RepeatPeriod: 0,
			Cpu:          0.1,
			Mem:          8,
			Cmd:          "bash /home/vagrant/some.sh test-task",
			CmdType:      protocol.CMD_SHELL,
			Name:         "test-task",
		})
		log.Infoln(get, err)

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
			Cpu:          0.1,
			Mem:          8,
			Cmd:          "bash /home/vagrant/some.sh test-task-3",
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

	}()

	go func() {
		time.Sleep(time.Second * 60)
		driver.Abort()
	}()

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s", stat.String(), err.Error())
	}
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}
