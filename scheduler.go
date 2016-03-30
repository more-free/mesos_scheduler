package main

// TODO support repeating tasks

import (
	"github.com/gogo/protobuf/proto"

	"errors"
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
	taskStore        storage.Storage
	taskRuntimeStore storage.RuntimeStore
	// runtime data is moved to history data once it completes or fails. these data can be further consumed by other services
	taskStatStore  storage.RuntimeStore
	tasks          *trigger.PostPriorityQueue
	taskLock       *sync.Mutex
	config         *SchedulerConfig
	cleanStateQuit chan struct{} // switch on/off cleanState()
	cleanLock      *sync.Mutex
}

type SchedulerConfig struct {
	StartTimeout   time.Duration // the max duration when a task stays in "STARTING" state
	CleanTimeout   time.Duration // the max duration after a task finishes or fails. Any state data exceeds
	HistoryTimeout time.Duration // the max duration for a stat data to exist
}

func (s *TriggerScheduler) nowInMS() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func (s *TriggerScheduler) nanoToMS(nano int64) int64 {
	return nano / (int64(time.Millisecond) / int64(time.Nanosecond))
}

// a cron job runs periodically
func (s *TriggerScheduler) cleanState() {
	run := func(ch chan<- struct{}, timeout time.Duration, quit <-chan struct{}) {
		for {
			select {
			case <-quit:
				return
			case <-time.After(timeout):
				ch <- struct{}{}
			}
		}
	}

	overTimeNow := func(lastModifiedMS int64, timeout time.Duration) bool {
		return s.nowInMS()-lastModifiedMS >= s.nanoToMS(timeout.Nanoseconds())
	}

	startChan := make(chan struct{})
	cleanChan := make(chan struct{})
	historyChan := make(chan struct{})

	go run(startChan, s.config.StartTimeout, s.cleanStateQuit)
	go run(cleanChan, s.config.CleanTimeout, s.cleanStateQuit)
	go run(historyChan, s.config.HistoryTimeout, s.cleanStateQuit)

	for {
		select {
		case <-startChan:
			go func() {
				ids, _ := s.taskRuntimeStore.GetAllId()
				for _, id := range ids {
					rt, err := s.taskRuntimeStore.GetRuntime(id)
					if err != nil && rt.State == protocol.TASK_STATE_STARTING &&
						overTimeNow(rt.LastModifiedMS, s.config.StartTimeout) {
						_, _, err = s.taskRuntimeStore.SetRuntime(
							id,
							&protocol.TaskRunTime{
								Failure:        rt.Failure,
								State:          protocol.TASK_STATE_STAGING,
								LastModifiedMS: s.nowInMS(),
							},
						)
						if err != nil {
							log.Errorf("Failed to update state for task : %v with error %v\n", id, err)
						} else {
							log.Infof("Update task %v from STARTING to STAGING\n", id)
						}
					}
				}
			}()

		case <-cleanChan: // do we really need this ?
			go func() {
				s.cleanLock.Lock()
				ids, _ := s.taskRuntimeStore.GetAllId()
				for _, id := range ids {
					rt, err := s.taskRuntimeStore.GetRuntime(id)
					if err != nil {
						if rt.State == protocol.TASK_STATE_FINISHED {
							s.deleteOrRepeat(string(id))
						} else if rt.State == protocol.TASK_STATE_FAILED {
							s.deleteOrRetry(string(id))
						}
					}
				}
				s.cleanLock.Unlock()
			}()

		case <-historyChan:
			go func() {
				ids, _ := s.taskStatStore.GetAllId()
				for _, id := range ids {
					rt, err := s.taskStatStore.GetRuntime(id)
					if err != nil && overTimeNow(rt.LastModifiedMS, s.config.HistoryTimeout) {
						err = s.taskStatStore.Delete(id)
						if err != nil {
							log.Errorf("Failed to delete task %v from history data store : %v\n", id, err)
						} else {
							log.Infof("Deleted task %v from history data store\n", id)
						}
					}
				}
			}()

		case <-s.cleanStateQuit:
			log.Infoln("State cleaner quit")
			return
		}
	}
}

func (s *TriggerScheduler) moveToHistory(id string) error {
	rt, err := s.taskRuntimeStore.GetRuntime(storage.TaskId(id))
	if err != nil {
		log.Errorf("Failed to move to history %v\n", err)
		return err
	}

	if rt.State == protocol.TASK_STATE_FINISHED || rt.State == protocol.TASK_STATE_FAILED {
		_, _, err = s.taskStatStore.SetRuntime(storage.TaskId(id), rt)
		if err != nil {
			log.Errorf("Failed to move to history %v\n", err)
			return err
		}

		err = s.taskRuntimeStore.Delete(storage.TaskId(id))
		if err != nil {
			log.Errorf("Failed to move to history %v\n", err)
			return err
		}
	}

	log.Infof("Moved task %v to history data\n", id)
	return nil
}

func NewTriggerScheduler(zkServers []string, config *SchedulerConfig) *TriggerScheduler {
	return &TriggerScheduler{
		taskStore: storage.NewZkStorage(zkServers, "/trigger-scheduler/task"),
		taskRuntimeStore: storage.NewZkRuntimeStore(
			storage.NewZkStorage(zkServers, "/trigger-scheduler/state").(*storage.ZkStorage),
		),
		taskStatStore: storage.NewZkRuntimeStore(
			storage.NewZkStorage(zkServers, "/trigger-scheduler/stat").(*storage.ZkStorage),
		),
		tasks:          trigger.NewPostPriorityQueue(),
		taskLock:       &sync.Mutex{},
		config:         config,
		cleanStateQuit: make(chan struct{}),
		cleanLock:      &sync.Mutex{},
	}
}

func (s *TriggerScheduler) Init() error {
	err := s.taskStore.Open()
	if err != nil {
		return err
	}

	err = s.taskRuntimeStore.Open()
	if err != nil {
		return err
	}

	err = s.taskStatStore.Open()
	if err != nil {
		return err
	}

	go s.cleanState()
	return nil
}

func (s *TriggerScheduler) Cleanup() error {
	close(s.cleanStateQuit)
	taskErr := s.taskStore.Close()
	rtErr := s.taskRuntimeStore.Close()
	stErr := s.taskStatStore.Close()
	if taskErr != nil || rtErr != nil || stErr != nil {
		return errors.New(s.errToStr(taskErr) + ";" + s.errToStr(rtErr) + ";" + s.errToStr(stErr))
	} else {
		return nil
	}
}

func (s *TriggerScheduler) errToStr(err error) string {
	if err == nil {
		return ""
	} else {
		return err.Error()
	}
}

func (s *TriggerScheduler) TriggerUpdated(updatedTask *protocol.Update) error {
	err := s.taskStore.Update(updatedTask)
	if err != nil {
		return err
	}
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	s.tasks.Update(updatedTask)
	return nil
}

func (s *TriggerScheduler) TriggerCreated(newTask *protocol.Post) (*protocol.Get, error) {
	get, err := s.taskStore.Post(newTask)
	if err != nil {
		return nil, err
	}
	_, _, err = s.taskRuntimeStore.SetRuntime(storage.TaskId(get.TaskId),
		&protocol.TaskRunTime{
			Failure:        0,
			State:          protocol.TASK_STATE_STAGING,
			LastModifiedMS: s.nowInMS(),
		},
	)
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
	updates, err := s.taskStore.GetAll()
	if err != nil {
		return err
	}

	// it is important to keep the date in zookeeper and the data in memory consistent,
	// i.e. all data in memory must be the data with STAGING state.
	for _, u := range updates {
		state, err := s.taskRuntimeStore.GetState(storage.TaskId(u.TaskId))
		if err != nil && state == protocol.TASK_STATE_STAGING {
			s.tasks.Push(u)
		}
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
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			// it is important to update the state before launching any task. Ideally "launch a task" and "update the task state"
			// should be done atomically, i.e. we have to update mesos and our state store at the same time. Given the difficulty of
			// committing a distributed transaction, here we use a workaround : updating task state before launching a task.
			// If we launch a task before updating a state, then if the "launching task" succeeds, but "updating state" fails, then
			// the next time there might be duplicate tasks launched. Instead we always update state first, by doing this, if "launching task"
			// fails, the states of those tasks remain "STARTING" for a while, until a cronjob clears all STARTING tasks when they exceed
			// the start timeout.
			_, err := s.taskRuntimeStore.SetState(storage.TaskId(t.TaskId), protocol.TASK_STATE_STARTING)
			if err == nil {
				mesosTasks = append(mesosTasks, task)
			} else {
				log.Errorf("Failed to change the state for task %v with error %v. Schedule aborted\n", task.GetName(), err)
			}
		}

		// if it fails to launch, we restore the state and put the tasks back. Note failure (ex. hardware failure) may happen
		// before running into this step, which may leave some state data inconsistent (ie. still in STARTING state). cleanState()
		// runs periodically to fix this.
		_, err := driver.LaunchTasks([]*mesos.OfferID{offer.Id}, mesosTasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		if err != nil {
			log.Errorf("Failed to launch tasks %v. Restoring...\n", mesosTasks)
			for _, t := range pendingTasks {
				s.taskRuntimeStore.SetState(storage.TaskId(t.TaskId), protocol.TASK_STATE_STAGING)
				s.tasks.Push(t)
			}
		} else {
			log.Infof("Launched %v tasks with offer %s\n", len(mesosTasks), offer.Id.GetValue())
		}
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

// compute the max number of tasks that can be launched given an offer.
// note it will modify tasks priorityQueue.
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

// it is important to notice that mesos guarantees that each status update message
// is delivered at least once, i.e., if scheduler aborts before handling the message,
// another status update message with the same content will be delivered.
func (s *TriggerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())

	switch status.GetState() {
	case mesos.TaskState_TASK_RUNNING:
		go func() {
			s.setRunning(status.GetTaskId().GetValue())
		}()

	case mesos.TaskState_TASK_FINISHED:
		go func() {
			s.setFinished(status.GetTaskId().GetValue())
			// pause cleanState() job to avoid race condition (otherwise one task may be launched twice)
			s.cleanLock.Lock()
			s.deleteOrRepeat(status.GetTaskId().GetValue()) // duplicate messages are NOT handled
			s.cleanLock.Unlock()
		}()

	case mesos.TaskState_TASK_FAILED:
		go func() {
			s.setFailed(status.GetTaskId().GetValue())
			s.cleanLock.Lock()
			s.deleteOrRetry(status.GetTaskId().GetValue()) // duplicate messages are handled properly
			s.cleanLock.Unlock()
		}()
	}
}

func (s *TriggerScheduler) setRunning(taskId string) error {
	_, err := s.taskRuntimeStore.SetState(storage.TaskId(taskId), protocol.TASK_STATE_RUNNING)
	if err != nil {
		log.Errorf("Failed to update state for task %v, %v\n", taskId, err)
	} else {
		log.Infof("Set state of task %v to RUNNING\n", taskId)
	}
	return err
}

func (s *TriggerScheduler) setFinished(taskId string) error {
	_, err := s.taskRuntimeStore.SetState(storage.TaskId(taskId), protocol.TASK_STATE_FINISHED)
	if err != nil {
		log.Errorf("Failed to update state for task %v, %v\n", taskId, err)
	} else {
		log.Infof("Set state of task %v to FINISHED\n", taskId)
	}
	return err
}

func (s *TriggerScheduler) setFailed(taskId string) error {
	rt, err := s.taskRuntimeStore.GetRuntime(storage.TaskId(taskId))
	if err != nil {
		return err
	}
	_, _, err = s.taskRuntimeStore.SetRuntime(
		storage.TaskId(taskId),
		&protocol.TaskRunTime{
			Failure:        rt.Failure + 1,
			State:          protocol.TASK_STATE_FAILED,
			LastModifiedMS: s.nowInMS(),
		},
	)
	if err != nil {
		log.Errorf("Failed to update state for task %v, %v\n", taskId, err)
		return err
	}
	return nil
}

// TODO repeating tasks haven't been supported yet
func (s *TriggerScheduler) deleteOrRepeat(taskId string) error {
	// finished tasks could be removed from storage immediately if it is not a repeating task.
	post, err := s.taskStore.Get(&protocol.Get{taskId})
	if err != nil {
		log.Errorf("Failed to load task data for %v\n", taskId)
		return err
	}

	if post.RepeatPeriod <= 0 {
		err = s.taskStore.Delete(&protocol.Delete{taskId})
		if err != nil {
			log.Errorf("Failed to delete task data %v\n", taskId)
			// do not return even err is not nil, because failure can happen at any time, so
			// it is possible for a task to only have runtime data but no store data, in which case
			// deleting a store data here may return error, but we still need to delete runtime data
		} else {
			log.Infof("Task %v finished and removed\n", taskId)
		}

		// it is important to delete from store before deleting from runtime, otherwise (if reversing the order), it may only exist
		// in store but not in runtime, which leads a never-deleted store entry.
		return s.moveToHistory(taskId)
	} else {
		// TODO handle repeating task here
		return nil
	}
}

func (s *TriggerScheduler) deleteOrRetry(taskId string) error {
	rt, err := s.taskRuntimeStore.GetRuntime(storage.TaskId(taskId))
	if err != nil {
		log.Errorf("Failed to update state for task %v, %v\n", taskId, err)
		return err
	}
	// it may happen due to duplicate status update message delivered by mesos
	if rt.State != protocol.TASK_STATE_FAILED {
		log.Infof("Task %v has already restarted, abort retry\n", taskId)
		return errors.New("task has already restarted")
	}

	post, err := s.taskStore.Get(&protocol.Get{taskId})
	if err != nil {
		log.Errorf("Failed to load task data for %v\n", taskId)
		return err
	}

	// remove task data for non-retriable tasks
	if rt.Failure >= post.MaxRetry { // including MaxRetry == 0
		return s.deleteFailedTask(taskId, rt.Failure)
	} else {
		return s.retryTask(taskId)
	}
}

func (s *TriggerScheduler) deleteFailedTask(taskId string, failure int32) error {
	err := s.taskStore.Delete(&protocol.Delete{taskId})
	if err != nil {
		log.Errorf("Failed to delete task data %v\n", taskId)
		// do not return here, for the same reason described in deleteOrRepeat()
	} else {
		log.Infof("Task %v removed due to too many failure : failure = %v\n", taskId, failure)
	}
	return s.moveToHistory(taskId)
}

func (s *TriggerScheduler) retryTask(taskId string) error {
	_, err := s.taskRuntimeStore.SetState(
		storage.TaskId(taskId),
		protocol.TASK_STATE_STAGING,
	)
	if err != nil {
		log.Errorf("Failed to retry task %v : %v\n", taskId, err)
		return err
	}

	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	post, err := s.taskStore.Get(&protocol.Get{taskId})
	if err != nil {
		log.Errorf("Failed to retry task %v : %v\n", taskId, err)
		return err
	}
	s.tasks.Push(&protocol.Update{taskId, post})
	log.Infof("Re-start task %v\n", taskId)
	return nil
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
	schedConfig := &SchedulerConfig{
		StartTimeout:   time.Second * 10,
		CleanTimeout:   time.Second * 60,
		HistoryTimeout: time.Second * 60,
	}
	scheduler := NewTriggerScheduler(zkServers, schedConfig)
	err := scheduler.Init()
	scheduler.taskStore.DeleteAll() // TODO remove this line

	if err != nil {
		log.Errorln(err)
		scheduler.Cleanup()
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
			MaxRetry:     3,
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

	}()

	go func() {
		time.Sleep(time.Second * 60)
		scheduler.Cleanup()
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
