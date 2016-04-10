package scheduler

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
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

type SchedulerStatus int8

const (
	SCHEDULER_STATUS_RUNNING = SchedulerStatus(0)
	SCHEDULER_STATUS_STOPPED = SchedulerStatus(1)
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
	quit           chan struct{} // used for quiting all go routines when Stop() is invoked
	cleanLock      *sync.Mutex
	status         SchedulerStatus
	resourceLock   *sync.Mutex // general lock
	driver         *sched.MesosSchedulerDriver
}

type SchedulerConfig struct {
	StartTimeout   time.Duration // the max duration when a task stays in "STARTING" state
	CleanTimeout   time.Duration // the max duration after a task finishes or fails. Any state data exceeds
	HistoryTimeout time.Duration // the max duration for a stat data to exist. -1 => never clean
	User           string        // user name of the framework
	Name           string        // framework (scheduler) name
	ZkServers      []string      // zookeeper servers, "host:ip"
	MesosMaster    string        // "host:ip"
}

func NewTriggerScheduler(config *SchedulerConfig) *TriggerScheduler {
	return &TriggerScheduler{
		taskStore: storage.NewZkStorage(config.ZkServers, "/trigger-scheduler/task"),
		taskRuntimeStore: storage.NewZkRuntimeStore(
			storage.NewZkStorage(config.ZkServers, "/trigger-scheduler/state").(*storage.ZkStorage),
		),
		taskStatStore: storage.NewZkRuntimeStore(
			storage.NewZkStorage(config.ZkServers, "/trigger-scheduler/stat").(*storage.ZkStorage),
		),
		tasks:          trigger.NewPostPriorityQueue(),
		taskLock:       &sync.Mutex{},
		config:         config,
		cleanStateQuit: make(chan struct{}),
		cleanLock:      &sync.Mutex{},
		status:         SCHEDULER_STATUS_STOPPED,
		resourceLock:   &sync.Mutex{},
		quit:           make(chan struct{}),
	}
}

// Start the scheduler. It will block unless a non-nil error returned.
// it is the caller's responsibility to call Stop() method to release resources.
func (s *TriggerScheduler) Start() error {
	if err := trigger.Cascade(
		s.taskStore.Open(),
		s.taskRuntimeStore.Open(),
		s.taskStatStore.Open(),
		s.initDriver()); err != nil {
		return err
	}

	go s.cleanState()
	go s.captureInterrupt()

	s.resourceLock.Lock()
	s.setStatus(SCHEDULER_STATUS_RUNNING)
	s.resourceLock.Unlock()

	// will block. driver.Abort() will cause a normal return (nil error) for driver.Run()
	if stat, err := s.driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s", stat.String(), err.Error())
		return err
	}
	return nil
}

func (s *TriggerScheduler) initDriver() error {
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(s.config.User),
		Name: proto.String(s.config.Name),
	}

	driverConf := sched.DriverConfig{
		Scheduler:  s,
		Framework:  fwinfo,
		Master:     s.config.MesosMaster,
		Credential: (*mesos.Credential)(nil),
		//BindingAddress: parseIP("127.0.0.1"),
	}

	driver, err := sched.NewMesosSchedulerDriver(driverConf)
	if err != nil {
		log.Errorln("Unable to create driver", err)
		return err
	}
	s.driver = driver
	return nil
}

func (s *TriggerScheduler) Stop() error {
	s.resourceLock.Lock()
	defer s.resourceLock.Unlock()

	if s.getStatus() == SCHEDULER_STATUS_STOPPED {
		return nil
	}

	close(s.cleanStateQuit)
	close(s.quit)

	abort := func() error {
		if stat, err := s.driver.Abort(); err != nil {
			log.Errorf("Framework stopped with status %s and error: %s", stat.String(), err.Error())
			return err
		} else {
			return nil
		}
	}

	if err := trigger.Cascade(
		s.taskStore.Close(),
		s.taskRuntimeStore.Close(),
		s.taskStatStore.Close(),
		abort()); err != nil {
		// maybe change to another status here
		return err
	} else {
		s.setStatus(SCHEDULER_STATUS_STOPPED)
		return nil
	}
}

// it requires "go build ..." to take effect, because "go run ..." will still
// receive the signal (ex. from CTRL-C)
func (s *TriggerScheduler) captureInterrupt() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	signal.Notify(ch, syscall.SIGTERM)

	select {
	case <-s.quit:
	case <-ch:
		log.Infoln("Interruption received. Pre-quit cleanup for scheduler...")
		s.Stop()

		// restore the signals for following interruption, works for go 1.5 or above
		//signal.Reset(os.Interrupt)
		//signal.Reset(syscall.SIGTERM)

		// for go 1.4 or below
		signal.Stop(ch)
	}
}

// clear all storage data. Use carefully.
func (s *TriggerScheduler) Clear() error {
	return trigger.Cascade(
		s.taskStore.DeleteAll(),
		s.taskRuntimeStore.DeleteAll(),
		s.taskStatStore.DeleteAll(),
	)
}

func (s *TriggerScheduler) CreateTrigger(newTask *protocol.Post) (*protocol.Get, error) {
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

func (s *TriggerScheduler) UpdateTrigger(updatedTask *protocol.Update) error {
	err := s.taskStore.Update(updatedTask)
	if err != nil {
		return err
	}
	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	s.tasks.Update(updatedTask)
	return nil
}

func (s *TriggerScheduler) DeleteTrigger(task *protocol.Delete) error {
	// eventually runtime data and history data will be deleted by cleanState() routine
	if err := s.taskStore.Delete(task); err != nil {
		return err
	}

	s.taskLock.Lock()
	defer s.taskLock.Unlock()
	s.tasks.Delete(task)
	return nil
}

// return deleted tasks
func (s *TriggerScheduler) DeleteBy(filter func(*protocol.Update) bool) ([]*protocol.Update, error) {
	deletedTasks := make([]*protocol.Update, 0)
	tasks, err := s.taskStore.GetAll()
	if err != nil {
		return deletedTasks, err
	}

	for _, t := range tasks {
		if filter(t) {
			if err := s.taskStore.Delete(t.ToDelete()); err == nil {
				s.taskLock.Lock()
				s.tasks.Delete(t.ToDelete())
				s.taskLock.Unlock()
				deletedTasks = append(deletedTasks, t)
			} else {
				return deletedTasks, err
			}
		}
	}
	return deletedTasks, nil
}

func (s *TriggerScheduler) DeleteAll() ([]*protocol.Update, error) {
	return s.DeleteBy(func(task *protocol.Update) bool { return true })
}

func (s *TriggerScheduler) GetTrigger(task *protocol.Get) (*protocol.Post, error) {
	return s.taskStore.Get(task)
}

func (s *TriggerScheduler) GetBy(filter func(*protocol.Update) bool) ([]*protocol.Update, error) {
	filteredTasks := make([]*protocol.Update, 0)
	tasks, err := s.taskStore.GetAll()
	if err != nil {
		return filteredTasks, err
	}

	for _, t := range tasks {
		if filter(t) {
			filteredTasks = append(filteredTasks, t)
		}
	}
	return filteredTasks, nil
}

func (s *TriggerScheduler) GetAll() ([]*protocol.Update, error) {
	return s.GetBy(func(task *protocol.Update) bool { return true })
}

// Get all runtime data, including current data and historical data, sorted by last modified time.
// right now at most 1 history version is kept for each task
func (s *TriggerScheduler) GetStat(task *protocol.Get) (protocol.TaskRunTimeList, error) {
	id := storage.TaskId(task.TaskId)
	var rtrs protocol.TaskRunTimeList = make([]*protocol.TaskRunTime, 0)
	rt, err := s.taskRuntimeStore.GetRuntime(id)
	if err != nil {
		return rtrs, err
	} else {
		rtrs = append(rtrs, rt)
	}

	his, err := s.taskStatStore.GetRuntime(id)
	if err != nil {
		return rtrs, err
	} else {
		rtrs = append(rtrs, his)
	}

	sort.Sort(rtrs)
	return rtrs, nil
}

func (s *TriggerScheduler) GetAllStat() (protocol.TaskRunTimeList, error) {
	ids, err := s.taskRuntimeStore.GetAllId()
	var rtrs protocol.TaskRunTimeList = make([]*protocol.TaskRunTime, 0)
	if err != nil {
		return rtrs, err
	}

	for _, id := range ids {
		rt, err := s.taskRuntimeStore.GetRuntime(id)
		if err != nil {
			return rtrs, err
		} else {
			// append meta data from task store
			if post, err := s.taskStore.Get(&protocol.Get{string(id)}); err == nil {
				rt = rt.WithPost(post)
				rtrs = append(rtrs, rt)
			} else {
				return rtrs, err
			}
		}
	}

	ids, err = s.taskStatStore.GetAllId()
	if err != nil {
		return rtrs, err
	}

	for _, id := range ids {
		rt, err := s.taskStatStore.GetRuntime(id)
		if err != nil {
			return rtrs, err
		} else {
			rtrs = append(rtrs, rt)
		}
	}

	sort.Sort(rtrs)
	return rtrs, nil
}

// framework specific callbacks below
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

func (s *TriggerScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	mesosTasks := make([]*mesos.TaskInfo, 0)
	for _, offer := range offers {
		pendingTasks := s.prepareTask(offer)
		for _, t := range pendingTasks {
			var task *mesos.TaskInfo
			switch t.CmdType {
			case protocol.CMD_DOCKER:
				task = s.asMesosDockerTask(t)
			case protocol.CMD_SHELL:
				task = s.asMesosShellTask(t)
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

			// note this may also be a failed-restarting task, so it should not overwrite the Failure field
			var failure int32
			if rt, err := s.taskRuntimeStore.GetRuntime(storage.TaskId(t.TaskId)); err == nil {
				failure = rt.Failure
			}

			_, _, err := s.taskRuntimeStore.SetRuntime(
				storage.TaskId(t.TaskId),
				&protocol.TaskRunTime{
					Failure:     failure,
					State:       protocol.TASK_STATE_STARTING,
					Host:        *offer.Hostname, // ip
					PortMapping: t.PortMapping,
				},
			)
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
			log.Errorf("Failed to launch tasks %v due to error : %v. Restoring...\n", mesosTasks, err)
			for _, t := range pendingTasks {
				s.taskRuntimeStore.SetState(storage.TaskId(t.TaskId), protocol.TASK_STATE_STAGING)
				s.tasks.Push(t)
			}
		} else {
			log.Infof("Launched %v tasks with offer %s\n", len(mesosTasks), offer.Id.GetValue())
		}
	}
}

// it is important to notice that mesos guarantees that each status update message
// is delivered at least once, i.e., if scheduler aborts before handling the message,
// another status update message with the same content will be delivered.
func (s *TriggerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.Message != nil {
		log.Infoln("Status update message : ", *status.Message)
	}

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

func (s *TriggerScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
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
	// also need to recover from runtime store : for finished but repeatable tasks or failed but retriable tasks,
	// re-load them into memorys
	s.scanFinishedFailedTasks()

	return nil
}

// compute the max number of tasks that can be launched given an offer.
// note it will modify tasks priorityQueue.
func (s *TriggerScheduler) prepareTask(offer *mesos.Offer) []*protocol.Update {
	pendingTasks := make([]*protocol.Update, 0)

	var cpus, mem, disk float64
	var ports []*mesos.Value_Range
	for _, resource := range offer.Resources {
		switch resource.GetName() {
		case "cpus":
			cpus = *resource.GetScalar().Value
		case "mem":
			mem = *resource.GetScalar().Value
		case "disk":
			disk = *resource.GetScalar().Value
		case "ports":
			ports = trigger.CloneRange(resource.GetRanges().GetRange())
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

		if cpus >= next.Cpu && mem >= next.Mem && disk >= next.Disk &&
			s.isPortAvailable(next.PortMapping, ports) {
			cpus -= next.Cpu
			mem -= next.Mem
			disk -= next.Disk
			ports = s.assignPorts(ports, next.PortMapping)
			pendingTasks = append(pendingTasks, next)
		}

		if cpus == 0.0 || mem == 0.0 || disk == 0.0 {
			break
		}
	}

	// add back unlaunched tasks to s.tasks
	for _, task := range left {
		s.tasks.Push(task)
	}

	return pendingTasks
}

// convert internal task struct (protocol.Post) to mesos.TaskInfo
func (s *TriggerScheduler) asMesosShellTask(task *protocol.Update) *mesos.TaskInfo {
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

func (s *TriggerScheduler) asMesosDockerTask(task *protocol.Update) *mesos.TaskInfo {
	containerType := mesos.ContainerInfo_DOCKER // const, must assign to a var to pass by reference
	// mesos starts this task via :
	// docker run [Container.Docker.Image] [Command.Value] [Command.Arguments]
	// where Command.Value is the name of the executable binary.
	// ex. docker run ubuntu /bin/echo hello
	// if the image already has default entrypoint, Command.Value field can be omitted.
	// see an example here :  https://github.com/derekchiang/Mesos-Bitcoin-Miner/blob/master/scheduler.go#L147
	// it is also possible to specify options for "docker run" command, one just needs to set
	// Container.Docker.Parameters field.

	// test only, remove later TODO
	envName := "MESOS_TASK_ID"
	envValue := task.TaskId

	return s.withPort(
		task.PortMapping,
		&mesos.TaskInfo{
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
				Environment: &mesos.Environment{
					Variables: []*mesos.Environment_Variable{
						&mesos.Environment_Variable{
							Name:  &envName,
							Value: &envValue,
						},
					}},
			},
			Resources: []*mesos.Resource{
				util.NewScalarResource("cpus", task.Cpu),
				util.NewScalarResource("mem", task.Mem),
			}})
}

func (s *TriggerScheduler) withPort(taskPortMapping []*protocol.PortMapping, taskInfo *mesos.TaskInfo) *mesos.TaskInfo {
	if len(taskPortMapping) > 0 {
		// port mapping requires bridge network mode and port resource
		bridgeNetwork := mesos.ContainerInfo_DockerInfo_BRIDGE
		taskInfo.Container.Docker.Network = &bridgeNetwork
		taskInfo.Container.Docker.PortMappings = s.asMesosPortMapping(taskPortMapping)
		taskInfo.Resources = append(taskInfo.Resources, s.asRangeResource(taskPortMapping))
	}
	return taskInfo
}

func (s *TriggerScheduler) asRangeResource(taskPortMapping []*protocol.PortMapping) *mesos.Resource {
	ranges := make([]*mesos.Value_Range, len(taskPortMapping))
	for i := 0; i < len(taskPortMapping); i++ {
		// here can be optimized
		ranges[i] = util.NewValueRange(uint64(taskPortMapping[i].HostPort), uint64(taskPortMapping[i].HostPort))
	}
	return util.NewRangesResource("ports", ranges)
}

func (s *TriggerScheduler) asMesosPortMapping(portMapping []*protocol.PortMapping) []*mesos.ContainerInfo_DockerInfo_PortMapping {
	mesosPorts := make([]*mesos.ContainerInfo_DockerInfo_PortMapping, len(portMapping))
	for i := 0; i < len(mesosPorts); i++ {
		mesosPorts[i] = &mesos.ContainerInfo_DockerInfo_PortMapping{
			HostPort:      &portMapping[i].HostPort,
			ContainerPort: &portMapping[i].ContainerPort,
			Protocol:      &portMapping[i].Protocol,
		}
	}
	return mesosPorts
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
		return s.moveToHistory(taskId, post)
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
	if post.MaxRetry < 0 || rt.Failure < post.MaxRetry { // negative MaxRetry means infinite retry
		return s.retryTask(taskId)
	} else {
		return s.deleteFailedTask(taskId, rt.Failure, post)
	}
}

func (s *TriggerScheduler) deleteFailedTask(taskId string, failure int32, post *protocol.Post) error {
	err := s.taskStore.Delete(&protocol.Delete{taskId})
	if err != nil {
		log.Errorf("Failed to delete task data %v\n", taskId)
		// do not return here, for the same reason described in deleteOrRepeat()
	} else {
		log.Infof("Task %v removed due to too many failure : failure = %v\n", taskId, failure)
	}
	return s.moveToHistory(taskId, post)
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

func (s *TriggerScheduler) moveToHistory(id string, post *protocol.Post) error {
	rt, err := s.taskRuntimeStore.GetRuntime(storage.TaskId(id))
	if err != nil {
		log.Errorf("Failed to move to history %v\n", err)
		return err
	}

	if rt.State == protocol.TASK_STATE_FINISHED || rt.State == protocol.TASK_STATE_FAILED {
		// append meta data (*Post) to runtime data and move to history
		_, _, err = s.taskStatStore.SetRuntime(storage.TaskId(id), rt.WithPost(post))
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

// a cron job runs periodically
func (s *TriggerScheduler) cleanState() {
	run := func(ch chan<- bool, timeout time.Duration, quit <-chan struct{}) {
		if timeout < 0 {
			ch <- true
			return
		}

		for {
			select {
			case <-quit:
				return
			case <-time.After(timeout):
				ch <- false
			}
		}
	}

	startChan := make(chan bool)
	cleanChan := make(chan bool)
	historyChan := make(chan bool)

	go run(startChan, s.config.StartTimeout, s.cleanStateQuit)
	go run(cleanChan, s.config.CleanTimeout, s.cleanStateQuit)
	go run(historyChan, s.config.HistoryTimeout, s.cleanStateQuit)

	for {
		select {
		case done := <-startChan:
			if !done {
				go s.scanStartingTasks()
			}

		case done := <-cleanChan:
			// do we really need this ?
			if !done {
				go s.scanFinishedFailedTasks()
			}

		case done := <-historyChan:
			if !done {
				go s.scanHistoryStore()
			}

		case <-s.cleanStateQuit:
			log.Infoln("State cleaner quit")
			return
		}
	}
}

func (s *TriggerScheduler) scanStartingTasks() {
	ids, _ := s.taskRuntimeStore.GetAllId()
	for _, id := range ids {
		rt, err := s.taskRuntimeStore.GetRuntime(id)
		if err != nil && rt.State == protocol.TASK_STATE_STARTING &&
			s.overTimeNow(rt.LastModifiedMS, s.config.StartTimeout) {
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
}

func (s *TriggerScheduler) scanFinishedFailedTasks() {
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
}

func (s *TriggerScheduler) scanHistoryStore() {
	ids, _ := s.taskStatStore.GetAllId()
	for _, id := range ids {
		rt, err := s.taskStatStore.GetRuntime(id)
		if err == nil && s.overTimeNow(rt.LastModifiedMS, s.config.HistoryTimeout) {
			err = s.taskStatStore.Delete(id)
			if err != nil {
				log.Errorf("Failed to delete task %v from history data store : %v\n", id, err)
			} else {
				log.Infof("Deleted task %v from history data store\n", id)
			}
		}
	}
}

func (s *TriggerScheduler) isPortAvailable(portMapping []*protocol.PortMapping, portsAvailable []*mesos.Value_Range) bool {
	totalAvailable := uint64(0)
	for _, pa := range portsAvailable {
		totalAvailable += *pa.End - *pa.Begin + 1
	}

	for _, pm := range portMapping {
		if pm.HostPort == 0 {
			continue
		}

		ok := false
		for _, rn := range portsAvailable {
			if uint64(pm.HostPort) >= *rn.Begin && uint64(pm.HostPort) <= *rn.End {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return uint64(len(portMapping)) <= totalAvailable // ensuring enough ports left for dynamic ports (0)
}

// assignPorts assign ports according to the requested ports and ports available.
// if it requests a dymanic port (0), then the original portsRequested will be modified by assigning
// a non-zero port.
func (s *TriggerScheduler) assignPorts(portsAvailable []*mesos.Value_Range, portsRequested []*protocol.PortMapping) []*mesos.Value_Range {
	if len(portsRequested) == 0 {
		return portsAvailable
	}

	// this brute force process can be optimized
	for _, pr := range portsRequested {
		port := uint64(pr.HostPort)
		if port == 0 { // simply assign the first port available to dynamic port
			port = *portsAvailable[0].Begin
			pr.HostPort = uint32(port) // modify the original 0 to a non-zero value
		}

		splits := make([]*mesos.Value_Range, 2)
		removal := -1
		for i, available := range portsAvailable {
			if port <= *available.End && port >= *available.Begin {
				low, high := port-1, port+1
				splits[0] = &mesos.Value_Range{
					Begin: available.Begin,
					End:   &low,
				}
				splits[1] = &mesos.Value_Range{
					Begin: &high,
					End:   available.End,
				}
				removal = i
				break
			}
		}

		if removal >= 0 {
			portsAvailable = append(portsAvailable[:removal], portsAvailable[removal+1:]...) // remove portsAvailable[removal]
			for _, split := range splits {
				if *split.Begin <= *split.End {
					portsAvailable = append(portsAvailable, split)
				}
			}
		}
	}
	return portsAvailable
}

func (s *TriggerScheduler) overTimeNow(lastModifiedMS int64, timeout time.Duration) bool {
	return s.nowInMS()-lastModifiedMS >= s.nanoToMS(timeout.Nanoseconds())
}

func (s *TriggerScheduler) isTriggerable(t *protocol.Update) bool {
	return t.StartTime <= time.Now().Unix()
}

func (s *TriggerScheduler) nowInMS() int64 {
	return time.Now().UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func (s *TriggerScheduler) nanoToMS(nano int64) int64 {
	return nano / (int64(time.Millisecond) / int64(time.Nanosecond))
}

func (s *TriggerScheduler) parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}

func (s *TriggerScheduler) getStatus() SchedulerStatus {
	return s.status
}

// return the old status
func (s *TriggerScheduler) setStatus(newStatus SchedulerStatus) SchedulerStatus {
	oldStatus := s.status
	s.status = newStatus
	return oldStatus
}

func (s *TriggerScheduler) errToStr(err error) string {
	if err == nil {
		return ""
	} else {
		return err.Error()
	}
}
