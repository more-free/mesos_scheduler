package main

import (
	"github.com/gogo/protobuf/proto"
	"strconv"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/more-free/mesos_scheduler/storage"
	"net"
	"time"
)

type TriggerScheduler struct {
	storeCli storage.Storage
}

func NewScheduler() *TriggerScheduler {
	return &TriggerScheduler{
		tasksLaunched: 0,
		totalTasks:    1,
		cpuPerTask:    0.1,
		memPerTask:    16.0,
	}
}

func (s *TriggerScheduler) TriggerUpdated(t *protocol.Update) {

}

func (s *TriggerScheduler) TriggerCreated(t *protocol.Post) {

}

func (sched *TriggerScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Registered with Master ", masterInfo)
}

func (sched *TriggerScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Scheduler Re-Registered with Master ", masterInfo)
}

func (sched *TriggerScheduler) Disconnected(sched.SchedulerDriver) {
	log.Infoln("Scheduler Disconnected")
}

func (sched *TriggerScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	if sched.tasksLaunched >= sched.totalTasks {
		log.Infoln("reject offer due to max task limit")
		return
	}

	log.Infoln("ready to accept offer and launch task")

	tasks := make([]*mesos.TaskInfo, 0)
	for _, offer := range offers {
		taskId := &mesos.TaskID{
			Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
		}
		task := &mesos.TaskInfo{
			Name:    proto.String("go-task-" + taskId.GetValue()),
			TaskId:  taskId,
			SlaveId: offer.SlaveId,
			Command: &mesos.CommandInfo{
				Shell: proto.Bool(true),
				//Value: proto.String("c=1; while [ $c -le 5 ] ; do echo 'Hello Mesos' ; ((c++)); sleep 1 ; done; touch /home/vagrant/some"),
				Value: proto.String("bash /home/vagrant/some.sh"),
			},
			Resources: []*mesos.Resource{
				util.NewScalarResource("cpus", sched.cpuPerTask),
				util.NewScalarResource("mem", sched.memPerTask),
			},
		}
		log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())
		tasks = append(tasks, task)
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		sched.tasksLaunched++

		// kill after some time if it doesn't complete
		go func() {
			select {
			case <-time.After(5 * time.Second):
				driver.KillTask(taskId)
			}
			log.Infof("task gets killed")
		}()

		break
	}
}

func (sched *TriggerScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
}

func (sched *TriggerScheduler) OfferRescinded(s sched.SchedulerDriver, id *mesos.OfferID) {
	log.Infof("Offer '%v' rescinded.\n", *id)
}

func (sched *TriggerScheduler) FrameworkMessage(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, msg string) {
	log.Infof("Received framework message from executor '%v' on slave '%v': %s.\n", *exId, *slvId, msg)
}

func (sched *TriggerScheduler) SlaveLost(s sched.SchedulerDriver, id *mesos.SlaveID) {
	log.Infof("Slave '%v' lost.\n", *id)
}

func (sched *TriggerScheduler) ExecutorLost(s sched.SchedulerDriver, exId *mesos.ExecutorID, slvId *mesos.SlaveID, i int) {
	log.Infof("Executor '%v' lost on slave '%v' with exit code: %v.\n", *exId, *slvId, i)
}

func (sched *TriggerScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}

func main() {
	//master := "10.88.196.178:5050"
	master := "127.0.0.1:5050"
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""),
		Name: proto.String("Morefree Test Framework (Go)"),
	}

	config := sched.DriverConfig{
		Scheduler:      NewScheduler(),
		Framework:      fwinfo,
		Master:         master,
		Credential:     (*mesos.Credential)(nil),
		BindingAddress: parseIP("127.0.0.1"), // must use this line, why ???
	}

	driver, err := sched.NewMesosSchedulerDriver(config)
	if err != nil {
		log.Errorln("Unable to create driver", err)
	}
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
