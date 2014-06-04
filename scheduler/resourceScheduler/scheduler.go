package resourceScheduler

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	log "github.com/ngaut/logging"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/scheduler"
	"mesos.apache.org/mesos"
)

type ResMan struct {
	s        *scheduler.TaskScheduler
	executor *mesos.ExecutorInfo
	exit     chan bool
	taskId   int
}

func NewResMan() *ResMan {
	return &ResMan{s: scheduler.NewTaskScheduler(), exit: make(chan bool)}
}

type TyrantTaskId struct {
	DagName  string
	TaskName string
}

func genTaskId(dagName string, taskName string) string {
	if str, err := json.Marshal(TyrantTaskId{DagName: dagName, TaskName: taskName}); err != nil {
		log.Fatal(err)
	} else {
		return string(str)
	}

	return ""
}

func splitTrim(s string) []string {
	tmp := strings.Split(s, ",")
	ss := make([]string, 0)
	for _, v := range tmp {
		if x := strings.Trim(v, " "); len(x) > 0 {
			ss = append(ss, x)
		}
	}

	return ss
}

func (self *ResMan) OnResourceOffers(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
	log.Debug("ResourceOffers")
	self.s.Refresh()
	for _, offer := range offers {
		td := self.s.GetReadyDag()
		if td == nil {
			log.Debug("no ready dag")
			driver.DeclineOffer(offer.Id)
			return
		}
		log.Debugf("got ready dag: %+v", td)
		td.Dag.ExportDot(td.DagName + ".dot")
		ts := td.GetReadyTask() //todo:make sure schedule time is match
		if len(ts) == 0 {
			driver.DeclineOffer(offer.Id)
			return
		}

		log.Debugf("%+v", ts)

		//todo:check if schedule time is match
		self.taskId++
		log.Debugf("Launching task: %d, name:%s\n", self.taskId, ts[0].Name)
		job, err := scheduler.GetJobByName(ts[0].Name)
		if err != nil {
			log.Error(err)
			driver.DeclineOffer(offer.Id)
			return
		}

		//todo: set dag state to running
		self.executor.Command.Value = proto.String("./example_executor")
		self.executor.ExecutorId = &mesos.ExecutorID{Value: proto.String("tyrantExecutorId_" + strconv.Itoa(self.taskId) + strconv.Itoa(time.Now().Day()))}
		log.Debug(job.Command, *self.executor.Command.Value)

		urls := splitTrim(job.Uris)
		taskUris := make([]*mesos.CommandInfo_URI, len(urls))
		for i, _ := range urls {
			taskUris[i] = &mesos.CommandInfo_URI{Value: &urls[i]}
		}

		tasks := []mesos.TaskInfo{
			mesos.TaskInfo{
				Name: proto.String("go-task"),
				TaskId: &mesos.TaskID{
					Value: proto.String(genTaskId(td.DagName, ts[0].Name)),
				},
				SlaveId: offer.SlaveId,
				//Executor: self.executor,
				Command: &mesos.CommandInfo{
					Value: proto.String(job.Command),
					Uris:  taskUris,
				},
				Resources: []*mesos.Resource{
					mesos.ScalarResource("cpus", 1),
					mesos.ScalarResource("mem", 512),
				},
			},
		}

		self.s.SetTaskDagStateRunning(td.DagName)

		driver.LaunchTasks(offer.Id, tasks)
	}
}

func (self *ResMan) OnStatusUpdate(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
	taskId := *status.TaskId
	log.Debugf("Received task status: %+v", status)
	switch *status.State {
	case mesos.TaskState_TASK_FINISHED:
		var ti TyrantTaskId
		err := json.Unmarshal([]byte(*taskId.Value), &ti)
		if err != nil {
			log.Fatal(err)
		}
		td := self.s.GetTaskDag(ti.DagName)
		td.RemoveTask(ti.TaskName)
		if td.Dag.Empty() {
			log.Debugf("task in dag %s is empty", td.DagName)
			self.s.RemoveTaskDag(td.DagName)
			return
		}

		self.s.SetTaskDagStateReady(ti.DagName)
		//todo:remove from task
	case mesos.TaskState_TASK_FAILED:
		//todo: retry
	case mesos.TaskState_TASK_KILLED:
		//todo:
	case mesos.TaskState_TASK_LOST:
		//todo:
	case mesos.TaskState_TASK_STAGING:
		//todo: update something
	case mesos.TaskState_TASK_STARTING:
		//todo:update something
	case mesos.TaskState_TASK_RUNNING:
		//todo:update something
	default:
		panic("should never happend")
	}
}

func (self *ResMan) OnError(driver *mesos.SchedulerDriver, err string) {
	log.Errorf("%s\n", err)
}

func (self *ResMan) OnDisconnected(driver *mesos.SchedulerDriver) {
	log.Warning("Disconnected")
}

func (self *ResMan) Run() {
	localExecutor, _ := executorPath()
	log.Debug(localExecutor)

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	executorUri := flag.String("executor-uri", localExecutor, "URI of executor executable")
	flag.Parse()

	self.executor = &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
		Command: &mesos.CommandInfo{
			Value: proto.String("./example_executor"),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{Value: executorUri},
			},
		},
		Name:   proto.String("Test Executor (Go)"),
		Source: proto.String("go_test"),
	}

	driver := mesos.SchedulerDriver{
		Master: *master,
		Framework: mesos.FrameworkInfo{
			Name: proto.String("GoFramework"),
			User: proto.String(""),
		},

		Scheduler: &mesos.Scheduler{
			ResourceOffers: self.OnResourceOffers,
			StatusUpdate:   self.OnStatusUpdate,
			Error:          self.OnError,
			Disconnected:   self.OnDisconnected,
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Start()
	<-self.exit
	driver.Stop(false)
}

func executorPath() (string, error) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return "", err
	}

	path := dir + "/example_executor"
	return path, nil
}
