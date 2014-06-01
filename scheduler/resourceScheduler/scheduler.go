package resourceScheduler

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"code.google.com/p/goprotobuf/proto"
	"github.com/ngaut/tyrant/scheduler"
	"mesos.apache.org/mesos"
)

type ResMan struct {
	s    *scheduler.TaskScheduler
	exit chan bool
}

func (self *ResMan) Run() {
	taskLimit := 5
	taskId := 0
	exit := make(chan bool)
	localExecutor, _ := executorPath()

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	executorUri := flag.String("executor-uri", localExecutor, "URI of executor executable")
	flag.Parse()

	executor := &mesos.ExecutorInfo{
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
			ResourceOffers: func(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
				println("ResourceOffers")
				for _, offer := range offers {
					driver.DeclineOffer(offer.Id)
					continue

					td := self.s.GetReadyDag()
					if td == nil {
						driver.DeclineOffer(offer.Id)
						return
					}

					ts := td.GetReadyTask()
					if len(ts) == 0 {
						driver.DeclineOffer(offer.Id)
						return
					}

					taskId++
					fmt.Printf("Launching task: %d, name:%s\n", taskId, ts[0].Name)
					job, err := scheduler.GetJobByName(ts[0].Name)
					if err != nil {
						fmt.Println(err)
						driver.DeclineOffer(offer.Id)
						return
					}

					//todo: set dag state to running
					executor.Command.Value = proto.String(job.Command)

					tasks := []mesos.TaskInfo{
						mesos.TaskInfo{
							Name: proto.String("go-task"),
							TaskId: &mesos.TaskID{
								Value: proto.String("go-task-" + strconv.Itoa(taskId)),
							},
							SlaveId:  offer.SlaveId,
							Executor: executor,
							Resources: []*mesos.Resource{
								mesos.ScalarResource("cpus", 1),
								mesos.ScalarResource("mem", 512),
							},
						},
					}

					driver.LaunchTasks(offer.Id, tasks)
				}
			},

			StatusUpdate: func(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
				fmt.Println("Received task status: " + *status.Message)

				if *status.State == mesos.TaskState_TASK_FINISHED {
					taskLimit--
					if taskLimit <= 0 {
						exit <- true
					}
				}
			},

			Error: func(driver *mesos.SchedulerDriver, err string) {
				fmt.Printf("%s\n", err)
			},

			Disconnected: func(driver *mesos.SchedulerDriver) {
				fmt.Print("Disconnected")
			},
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
