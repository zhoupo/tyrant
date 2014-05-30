package scheduler

import (
	"log"
	"os"
	"testing"
	"time"
)

func init() {
	InitConfig("../config.ini")
}

func TestDbMap(t *testing.T) {
	InitSharedDbMap()
	defer func() {
		os.Remove("/tmp/test.db")
	}()
	j := &Job{
		Name:    "Test",
		Command: "ls",
		Epsilon: "fuck",
	}

	j2 := Job{}

	err := sharedDbMap.Insert(j)
	if err != nil {
		t.Error(err)
	}

	err = sharedDbMap.SelectOne(&j2, "select * from jobs where name = ?", j.Name)
	if err != nil {
		t.Error(err)
	}

	_, err = GetJobByName("Test")
	if err != nil {
		t.Error(err)
	}
}

func TestJobDependency(t *testing.T) {
	InitSharedDbMap()
	defer func() {
		os.Remove("/tmp/test.db")
	}()

	j1 := &Job{Name: "j1"}
	j2 := &Job{Name: "j2", Parents: "j1"}
	j3 := &Job{Name: "j3", Parents: "j1"}
	j4 := &Job{Name: "j4", Parents: "j2,j3"}

	sharedDbMap.Insert(j1)
	sharedDbMap.Insert(j2)
	sharedDbMap.Insert(j3)
	sharedDbMap.Insert(j4)

	parents := j4.GetParentJobs()
	if parents == nil || len(parents) != 2 {
		t.Error("get parents error")
	} else {
		log.Println(parents[0].Name, parents[1].Name)
	}

	err := j2.Disable(true)
	if err != nil {
		t.Error(err)
	}

	parents = j4.GetParentJobs()
	if parents == nil || len(parents) != 1 {
		t.Error("get parents error")
	} else {
		log.Println(parents[0].Name)
	}

	parents = j1.GetParentJobs()
	if parents != nil {
		t.Error("j1 have no parents")
	}

}

func TestAutoRun(t *testing.T) {
	j := &Job{
		Name:     "TestJob",
		Schedule: "1 * * * * * *", // run job every minute
	}
	b, c := j.AutoRunSignal()
	if b {
	L:
		for {
			select {
			case <-c:
				{
					break L
				}
			case <-time.After(time.Minute * 2):
				{
					t.Error("time out for auto trigger")
				}
			}
		}
	}
}
