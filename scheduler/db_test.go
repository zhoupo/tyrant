package scheduler

import (
	"os"
	"testing"
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
		Epsilon: "1000",
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

	j2.Disabled = true
	j2.Save()

	j22, err := GetJobByName("Test")
	if err != nil || j22 == nil || j22.Disabled == false {
		t.Error("save job failed")
	}

	j22.Remove()
	j3, err := GetJobByName("Test")
	if j3 != nil {
		t.Error("remove failed")
	}
}
