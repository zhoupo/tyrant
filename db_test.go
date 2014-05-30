package tyrant

import (
	"os"
	"testing"
	"time"
)

func TestDbMap(t *testing.T) {
	dbMap := NewDbMap()
	defer func() {
		os.Remove("/tmp/test.db")
	}()
	j := &Job{
		Name:    "Test",
		Command: "ls",
		Epsilon: "fuck",
	}

	j2 := Job{}

	err := dbMap.Insert(j)
	if err != nil {
		t.Error(err)
	}

	err = dbMap.SelectOne(&j2, "select * from jobs where name = ?", j.Name)
	if err != nil {
		t.Error(err)
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
