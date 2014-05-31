package scheduler

import (
	"github.com/gorhill/cronexpr"
	"time"
)

type DagJob struct {
	Id        int64  `db:"id" json:"id"`
	DagName   string `db:"dag_name" json:"dag_name"`
	JobName   string `db:"job_name" json:"job_name"`
	ParentJob string `db:"parent" json:"parent"`
}

type DagMeta struct {
	Id            int64  `db:"id" json:"id"`
	Name          string `db:"name" json:"name"`
	Schedule      string `db:"schedule" json:"schedule"`
	CreateTs      int64  `db:"create_ts" json:"create_ts"`
	LastSuccessTs int64  `db:"last_success_ts" json:"last_success_ts"`
	LastErrTs     int64  `db:"last_error_ts" json:"last_error_ts"`
	LastErrMsg    string `db:"last_error_msg" json:"last_error_msg"`
}

func NewDag(name, schedule string) *DagMeta {
	return &DagMeta{
		Name:     name,
		Schedule: schedule,
		CreateTs: time.Now().Unix(),
	}
}

func NewDagJob(dag *DagMeta, jobName, parentJobName string) *DagJob {
	return &DagJob{
		DagName:   dag.Name,
		JobName:   jobName,
		ParentJob: parentJobName,
	}
}

func GetDagMetaList() []DagMeta {
	var dags []DagMeta
	_, err := sharedDbMap.Select(&dags, "select * from dagmeta order by create_ts desc")
	if err != nil {
		return nil
	}
	return dags
}

func (dag *DagMeta) GetDagJobs() []DagJob {
	var dagJobs []DagJob
	_, err := sharedDbMap.Select(&dagJobs, "select * from dagjobs where dag_name = ?", dag.Name)
	if err != nil {
		return nil
	}
	return dagJobs
}

func (dag *DagMeta) AutoRunSignal() (bool, <-chan *DagMeta) {
	c := make(chan *DagMeta)
	if len(dag.Schedule) <= 0 {
		return false, nil
	}

	go func() {
		for {
			now := time.Now()
			nextTime := cronexpr.MustParse(dag.Schedule).Next(now)
			dur := nextTime.Sub(now)
			select {
			case <-time.After(dur):
				{
					c <- dag
				}
			}
		}
	}()
	return true, c
}
