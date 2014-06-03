package scheduler

import (
	"fmt"
	"time"

	"github.com/gorhill/cronexpr"
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

func NewDagMeta(name, schedule string) *DagMeta {
	return &DagMeta{
		Name:     name,
		Schedule: schedule,
		CreateTs: time.Now().Unix(),
	}
}

func NewDagJob(jobName, parentJobName string) *DagJob {
	return &DagJob{
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

func GetDagFromName(name string) *DagMeta {
	var dag DagMeta
	err := sharedDbMap.SelectOne(&dag, "select * from dagmeta where name=?", name)
	if err != nil {
		return nil
	}
	return &dag
}

func (j *DagJob) Save() error {
	if j.Id <= 0 {
		return sharedDbMap.Insert(j)
	} else {
		_, err := sharedDbMap.Update(j)
		return err
	}
}

func (j *DagJob) Remove() {
	sharedDbMap.Delete(j)
}

func (j DagJob) String() string {
	return fmt.Sprintf("DagJob{DagName:%s, Id:%d, JobName:%s, ParentJob:%s}",
		j.DagName, j.Id, j.JobName, j.ParentJob)
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

func (d *DagMeta) AddDagJob(j *DagJob) error {
	j.DagName = d.Name
	return sharedDbMap.Insert(j)
}

func (d *DagMeta) Save() error {
	if d.Id <= 0 {
		return sharedDbMap.Insert(d)
	} else {
		_, err := sharedDbMap.Update(d)
		return err
	}
}

func (d *DagMeta) Remove() error {
	if d.Id > 0 {
		for _, j := range d.GetDagJobs() {
			(&j).Remove()
		}
		cnt, err := sharedDbMap.Delete(d)
		if cnt == 1 && err == nil {
			d.Id = -1
			return nil
		}
		return err
	}
	d.Id = -1
	return nil
}
