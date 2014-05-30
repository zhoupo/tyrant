package tyrant

import (
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

type Job struct {
	Id            int64  `db:"id", json:"id"`
	Name          string `db:"name", json:"name"`       // 512, unique
	Command       string `db:"command", json:"command"` // 4096
	Epsilon       string `db:"epsilon, json:"epsilon`
	Executor      string `db:"executor", json:"executor"`             // 4096
	ExecutorFlags string `db:"executor_flags", json:"executor_flags"` // 4096
	Retries       int    `db:"retries", json:"retries"`
	Owner         string `db:"owner", json:"owner"`
	Async         bool   `db:"async", json:"async"`
	SuccessCnt    int    `db:"success_cnt", json:"success_cnt"`
	ErrCnt        int    `db:"error_cnt", json:"error_cnt"`
	CreateTs      int64  `db:"create_ts", json:"create_ts"`
	LastSuccess   int64  `db:"last_success", json:"last_success"`
	LastErr       int64  `db:"last_error", json:"last_error"`
	Cpus          int    `db:"cpus", json:"cpus"`
	Mem           int    `db:"mem", json:"mem"`
	Disk          int64  `db:"disk", json:"disk"`
	Disabled      bool   `db:"disabled", json:"disabled"`
	Uris          string `db:"uris", json:"uris"`         // 2048
	Schedule      string `db:"schedule", json:"schedule"` // 255, crontab expr
	Parents       string `db:"parents", json:"parents"`   // 4096
}

func GetJobList() []Job {
	var jobs []Job
	_, err := sharedDbMap.Select(&jobs, "select * from jobs order by create_ts desc")
	if err != nil {
		return nil
	}
	return jobs
}

func GetJobByName(name string) (*Job, error) {
	var job Job
	err := sharedDbMap.SelectOne(&job, "select * from jobs where name=?", name)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func GetJobById(id int) (*Job, error) {
	var job Job
	err := sharedDbMap.SelectOne(&job, "select * from jobs where id=?", id)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (j *Job) AutoRunSignal() (bool, <-chan *Job) {
	c := make(chan *Job)
	if len(j.Schedule) <= 0 {
		return false, nil
	}

	go func() {
		for {
			now := time.Now()
			nextTime := cronexpr.MustParse(j.Schedule).Next(now)
			dur := nextTime.Sub(now)
			select {
			case <-time.After(dur):
				{
					c <- j
				}
			}
		}
	}()

	return true, c
}

func (j *Job) Disable(b bool) error {
	j.Disabled = b
	_, err := sharedDbMap.Update(j)
	return err
}

func (j *Job) GetParentJobs() []*Job {
	var jobs []*Job
	parentNames := strings.Split(j.Parents, ",")
	for _, name := range parentNames {
		name := strings.Trim(name, " \t")
		job, err := GetJobByName(name)
		if err == nil && job.Disabled == false {
			jobs = append(jobs, job)
		}
	}
	return jobs
}
