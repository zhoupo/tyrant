package tyrant

import (
	"github.com/gorhill/cronexpr"
	log "github.com/ngaut/logging"
	"strings"
	"time"
)

type Job struct {
	Id            int64  `db:"id"`
	Name          string `db:"name"`    // 512, unique
	Command       string `db:"command"` // 4096
	Epsilon       string `db:"epsilon"`
	Executor      string `db:"executor"`       // 4096
	ExecutorFlags string `db:"executor_flags"` // 4096
	Retries       int    `db:"retries"`
	Owner         string `db:"owner"`
	Async         bool   `db:"async"`
	SuccessCnt    int    `db:"success_cnt"`
	ErrCnt        int    `db:"error_cnt"`
	CreateTs      int64  `db:"create_ts"`
	LastSuccess   int64  `db:"last_success"`
	LastErr       int64  `db:"last_error"`
	Cpus          int    `db:"cpus"`
	Mem           int    `db:"mem"`
	Disk          int64  `db:"disk"`
	Disabled      bool   `db:"disabled"`
	Uris          string `db:"uris"`     // 2048
	Schedule      string `db:"schedule"` // 255, crontab expr
	Parents       string `db:"parents"`  // 4096
}

func GetJobList() []Job {
	var jobs []Job
	_, err := sharedDbMap.Select(&jobs, "select * from jobs where disabled = FALSE order by create_ts desc")
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
	var job *Job
	err := sharedDbMap.SelectOne(&job, "select * from jobs where id=?", id)
	if err != nil {
		return nil, err
	}
	return job, nil
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

func (j *Job) GetParentJobs() []*Job {
	var jobs []*Job
	parentNames := strings.Split(j.Parents, ",")
	for _, name := range parentNames {
		name := strings.Trim(name, " \t")
		log.Info(name)
		job, err := GetJobByName(name)
		if err == nil {
			jobs = append(jobs, job)
		} else {
			log.Debug(err)
		}
	}
	return jobs
}
