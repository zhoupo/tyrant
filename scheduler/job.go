package scheduler

type Job struct {
	Id            int64  `db:"id" json:"id"`
	Name          string `db:"name" json:"name"`       // 512, unique
	Command       string `db:"command" json:"command"` // 4096
	Epsilon       string `db:"epsilon" json:"epsilon"`
	Executor      string `db:"executor" json:"executor"`             // 4096
	ExecutorFlags string `db:"executor_flags" json:"executor_flags"` // 4096
	Retries       int    `db:"retries" json:"retries"`
	Owner         string `db:"owner" json:"owner"`
	Async         bool   `db:"async" json:"async"`
	SuccessCnt    int    `db:"success_cnt" json:"success_cnt"`
	ErrCnt        int    `db:"error_cnt" json:"error_cnt"`
	CreateTs      int64  `db:"create_ts" json:"create_ts"`
	LastSuccessTs int64  `db:"last_success_ts" json:"last_success_ts"`
	LastErrTs     int64  `db:"last_error_ts" json:"last_error_ts"`
	Cpus          int    `db:"cpus" json:"cpus"`
	Mem           int    `db:"mem" json:"mem"`
	Disk          int64  `db:"disk" json:"disk"`
	Disabled      bool   `db:"disabled" json:"disabled"`
	Uris          string `db:"uris" json:"uris"` // 2048
}

func GetJobList() []Job {
	var jobs []Job
	_, err := sharedDbMap.Select(&jobs, "select * from jobs order by create_ts desc")
	if err != nil {
		return nil
	}
	return jobs
}

func JobExists(name string) bool {
	var cnt int
	err := sharedDbMap.SelectOne(&cnt, "select count(*) from jobs where name=?", name)
	if err != nil {
		return false
	}
	if cnt <= 1 {
		return false
	}
	return true
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

func (j *Job) Disable(b bool) error {
	j.Disabled = b
	_, err := sharedDbMap.Update(j)
	return err
}

func (j *Job) Save() error {
	if j.Id <= 0 {
		return sharedDbMap.Insert(j)
	} else {
		_, err := sharedDbMap.Update(j)
		return err
	}
}

func (j *Job) Remove() error {
	if j.Id > 0 {
		cnt, err := sharedDbMap.Delete(j)
		if cnt == 1 && err == nil {
			j.Id = -1
			return nil
		}
		return err
	}
	j.Id = -1
	return nil
}
