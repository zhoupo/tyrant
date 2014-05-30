package tyrant

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
	Schedule      string `db:"schedule"` // 255
	Parents       string `db:"parents"`  // 4096
}
