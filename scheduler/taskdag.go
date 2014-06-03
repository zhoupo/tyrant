package scheduler

import (
	"strings"

	log "github.com/ngaut/logging"
)

const (
	taskReady  = 1
	taskRuning = 2
)

type Task struct {
	Name string
	job  *DagJob
}

type TaskDag struct {
	DagName string
	Dag     *DGraph
	state   int
	dagMeta *DagMeta
}

func NewTaskDag(name string, meta *DagMeta) *TaskDag {
	td := &TaskDag{DagName: name, Dag: NewDag(), state: taskReady,
		dagMeta: meta,
	}
	log.Debug("create TaskDag", name)
	return td
}

func getParents(parents string) []string {
	ss := strings.Split(parents, ",")
	ps := make([]string, 0)
	for _, s := range ss { //trim blank
		ts := strings.Trim(s, " ")
		if len(ts) > 0 {
			ps = append(ps, ts)
		}
	}

	return ps
}

func getRoot(jobs map[string]*DagJob, name string) string {
	parentstr := jobs[name].ParentJob
	parents := getParents(parentstr)
	log.Debug(name, "parents", parents, len(parents))
	for _, p := range parents {
		pp, ok := jobs[p]
		if ok {
			return getRoot(jobs, pp.JobName)
		}
		continue
	}

	return name
}

func (self *TaskDag) addTask(jobs map[string]*DagJob) {
	for name, _ := range jobs {
		log.Debug("checking", name)
		if len(name) == 0 {
			log.Errorf("job name can't be empty")
			continue
		}

		r := getRoot(jobs, name)
		log.Debugf("add %s to %s", r, self.DagName)
		err := self.Dag.AddVertex(r, &Task{Name: r, job: jobs[r]}, getParents(jobs[name].ParentJob))
		if err != nil {
			log.Error(err)
		}
		delete(jobs, r)
		return
	}
}

func (self *TaskDag) BuildTaskDag(jobs []*DagJob) {
	log.Debugf("BuildTaskDag:%+v", jobs)
	m := make(map[string]*DagJob, len(jobs))
	for _, j := range jobs {
		m[j.JobName] = j
	}

	log.Debugf("DagJob map: %+v", m)

	//todo: maybe need to clean up running tasks
	self.Dag = NewDag()
	for i := 0; i < len(jobs); i++ {
		self.addTask(m)
	}
}

//todo: just need to search first level, which would be faster
func (self *TaskDag) GetReadyTask() []*Task {
	ready := make(map[string]*Vertex)
	filter := func(v *Vertex) {
		log.Debugf("check dependency %+v", v)
		if !v.hasDependency() {
			ready[v.Name] = v
		}
	}

	self.Dag.travel(filter)

	if len(ready) == 0 {
		return nil
	}

	tasks := make([]*Task, 0, len(ready))
	for _, v := range ready {
		tasks = append(tasks, v.Value.(*Task))
	}

	return tasks
}

func (self *TaskDag) RemoveTask(name string) {
	log.Debugf("remove task %s from %s", name, self.DagName)
	self.Dag.RemoveVertexByName(name)
}

type TaskScheduler struct {
	tds map[string]*TaskDag
}

func NewTaskScheduler() *TaskScheduler {
	return &TaskScheduler{tds: make(map[string]*TaskDag)}
}

func (self *TaskScheduler) AddTaskDag(td *TaskDag) {
	if len(td.DagName) == 0 {
		return
	}
	if _, ok := self.tds[td.DagName]; ok {
		return
	}

	log.Debugf("AddTaskDag: %+v", td)

	self.tds[td.DagName] = td
}

func (self *TaskScheduler) GetTaskDag(name string) *TaskDag {
	return self.tds[name]
}

func (self *TaskScheduler) RemoveTaskDag(name string) {
	delete(self.tds, name)
	//todo: may be need to clean up running tasks
}

func (self *TaskScheduler) GetReadyDag() *TaskDag {
	for _, td := range self.tds {
		if td.state == taskReady {
			return td
		}
	}

	return nil
}

func (self *TaskScheduler) SetTaskDagStateRunning(name string) {
	td, ok := self.tds[name]
	if !ok {
		return
	}

	td.state = taskRuning
}

func (self *TaskScheduler) SetTaskDagStateReady(name string) {
	td, ok := self.tds[name]
	if !ok {
		return
	}

	td.state = taskReady
}

func (self *TaskScheduler) Refresh() {
	metas := GetDagMetaList()
	for _, meta := range metas {
		m := meta
		if _, ok := self.tds[m.Name]; ok { //already exist
			continue
		}

		td := NewTaskDag(m.Name, &m)
		tmp := m.GetDagJobs()
		log.Debugf("add dagMeta: %+v, jobs:%+v", m, tmp)
		jobs := make([]*DagJob, len(tmp))
		for i := 0; i < len(jobs); i++ {
			jobs[i] = &tmp[i]
		}
		td.BuildTaskDag(jobs)
		self.AddTaskDag(td)
	}
}
