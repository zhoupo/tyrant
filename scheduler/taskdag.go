package scheduler

import (
	"strings"

	log "github.com/ngaut/logging"
)

const (
	taskCreate = 1
	taskRuning = 2
)

type Task struct {
	Name string
	job  *DagJob
}

type TaskDag struct {
	DagName string
	dag     *DGraph
	state   int
	dagMeta *DagMeta
}

func NewTaskDag(name string, meta *DagMeta) *TaskDag {
	td := &TaskDag{DagName: name, dag: NewDag(), state: taskCreate,
		dagMeta: meta,
	}
	log.Debug("create TaskDag", name)
	return td
}

func getParents(parents string) []string {
	return strings.Split(parents, ",")
}

func getRoot(jobs map[string]*DagJob, name string) string {
	parents := getParents(name)
	for _, p := range parents {
		pp, ok := jobs[p]
		if !ok || len(getParents(pp.JobName)) == 0 {
			return p
		}
		return getRoot(jobs, pp.JobName)
	}

	return name
}

func (self *TaskDag) addTask(jobs map[string]*DagJob) {
	for name, _ := range jobs {
		r := getRoot(jobs, name)
		self.dag.AddVertex(r, &Task{Name: r, job: jobs[r]}, getParents(r))
		delete(jobs, r)
		return
	}
}

func (self *TaskDag) BuildTaskDag(jobs []*DagJob) {
	m := make(map[string]*DagJob, len(jobs))
	for _, j := range jobs {
		m[j.JobName] = j
	}

	//todo: maybe need to clean up running tasks
	self.dag = NewDag()
	for i := 0; i < len(jobs); i++ {
		self.addTask(m)
	}
}

//todo: just need to search first level, which would be faster
func (self *TaskDag) GetReadyTask() []*Task {
	ready := make(map[string]*Vertex)
	filter := func(v *Vertex) {
		//log.Debugf("%+v", v)
		if !v.hasDependency() {
			ready[v.Name] = v
		}
	}

	self.dag.travel(filter)

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
	self.dag.RemoveVertexByName(name)
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

	self.tds[td.DagName] = td
}

func (self *TaskScheduler) RemoveTaskDag(name string) {
	delete(self.tds, name)
	//todo: may be need to clean up running tasks
}

func (self *TaskScheduler) GetReadyDag() *TaskDag {
	for _, td := range self.tds {
		if td.state == taskCreate {
			return td
		}
	}

	return nil
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
		jobs := make([]*DagJob, len(tmp))
		for i := 0; i < len(jobs); i++ {
			jobs[i] = &tmp[i]
		}
		td.BuildTaskDag(jobs)
		self.AddTaskDag(td)
	}
}
