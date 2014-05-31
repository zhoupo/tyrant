package scheduler

import (
	log "github.com/ngaut/logging"
)

const (
	taskCreate = 1
	taskRuning = 2
)

type Task struct {
	Name string
}

type TaskDag struct {
	DagName string
	dag     *DGraph
	state   int
}

func NewTaskDag(name string) *TaskDag {
	td := &TaskDag{DagName: name, dag: NewDag(), state: taskCreate}
	log.Debug("create TaskDag", name)
	return td
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

type TaskScheduler struct {
	tds map[string]*TaskDag
}

func NewTaskScheduler() *TaskScheduler {
	return &TaskScheduler{tds: make(map[string]*TaskDag)}
}

func (self *TaskScheduler) AddTaskDag(name string, td *TaskDag) {
	if _, ok := self.tds[name]; ok {
		return
	}

	self.tds[name] = td
}

func (self *TaskScheduler) RemoveTaskDag(name string) {
	delete(self.tds, name)
	//todo: may be need to clean up running tasks
}
