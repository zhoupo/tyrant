package scheduler

import (
	"testing"
)

func TestPickTask(t *testing.T) {
	td := NewTaskDag("test", nil)
	dag := td.dag
	for i := 0; i < 100; i++ {
		if err := dag.AddVertex("foo", &Task{Name: "foo"}, nil); err != nil {
			t.Error(err)
		}

		if err := dag.AddVertex("bar", &Task{Name: "bar"}, []string{"foo"}); err != nil {
			t.Error(err)
		}

		if err := dag.AddVertex("cc", &Task{Name: "cc"}, []string{"foo", "bar"}); err != nil {
			t.Error(err)
		}

		if err := dag.AddVertex("dd", &Task{Name: "dd"}, []string{"foo", "bar", "cc"}); err != nil {
			t.Error(err)
		}

		tasks := td.GetReadyTask()
		if len(tasks) != 1 || tasks[0].Name != "foo" {
			t.Errorf("should get foo %+v", tasks)
			return
		}

		td.dag.RemoveVertexByName("foo")
		tasks = td.GetReadyTask()
		if len(tasks) != 1 || tasks[0].Name != "bar" {
			t.Error("should get bar")
		}

		td.dag.RemoveVertexByName("bar")
		tasks = td.GetReadyTask()
		if len(tasks) != 1 || tasks[0].Name != "cc" {
			t.Error("should get cc")
		}

		td.dag.RemoveVertexByName("cc")
		tasks = td.GetReadyTask()
		if len(tasks) != 1 || tasks[0].Name != "dd" {
			t.Error("should get dd")
		}

		td.dag.RemoveVertexByName("dd")
		tasks = td.GetReadyTask()
		if len(tasks) != 0 {
			t.Error("should get nothing")
		}
	}
}
