package scheduler

import (
	"testing"
)

func TestAdd(t *testing.T) {
	dag := &DGraph{root: &Vertex{}}
	if err := dag.AddVertex("foo", "hi", nil); err != nil {
		t.Error(err)
	}

	f := func(v *Vertex) {
		println(v.Name)
	}

	v := dag.Lookup("foo")
	if v == nil {
		t.Error("lookup failed")
	}

	dag.travel(f)

	if err := dag.AddVertex("bar", "hello", []string{"foo"}); err != nil {
		t.Error(err)
	}

	v = dag.Lookup("bar")
	if v == nil {
		t.Error("lookup failed")
	}

	dag.travel(f)

	if err := dag.AddVertex("cc", "hello", []string{"foo", "bar"}); err != nil {
		t.Error(err)
	}

	dag.travel(f)
}
