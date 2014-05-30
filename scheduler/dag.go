package scheduler

import (
	"fmt"

	log "github.com/ngaut/logging"
)

type Vertex struct {
	Name    string
	Value   interface{}
	InEdge  []*Vertex
	OutEdge []*Vertex
}

type DGraph struct {
	root *Vertex
}

func (self *Vertex) lookup(name string) *Vertex {
	if self.Name == name {
		return self
	}

	for _, v := range self.OutEdge {
		if vt := v.lookup(name); vt != nil {
			return vt
		}
	}

	return nil
}

func (self *Vertex) travel(f func(v *Vertex)) {
	f(self)
	for _, v := range self.OutEdge {
		v.travel(f)
	}
}

func (self *DGraph) checkParent(v *Vertex) error {
	for _, in := range v.InEdge {
		parent := self.root.lookup(in.Name) //parent should exist, akka: dependency should be valid
		if parent == nil {
			return fmt.Errorf("parent %s not exist", in.Name)
		}

		for _, child := range parent.OutEdge {
			if child.Name == v.Name {
				return fmt.Errorf("parent %s already has child %s", in.Name, v.Name)
			}
		}
	}

	return nil
}

func (self *DGraph) add(v *Vertex) error {
	if err := self.checkParent(v); err != nil {
		return err
	}

	for _, in := range v.InEdge {
		parent := self.root.lookup(in.Name)
		if parent == nil {
			log.Fatal("should never happend")
		}

		parent.OutEdge = append(parent.OutEdge, v)
		for i, tmp := range v.InEdge {
			if tmp.Name == parent.Name {
				v.InEdge[i] = parent
			}
		}
	}

	return nil
}

func (self *DGraph) AddVertex(name string, val interface{}, in []string) error {
	log.Debug("AddVertex", name, val)
	v := &Vertex{Name: name, Value: val,
		InEdge: make([]*Vertex, 0, len(in)),
	}
	if len(in) == 0 {
		if self.Lookup(name) != nil {
			return fmt.Errorf("%s already exist", name)
		}
		self.root.OutEdge = append(self.root.OutEdge, v)
		return nil
	}
	for _, n := range in {
		v.InEdge = append(v.InEdge, &Vertex{Name: n})
	}

	return self.add(v)
}

func (self *DGraph) travel(f func(v *Vertex)) {
	for _, v := range self.root.OutEdge {
		v.travel(f)
	}
}

func (self *DGraph) Lookup(name string) *Vertex {
	//log.Debug("Lookup", name)
	return self.root.lookup(name)
}
