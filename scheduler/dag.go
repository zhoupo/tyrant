package scheduler

import (
	"fmt"
	"io"
	"os"

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
	for _, v := range self.OutEdge {
		f(v)
		v.travel(f)
	}
}

func findByName(vertexs []*Vertex, name string) int {
	for i, v := range vertexs {
		if v.Name == name {
			return i
		}
	}

	return -1
}

func (self *Vertex) removeChild(name string) error {
	idx := findByName(self.OutEdge, name)
	if idx == -1 {
		return fmt.Errorf("%s not exist", name)
	}
	//remove it from out edge
	self.OutEdge = append(self.OutEdge[:idx], self.OutEdge[idx+1:]...)
	return nil
}

func (self *DGraph) checkParent(v *Vertex) error {
	for _, in := range v.InEdge {
		parent := self.root.lookup(in.Name) //parent should exist, akka: dependency should be valid
		if parent == nil {
			return fmt.Errorf("parent %s not exist", in.Name)
		}
		//should not in children node
		if findByName(parent.OutEdge, v.Name) != -1 {
			return fmt.Errorf("parent %s already has child %s", in.Name, v.Name)
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
		v.InEdge = append(v.InEdge, self.root)
		self.root.OutEdge = append(self.root.OutEdge, v)
		return nil
	}
	for _, n := range in {
		if name == n { //check dependence ring
			fmt.Errorf("job %s can't depend on self", name)
		}
		v.InEdge = append(v.InEdge, &Vertex{Name: n})
	}

	return self.add(v)
}

func (self *DGraph) removeVertex(v *Vertex) {
	for _, p := range v.InEdge {
		p.removeChild(v.Name)
		//check the same child
		for _, o := range v.OutEdge {
			//already exist
			if findByName(p.OutEdge, o.Name) >= 0 {
				continue
			}
			p.OutEdge = append(p.OutEdge, o)
		}
	}

	for _, c := range v.OutEdge {
		idx := findByName(c.InEdge, v.Name)
		if idx == -1 {
			log.Fatal("should never happend")
		}
		//remove parent
		c.InEdge = append(c.InEdge[:idx], c.InEdge[idx+1:]...)
		//add new parents
		for _, in := range v.InEdge {
			if index := findByName(c.InEdge, in.Name); index == -1 {
				c.InEdge = append(c.InEdge, in)
			}
		}
	}
}

func (self *DGraph) RemoveVertexByName(name string) {
	v := self.Lookup(name)
	if v != nil {
		self.removeVertex(v)
	}
}

//BFS
func (self *DGraph) travel(f func(v *Vertex)) {
	for _, v := range self.root.OutEdge {
		f(v)
		v.travel(f)
	}
}

func (self *DGraph) ExportDot(fname string) {
	relations := make(map[string]string)
	f := func(v *Vertex) {
		for _, c := range v.OutEdge {
			relations[v.Name+" -> "+c.Name] = ""
		}
	}

	self.travel(f)

	file, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666) // For read access.
	if err != nil {
		log.Fatal(err)
	}

	io.WriteString(file, "digraph job {\n")

	for k, _ := range relations {
		io.WriteString(file, k)
		io.WriteString(file, "\n")
	}

	io.WriteString(file, "}\n")
}

func (self *DGraph) Lookup(name string) *Vertex {
	//log.Debug("Lookup", name)
	return self.root.lookup(name)
}
