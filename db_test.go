package tyrant

import (
	"testing"
)

func TestDbMap(t *testing.T) {
	dbMap := NewDbMap()
	j := &Job{
		Name:    "Test",
		Command: "ls",
		Epsilon: "fuck",
	}

	err := dbMap.Insert(j)
	if err != nil {
		t.Error(err)
	}
}
