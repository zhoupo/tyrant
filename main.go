package main

import (
	"github.com/ngaut/tyrant/scheduler"
	"github.com/ngaut/tyrant/scheduler/resourceScheduler"
)

func init() {
	scheduler.InitConfig("config.ini")
	scheduler.InitSharedDbMap()
}

func main() {
	go func() {
		resourceScheduler.NewResMan().Run()
	}()
	s := scheduler.Server{}
	s.Serve()
}
