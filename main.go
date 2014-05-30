package main

import (
	"github.com/ngaut/tyrant/scheduler"
)

func init() {
	scheduler.InitConfig("config.ini")
	scheduler.InitSharedDbMap()
}

func main() {
	s := scheduler.Server{}
	s.Serve()
}
