package scheduler

import (
	"fmt"
	"log"
	"net/http"
)

type Server struct {
	addr string
}

func jobsListHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello world")
}

func (srv *Server) Serve() {
	http.HandleFunc("/jobs/list", jobsListHandler)
	addr, _ := globalCfg.ReadString("http_addr", ":9090")
	log.Fatal(http.ListenAndServe(addr, nil))
}
