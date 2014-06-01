package scheduler

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

type Server struct {
	addr string
}

func response(w http.ResponseWriter, status int, content string) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(content))
}

func jobsListHandler(w http.ResponseWriter, r *http.Request) {
	jobs := GetJobList()
	if jobs != nil && len(jobs) > 0 {
		content, _ := json.MarshalIndent(jobs, " ", "  ")
		response(w, http.StatusOK, string(content))
		return
	}
	response(w, http.StatusOK, "[]")
}

func updateJobHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")

	if JobExists(name) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			response(w, http.StatusBadRequest, err.Error())
			return
		}
		var job Job
		err = json.Unmarshal(b, &job)
		j, _ := GetJobByName(name)
		if err != nil {
			response(w, http.StatusBadRequest, err.Error())
			return
		}
		job.Id = j.Id
		job.Save()
		response(w, http.StatusOK, string(b))
		return
	} else {
		response(w, http.StatusNotFound, "no such job")
	}
}

func newJobHandler(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	var job Job
	err = json.Unmarshal(b, &job)
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	err = sharedDbMap.Insert(&job)
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	content, _ := json.MarshalIndent(job, " ", "  ")
	response(w, http.StatusOK, string(content))
}

func (srv *Server) Serve() {
	http.HandleFunc("/job/list", jobsListHandler)
	http.HandleFunc("/job/new", newJobHandler)
	http.HandleFunc("/job/update", updateJobHandler)
	addr, _ := globalCfg.ReadString("http_addr", ":9090")
	log.Fatal(http.ListenAndServe(addr, nil))
}
