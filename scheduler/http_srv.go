package scheduler

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
)

type Server struct {
	addr string
}

func response(w http.ResponseWriter, status int, content string) {
	w.WriteHeader(status)
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

func removeJobHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	j, _ := GetJobByName(name)
	if j != nil {
		j.Remove()
		response(w, http.StatusOK, "")
	} else {
		response(w, http.StatusNotFound, "no such job")
	}
}

func newDagHandler(w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	var dag DagMeta
	err = json.Unmarshal(b, &dag)
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	err = dag.Save()
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	content, _ := json.MarshalIndent(dag, " ", "  ")
	response(w, http.StatusOK, string(content))
}

func addDagJob(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		response(w, http.StatusBadRequest, err.Error())
		return
	}
	dag := GetDagFromName(name)
	if dag != nil {
		var job DagJob
		err = json.Unmarshal(b, &job)
		if err != nil {
			response(w, http.StatusBadRequest, err.Error())
			return
		}
		(&job).Save()
		dag.AddDagJob(&job)
	}
}

func removeDagJob(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("name")
	dag := GetDagFromName(name)
	if dag != nil {
		if dag.Remove() == nil {
			response(w, http.StatusOK, "")
			return
		}
	} else {
		response(w, http.StatusBadRequest, "")
	}
}

func (srv *Server) Serve() {
	http.HandleFunc("/job/list", jobsListHandler)
	http.HandleFunc("/job/new", newJobHandler)
	http.HandleFunc("/job/remove", removeJobHandler)
	http.HandleFunc("/job/update", updateJobHandler)
	http.HandleFunc("/dag/new", newDagHandler)
	http.HandleFunc("/dag/job/add", addDagJob)
	http.HandleFunc("/dag/job/remove", removeDagJob)
	addr, _ := globalCfg.ReadString("http_addr", ":9090")
	log.Fatal(http.ListenAndServe(addr, nil))
}
