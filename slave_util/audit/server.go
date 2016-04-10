package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

// http service
type AuditServiceHandler interface {
	GetContainerByID(w http.ResponseWriter, r *http.Request)
	Run(w http.ResponseWriter, r *http.Request)
}

type AuditServiceHandlerImpl struct {
	as AuditService
}

type RunCmd struct {
	Container *Container `json:"container"`
	Command   *Command   `json:"cmd"`
}

// req:  GET /task/<task-id>
// res : json { "id" : <container-id> }
func (a *AuditServiceHandlerImpl) GetContainerByID(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/task/"):]
	if len(id) == 0 {
		http.Error(w, "Invalid task id, please use the request GET /task/<id>", http.StatusBadRequest)
		return
	}

	taskId := TaskID(id)
	c, err := a.as.GetContainerByID(&taskId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resBody, err := json.Marshal(*c)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(resBody)
}

// req : POST /exec ,  with body : { "container" : { "id" : <container-id> }, "cmd" : [<cmd>, <arg1>, <arg2>, ...] }
// res : json { "stdout" : <stdout>, "stderr" : <stderr>, "status" : <status code> }
func (a *AuditServiceHandlerImpl) Run(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Only http POST is allowed to submit a remote command", http.StatusBadRequest)
		return
	}

	var cmd RunCmd
	if err := json.Unmarshal(reqBody, &cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cmdRes, err := a.as.Run(cmd.Container, cmd.Command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resBody, err := json.Marshal(*cmdRes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(resBody)
}

func StartServer(port int) {
	s, _ := NewAuditService()
	as := &AuditServiceHandlerImpl{
		as: s,
	}
	http.HandleFunc("/task/", as.GetContainerByID)
	http.HandleFunc("/exec/", as.Run)
	log.Fatalln(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func main() {
	port := flag.Int("port", 8080, "port number for docker audit service")
	flag.Parse()
	log.Println("docker audit service starting at port ", *port)
	StartServer(*port)
}
