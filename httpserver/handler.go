package httpserver

import (
	"net/http"
)

type SchedulerServer interface {
	CreateTrigger(http.ResponseWriter, *http.Request)
	GetStat(http.ResponseWriter, *http.Request)
}

// with leader election
type HASchedulerServer struct {
}
