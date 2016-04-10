package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	dc "github.com/samalba/dockerclient"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
)

// a simple audit service running on all mesos-slave
// docker cmd for debugging  : (all need sudo su)
// /home/vagrant/go/bin/go run audit.go
// echo -e  "GET /images/json HTTP/1.0\r\n" | nc -U /var/run/docker.sock

type TaskID string

type Command []string

type Container struct {
	ID string `json:"id"`
}

type CommandRes struct {
	Stdout []byte `json:"stdout"`
	Stderr []byte `json:"stderr"`
	Status int32  `json:"status"`
}

type AuditService interface {
	GetContainerByID(*TaskID) (*Container, error)
	Run(*Container, *Command) (*CommandRes, error)
}

type AuditServiceImpl struct {
	unixDomain string
	client     *dc.DockerClient
	c          *dockerClient // customized client for ExecStart API
}

func NewAuditService() (AuditService, error) {
	domain := "unix:///var/run/docker.sock"
	if client, err := dc.NewDockerClient(domain, nil); err != nil {
		return nil, err
	} else {
		c, _ := newDockerClient(domain)
		return &AuditServiceImpl{
			unixDomain: domain,
			client:     client,
			c:          c,
		}, nil
	}
}

func (a *AuditServiceImpl) GetContainerByID(id *TaskID) (*Container, error) {
	containers, err := a.client.ListContainers(false, false, "")
	if err != nil {
		return nil, err
	}

	for _, c := range containers {
		if info, err := a.client.InspectContainer(c.Id); err == nil {
			for _, env := range info.Config.Env {
				if env == fmt.Sprintf("MESOS_TASK_ID=%v", *id) {
					return &Container{
						ID: c.Id,
					}, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("Could not find container with for task %v", *id)
}

func (a *AuditServiceImpl) Run(container *Container, cmd *Command) (*CommandRes, error) {
	config := &dc.ExecConfig{
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          *cmd,
		Container:    container.ID,
		Tty:          true,
	}

	execId, err := a.client.ExecCreate(config)
	if err != nil {
		return nil, err
	}
	// use customized ExecStart
	conf := &execStartConfig{
		Detach: false,
		Tty:    false,
	}
	res, err := a.c.execStart(execId, conf)
	if err != nil {
		return nil, err
	}
	return &CommandRes{
		Stdout: res, // for execStart command stdout and stderr are multiplexed
		Stderr: make([]byte, 0),
		Status: 0,
	}, nil
}

// customized the client for ExecStart() as in current dockerClient's ExecStart() it doesn't
// really return any data
type dockerClient struct {
	httpClient *http.Client
	u          *url.URL
}

type execStartConfig struct {
	Tty    bool
	Detach bool
}

func newDockerClient(unixDomain string) (*dockerClient, error) {
	u, err := url.Parse(unixDomain)
	if err != nil {
		return nil, err
	}
	socketPath := u.Path
	unixDial := func(proto, addr string) (net.Conn, error) {
		return net.Dial("unix", socketPath)
	}
	httpTransport := &http.Transport{}
	httpTransport.Dial = unixDial
	// Override the main URL object so the HTTP lib won't complain
	u.Scheme = "http"
	u.Host = "unix.sock"
	u.Path = ""

	return &dockerClient{
		httpClient: &http.Client{
			Transport: httpTransport,
		},
		u: u,
	}, nil
}

func (c *dockerClient) execStart(execId string, conf *execStartConfig) ([]byte, error) {
	reqBody, err := json.Marshal(*conf)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST",
		c.u.String()+fmt.Sprintf("/exec/%v/start", execId),
		bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}
