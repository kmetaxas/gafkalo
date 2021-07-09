package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"io"
	"io/ioutil"
	"net/http"
)

type ConnectAdmin struct {
	Url       string
	Username  string // basic auth username
	Password  string // basic auth password
	TlsConfig *tls.Config
}

func NewConnectAdin(config *ConnectConfig) (*ConnectAdmin, error) {
	var admin ConnectAdmin
	if config.Url == "" {
		return &admin, fmt.Errorf("url is required for connect")
	}
	admin.Url = config.Url
	admin.Username = config.User
	admin.Password = config.Password
	// TODO construct TLSconfig
	return &admin, nil
}

// Status of a task as returned by /task/<num>/status endpoint
type TaskStatus struct {
	ID       int    `json:"id"`
	Status   string `json:"state"`
	WorkerID string `json:"worker_id"`
	// not part of resposne but utility functions fill it in to make boolean check easy.
	isRunning bool
}

// Perform REST call on Connect
// method is POST,GET,PUT etc.
// `api` is the part param after the host so the /connectors/myconnector/config for eample
// payload is the optional payload to send (or nil)
func (admin *ConnectAdmin) doREST(method, api string, payload io.Reader) ([]byte, int, error) {
	var httpStatus int = 0
	hClient := http.Client{}
	uri := fmt.Sprintf("%s%s", admin.Url, api)
	req, err := http.NewRequest(method, uri, nil)
	if err != nil {
		return nil, httpStatus, err
	}
	if admin.Username != "" && admin.Password != "" {
		req.SetBasicAuth(admin.Username, admin.Password)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	resp, err := hClient.Do(req)
	if err != nil {
		return nil, httpStatus, err
	}
	httpStatus = resp.StatusCode
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, httpStatus, err
	}
	return respBody, httpStatus, nil
}

func (admin *ConnectAdmin) ListConnectors() ([]string, error) {
	var connectors []string
	respBody, _, err := admin.doREST("GET", "/connectors", nil)
	if err != nil {
		return connectors, err
	}
	err = json.Unmarshal(respBody, &connectors)
	if err != nil {
		return connectors, err
	}
	return connectors, nil
}
func (admin *ConnectAdmin) ListTasksForConnector(connector string) (map[int]*TaskStatus, error) {
	var connectors map[int]*TaskStatus
	type Task struct {
		Connector string `json:"connector"`
		Task      int    `json:"task"`
	}
	type TasksResp struct {
		// Lots of information in the response that we ignore here
		Tasks []Task `json:"tasks"`
	}
	connectors = make(map[int]*TaskStatus)
	uri := fmt.Sprintf("/connectors/%s", connector)
	var taskList *TasksResp
	respBody, _, err := admin.doREST("GET", uri, nil)
	if err != nil {
		return connectors, err
	}
	err = json.Unmarshal(respBody, &taskList)
	if err != nil {
		return connectors, err
	}
	// Get the status of each task using REST calls
	for _, task := range taskList.Tasks {
		taskStatus, err := admin.GetTaskStatus(connector, task.Task)
		if err != nil {
			return connectors, err
		}
		connectors[task.Task] = taskStatus
	}
	return connectors, nil
}
func (admin *ConnectAdmin) GetTaskStatus(connector string, task int) (*TaskStatus, error) {
	uri := fmt.Sprintf("/connectors/%s/tasks/%d/status", connector, task)
	respBody, _, err := admin.doREST("GET", uri, nil)
	var taskStatus *TaskStatus
	if err != nil {
		return taskStatus, err
	}
	err = json.Unmarshal(respBody, &taskStatus)
	if err != nil {
		return taskStatus, err
	}
	if taskStatus.Status == "RUNNING" {
		taskStatus.isRunning = true
	}
	return taskStatus, nil

}

func prettyPrintTaskStatus(task *TaskStatus) {
	stateFmt := color.New(color.FgGreen).SprintFunc()
	if !task.isRunning {
		stateFmt = color.New(color.FgGreen).SprintFunc()
	}
	msg := fmt.Sprintf("Task [%d] has status %s on worker '%s' (running: %v)\n", task.ID, stateFmt(task.Status), task.WorkerID, stateFmt(task.isRunning))
	fmt.Printf(msg)

}
