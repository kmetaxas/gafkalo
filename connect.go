package main

import (
	"bytes"
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
	if config.CAPath != "" {
		admin.TlsConfig = createTlsConfig(config.CAPath, config.SkipVerify)
	}
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

type Task struct {
	Connector string `json:"connector"`
	Task      int    `json:"task"`
}
type ConnectorInfo struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
	// Lots of information in the response that we ignore here
	Tasks []Task `json:"tasks"`
}

type ConnectorStatus struct {
	Name      string                 `json:"name"`
	Connector map[string]interface{} `json:"connector"`
	Tasks     []TaskStatus           `json:"tasks"`
}

// Perform REST call on Connect
// method is POST,GET,PUT etc.
// `api` is the part param after the host so the /connectors/myconnector/config for eample
// payload is the optional payload to send (or nil)
func (admin *ConnectAdmin) doREST(method, api string, payload io.Reader) ([]byte, int, error) {
	var httpStatus int = 0
	transport := &http.Transport{TLSClientConfig: admin.TlsConfig}
	hClient := http.Client{Transport: transport}
	uri := fmt.Sprintf("%s%s", admin.Url, api)
	req, err := http.NewRequest(method, uri, payload)
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

// Get information about the connector. corresponds to  GET /connectors/(string: name)
func (admin *ConnectAdmin) GetConnectorInfo(connector string) (*ConnectorInfo, error) {
	var resp ConnectorInfo
	uri := fmt.Sprintf("/connectors/%s", connector)
	respBody, _, err := admin.doREST("GET", uri, nil)
	if err != nil {
		return &resp, err
	}
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		return &resp, err
	}
	return &resp, nil

}

// Get connector status
func (admin *ConnectAdmin) GetConnectorStatus(connector string) (*ConnectorStatus, error) {
	var status ConnectorStatus
	var err error
	uri := fmt.Sprintf("/connectors/%s/status", connector)
	respBody, _, err := admin.doREST("GET", uri, nil)
	if err != nil {
		return &status, err
	}
	err = json.Unmarshal(respBody, &status)

	return &status, nil
}
func (admin *ConnectAdmin) ListTasksForConnector(connector string) (map[int]*TaskStatus, error) {
	connectors := make(map[int]*TaskStatus)

	// Get the status of each task using REST calls
	connectorInfo, err := admin.GetConnectorInfo(connector)
	if err != nil {
		return connectors, err
	}
	for _, task := range connectorInfo.Tasks {
		taskStatus, err := admin.GetTaskStatus(connector, task.Task)
		if err != nil {
			return connectors, err
		}
		connectors[task.Task] = taskStatus
	}
	return connectors, nil
}

// Query the API for the status of a task
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

func (admin *ConnectAdmin) CreateConnector(jsonDefinition string) (string, error) {
	var name string
	type createConnectorRequest struct {
		Name   string                 `json:"name"`
		Config map[string]interface{} `json:"config"`
	}
	type createConnectorResponse struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
		Tasks  []Task            `json:"tasks"`
	}
	var request createConnectorRequest
	err := json.Unmarshal([]byte(jsonDefinition), &request)
	if err != nil {
		return name, err
	}
	var response createConnectorResponse
	uri := "/connectors"
	reqBody, err := json.Marshal(request)
	if err != nil {
		return name, err
	}
	respBody, statusCode, err := admin.doREST("POST", uri, bytes.NewBuffer(reqBody))
	if statusCode < 200 || statusCode > 400 {
		return name, fmt.Errorf("Request failed with status code %d\nResponse body: %s\n", statusCode, respBody)
	}
	if err != nil {
		return name, err
	}
	err = json.Unmarshal(respBody, &response)
	if err != nil {
		return name, err
	}
	name = response.Name
	return name, nil

}
func (admin *ConnectAdmin) DeleteConnector(connector string) error {
	uri := fmt.Sprintf("/connectors/%s", connector)
	respBody, statusCode, err := admin.doREST("DELETE", uri, nil)
	if err != nil {
		return err
	}
	if statusCode == 409 {
		return fmt.Errorf("Rebalance in progress.. Check status and try again later")
	}
	if statusCode < 200 || statusCode > 400 {
		return fmt.Errorf("Request failed with status code %d\nResponse body: %s\n", statusCode, respBody)
	}
	return nil

}

// Restart task number for connector
func (admin *ConnectAdmin) RestartTask(connector string, taskID int) error {
	uri := fmt.Sprintf("/connectors/%s/tasks/%d/restart", connector, taskID)
	respBody, statusCode, err := admin.doREST("POST", uri, nil)
	if err != nil {
		return err
	}
	if statusCode < 200 || statusCode > 400 {

		return fmt.Errorf("Failed to restart task %d with status %d (response:%s)", taskID, statusCode, respBody)
	}
	return nil

}

// Restart connector
func (admin *ConnectAdmin) RestartConnector(connector string) error {
	uri := fmt.Sprintf("/connectors/%s/restart", connector)
	respBody, statusCode, err := admin.doREST("POST", uri, nil)
	if err != nil {
		return err
	}
	if statusCode < 200 || statusCode > 400 {

		return fmt.Errorf("Failed to restart connector %s (http: %d)(response:%s)", connector, statusCode, respBody)
	}
	return nil
}
func prettyPrintTaskStatus(task *TaskStatus) {
	stateFmt := color.New(color.FgGreen).SprintFunc()
	if !task.isRunning {
		stateFmt = color.New(color.FgGreen).SprintFunc()
	}
	msg := fmt.Sprintf("Task [%d] has status %s on worker '%s' (running: %v)\n", task.ID, stateFmt(task.Status), task.WorkerID, stateFmt(task.isRunning))
	fmt.Print(msg)

}

// Get if the connector if healthy. This means that the connector itself reports healthy *and* all the tasks report healthy
func (status *ConnectorStatus) isHealthy() bool {
	if status.Connector["state"] != "RUNNING" {
		return false
	}
	for _, task := range status.Tasks {
		if task.Status != "RUNNING" {
			return false
		}
	}
	return true
}
