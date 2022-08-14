package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
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

func NewConnectAdmin(config *ConnectConfig) (*ConnectAdmin, error) {
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

// Represents the current state of a Connect cluster.
// It lists connectors , their config, tasks etc
type ConnectClusterState struct {
	Connectors map[string]Connector `json:"connectors"`
}

// Status of a task as returned by /task/<num>/status endpoint
type TaskStatus struct {
	ID       int    `json:"id" mapstructure:"id"`
	Status   string `json:"state" mapstructure:"state"`
	WorkerID string `json:"worker_id" mapstructure:"worker_id"`
	// not part of resposne but utility functions fill it in to make boolean check easy.
	isRunning bool
}

type Task struct {
	Connector string `json:"connector" mapstructure:"connector"`
	Task      int    `json:"task" mapstructure:"task"`
}
type Connector struct {
	Name   string            `json:"name" mapstructure:"name"`
	Config map[string]string `json:"config" mapstructure:"config"`
	// Lots of information in the response that we ignore here
	Tasks []Task `json:"tasks" mapstructure:"tasks"`
}

type ConnectorStatus struct {
	Name      string            `json:"name" mapstructure:"name"`
	Connector map[string]string `json:"connector" mapstructure:"connector"`
	Tasks     []TaskStatus      `json:"tasks" mapstructure:"tasks"`
}

/*
Represents a connector plugin.
Useful in /connector-plugins endoint respone
*/
type ConnectorPlugin struct {
	Class   string `json:"class"`
	Type    string `json:"type"`
	Version string `json:"version"`
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

// Will list connectors without expanded info.
// Returns a list of strings
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

/*
Will list connectors and set expanded info for 'status' and 'info', effectively
 Getting all the info for the cluster
*/
func (admin *ConnectAdmin) ListConnectorsExpanded() (*ConnectClusterState, error) {

	type ConnectorResponse struct {
		Info struct {
			Name   string            `json:"name" mapstructure:"name"`
			Config map[string]string `json:"config mapstructure:"name"`
			Type   string            `json:"type"`
		} `json:"info"`
		Status struct {
			Name          string `json:"name"`
			ConnectorStat struct {
				State    string `json:"state"`
				WorkerID string `json:"worker_id"`
			} `json:"connector"`
			Tasks []Task `json:"tasks"`
			Type  string `json:"type"`
		} `json:"status"`
	}
	var clusterState ConnectClusterState
	clusterState.Connectors = make(map[string]Connector)
	log.Tracef("Requesting Expanded connector listing")
	respBody, _, err := admin.doREST("GET", "/connectors?expand=status&expand=info", nil)
	log.Debugf("respBody=%s\n", string(respBody))
	if err != nil {
		return &clusterState, err
	}
	err = json.Unmarshal(respBody, &clusterState)
	if err != nil {
		return &clusterState, err
	}

	var f interface{}
	err = json.Unmarshal(respBody, &f)
	itemsMap := f.(map[string]interface{})
	log.Debugf("itemsMap = %s\n", itemsMap)
	for name, infoBlob := range itemsMap {
		log.Debugf("Unmarshal name %s - infoBlob %v\n", name, infoBlob)
		resp := ConnectorResponse{}
		err = mapstructure.Decode(infoBlob, &resp)
		if err != nil {
			log.Panic(err)
			return &clusterState, err
		}
		log.Debugf("[%s] Conn = %v\n", name, resp)
		conn := Connector{
			Name:   resp.Info.Name,
			Config: resp.Info.Config,
			Tasks:  resp.Status.Tasks,
		}
		clusterState.Connectors[conn.Name] = conn

	}
	log.Debugf("Returning clusterstate %v\n", clusterState)
	return &clusterState, nil

}

// Get information about the connector. corresponds to  GET /connectors/(string: name)
func (admin *ConnectAdmin) GetConnectorInfo(connector string) (*Connector, error) {
	var resp Connector
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
	if err != nil {
		return &status, err
	}

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

func NewConnectorFromJson(jsonDefinition string) (*Connector, error) {
	log.Tracef("jsonDefinition =%s", jsonDefinition)
	var conn Connector
	err := json.Unmarshal([]byte(jsonDefinition), &conn)
	if err != nil {
		log.Trace("Failed to create Connector from JSON")
		return &conn, err
	}
	log.Tracef("Returning conn %v", conn)
	return &conn, nil

}

// Patch an existing connector config.
// returns  the name, and a boolean being true if a new connector was created instead, as the API will do that automatically.
func (admin *ConnectAdmin) PatchConnector(conn *Connector) (Connector, bool, error) {
	var createdNew bool = false
	var err error
	var respConnector Connector
	request := make(map[string]string)
	type createConnectorResponse struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
		Tasks  []Task            `json:"tasks"`
	}
	for confName, confVal := range conn.Config {
		request[confName] = confVal
	}
	var response createConnectorResponse
	uri := fmt.Sprintf("/connectors/%s/config", conn.Name)
	reqBody, err := json.Marshal(request)
	if err != nil {
		return respConnector, createdNew, err
	}

	respBody, statusCode, err := admin.doREST("PUT", uri, bytes.NewBuffer(reqBody))
	log.Debugf("respBody=%s", respBody)
	log.Debugf("ReqBody = %s, param conn %v", reqBody, conn)
	if statusCode < 200 || statusCode > 399 {
		return respConnector, createdNew, fmt.Errorf("request failed with status code %d\nResponse body: %s", statusCode, respBody)
	}
	if err != nil {
		return respConnector, createdNew, err
	}
	// This endpoint will return 201 Created for when it creates a new connector instead of updating (and a 200 OK for updates)
	if statusCode == 201 {
		createdNew = true
	}
	err = json.Unmarshal(respBody, &response)
	if err != nil {
		return respConnector, createdNew, err
	}
	respConnector.Name = response.Name
	respConnector.Config = response.Config
	respConnector.Tasks = response.Tasks
	log.Tracef("patch response var %v. Copied var = %v", response, respConnector)
	return respConnector, createdNew, nil

}

// Create a new Connector.
func (admin *ConnectAdmin) CreateConnector(conn *Connector) (string, error) {
	var name string
	type createConnectorRequest struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	type createConnectorResponse struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
		Tasks  []Task            `json:"tasks"`
	}
	var request createConnectorRequest
	request.Name = conn.Name
	request.Config = make(map[string]string)
	for confName, confVal := range conn.Config {
		request.Config[confName] = confVal
	}
	var response createConnectorResponse
	uri := "/connectors"
	reqBody, err := json.Marshal(request)
	if err != nil {
		return name, err
	}
	respBody, statusCode, err := admin.doREST("POST", uri, bytes.NewBuffer(reqBody))
	if statusCode < 200 || statusCode > 399 {
		return name, fmt.Errorf("request failed with status code %d\nResponse body: %s", statusCode, respBody)
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
		return fmt.Errorf("rebalance in progress.. Check status and try again later")
	}
	if statusCode < 200 || statusCode > 400 {
		return fmt.Errorf("request failed with status code %d\nResponse body: %s", statusCode, respBody)
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

		return fmt.Errorf("railed to restart task %d with status %d (response:%s)", taskID, statusCode, respBody)
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

		return fmt.Errorf("railed to restart connector %s (http: %d)(response:%s)", connector, statusCode, respBody)
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

/*
Retrieve the list of Connector plugins fron Connect Rest API
*/
func (admin *ConnectAdmin) ListPlugins() ([]ConnectorPlugin, error) {
	var plugins []ConnectorPlugin
	respBody, httpStatusCode, err := admin.doREST("GET", "/connector-plugins", nil)
	log.Tracef("listplugins respBody=%s", respBody)
	if err != nil {
		return plugins, err
	}
	if httpStatusCode != 200 {
		return plugins, fmt.Errorf("Unable to retrieve connector plugins as endpoint returned http status %d", httpStatusCode)
	}
	err = json.Unmarshal(respBody, &plugins)
	if err != nil {
		return plugins, err
	}
	return plugins, nil
}

/*
Reconcile connector status with config (yaml) contents
This is called by plan/apply calls.
*/
func (admin *ConnectAdmin) Reconcile(connectorConfigs map[string]Connector, dryRun bool) []ConnectorResult {
	var connectorResults []ConnectorResult
	/*
		- Get existing connectors and their definitions
		- For each connector defined in YAML,
		  - Check if an existing connector exists:
		    - If if exists -> PATCH (and restart?)
		    - If not -> CREATE
	*/
	existingConnectorNames, err := admin.ListConnectorsExpanded()
	if err != nil {
		log.Fatal("Failed to list connectors")
	}
	for _, connectorConf := range connectorConfigs {
		if _, exists := existingConnectorNames.Connectors[connectorConf.Name]; exists {
			log.Debugf("Connector '%v' exists already. New conf %v", connectorConf, connectorConf)
			if !dryRun {
				newConn, _, err := admin.PatchConnector(&connectorConf)
				if err != nil {
					log.Fatalf("Failed to create connector %v - error: %v", newConn, err)
				}
			} else {
				res := ConnectorResult{
					Name:       connectorConf.Name,
					NewConfigs: connectorConf.Config,
					OldConfigs: existingConnectorNames.Connectors[connectorConf.Name].Config,
				}
				connectorResults = append(connectorResults, res)
			}

		} else {
			if !dryRun {
				// New connector. Create it
				name, err := admin.CreateConnector(&connectorConf)
				if err != nil {
					log.Fatalf("Failed to create connector %s - error: %s", name, err)
				}
			} else {
				res := ConnectorResult{
					Name:       connectorConf.Name,
					NewConfigs: connectorConf.Config,
					OldConfigs: existingConnectorNames.Connectors[connectorConf.Name].Config,
				}
				connectorResults = append(connectorResults, res)
			}
		}
	}
	return connectorResults
}
