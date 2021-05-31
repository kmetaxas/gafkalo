package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

type Client_topic_roles struct {
	Topic    string `yaml:"topic"`
	Prefixed bool   `yaml:"prefixed"`
}

type Client_group_roles struct {
	Name     string   `yaml:"name"`
	Roles    []string `yaml:"roles"`
	Prefixed bool     `yaml:"prefixed"`
}
type Client struct {
	Principal        string               `yaml:"principal"`
	ConsumerFor      []Client_topic_roles `yaml:"consumer_for"`
	ProducerFor      []Client_topic_roles `yaml:"producer_for"`
	ResourceownerFor []Client_topic_roles `yaml:"resourceowner_for"`
	Groups           []Client_group_roles `yaml:"groups"`
}

type MDSAdmin struct {
	User                    string
	Password                string
	Url                     string
	SchemaRegistryClusterID string
	ConnectClusterId        string
	KafkaClusterID          string
	KSQLClusterID           string
}

const (
	CTX_KAFKA   = iota
	CTX_SR      = iota
	CTX_KSQL    = iota
	CTX_CONNECT = iota
)

type MDSContext struct {
	Clusters map[string]string `json:"clusters"` // cluster name : cluster ID map
}

func NewMDSAdmin(config MDSConfig) *MDSAdmin {
	admin := MDSAdmin{
		User:     config.User,
		Password: config.Password,
	}
	admin.Url = config.Url
	admin.KafkaClusterID = admin.getKafkaClusterID()
	admin.SchemaRegistryClusterID = config.SchemaRegistryClusterID
	admin.ConnectClusterId = config.ConnectClusterId
	admin.KSQLClusterID = config.KSQLClusterID
	return &admin
}

func (admin *MDSAdmin) getKafkaClusterID() string {
	url := fmt.Sprintf("%s/security/1.0/metadataClusterId", admin.Url)
	resp, err := admin.doRest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}
	return string(resp)
}

// low level function that sets a role binding.
func (admin *MDSAdmin) SetRoleBinding(context int, res_type string, res_name string, principal string, roles []string, prefixed bool, dry_run bool) {
	type MDSResourcePattern struct {
		ResourceType string `json:"resourceType"`
		Name         string `json:"name"`
		PatternType  string `json:"patternType"`
	}
	type MDSRequest struct {
		Scope            MDSContext           `json:"scope"`
		ResourcePatterns []MDSResourcePattern `json:"resourcePatterns"`
	}
	ctx := admin.getContext(context)
	var reqData MDSRequest
	reqData.Scope.Clusters = ctx.Clusters
	resPattern := MDSResourcePattern{
		ResourceType: res_type,
		Name:         res_name,
	}
	if prefixed {
		resPattern.PatternType = "PREFIXED"
	} else {
		resPattern.PatternType = "LITERAL"
	}
	reqData.ResourcePatterns = append(reqData.ResourcePatterns, resPattern)
	// Now should be read to do the POST
	for _, role := range roles {
		// note we queryescape principals as they contain a ':' and fuck-up the endpoint and we get a 404
		url := fmt.Sprintf("%s/security/1.0/principals/%s/roles/%s/bindings", admin.Url, url.QueryEscape(principal), role)
		payload, err := json.Marshal(reqData)
		if err != nil {
			log.Fatal(err)
		}
		resp, err := admin.doRest("POST", url, bytes.NewBuffer(payload))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Got response: %s--\n", resp)
	}

}

func (admin *MDSAdmin) doRest(method string, url string, payload io.Reader) ([]byte, error) {
	fmt.Printf("Will make %s call to '%s' with payload: %s\n", method, url, payload)
	hClient := http.Client{}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		log.Fatal(err)
	}
	req.SetBasicAuth(admin.User, admin.Password)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")

	resp, err := hClient.Do(req)
	if err != nil {
		log.Println(err)
	}
	respBody, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Println(err)
	}
	return respBody, nil

}

// context is one of CTX_* constants
func (admin *MDSAdmin) getContext(context int) MDSContext {
	var ctx MDSContext
	ctx.Clusters = make(map[string]string)
	// kafka-cluster is always present.
	ctx.Clusters["kafka-cluster"] = admin.KafkaClusterID

	switch context {
	case CTX_KAFKA:
		return ctx
	case CTX_SR:
		ctx.Clusters["schema-registry-cluster"] = admin.SchemaRegistryClusterID
	case CTX_KSQL:
		ctx.Clusters["ksql-cluster"] = admin.KSQLClusterID
	case CTX_CONNECT:
		ctx.Clusters["connect-cluster"] = admin.ConnectClusterId
	}
	return ctx
}
