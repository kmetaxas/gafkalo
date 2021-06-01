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

type ClientTopicRole struct {
	Topic    string `yaml:"topic"`
	Prefixed bool   `yaml:"prefixed"`
	Strict   bool   `yaml:"strict" default:"false"`
}

type ClientGroupRole struct {
	Name     string   `yaml:"name"`
	Roles    []string `yaml:"roles"`
	Prefixed bool     `yaml:"prefixed" default:"true"`
}
type Client struct {
	Principal        string            `yaml:"principal"`
	ConsumerFor      []ClientTopicRole `yaml:"consumer_for"`
	ProducerFor      []ClientTopicRole `yaml:"producer_for"`
	ResourceownerFor []ClientTopicRole `yaml:"resourceowner_for"`
	Groups           []ClientGroupRole `yaml:"groups"`
}

type MDSAdmin struct {
	User                    string
	Password                string
	Url                     string
	SchemaRegistryClusterID string
	ConnectClusterId        string
	KafkaClusterID          string
	KSQLClusterID           string
	ClientCache             []MDSRolebindings
}

const (
	CTX_KAFKA   = iota // 0
	CTX_SR      = iota // 1
	CTX_KSQL    = iota // 2
	CTX_CONNECT = iota // 3
)

type MDSRolebindings struct {
	DeveloperRead  []MDSResourcePattern `json:"DeveloperRead"`
	DeveloperWrite []MDSResourcePattern `json:"DeveloperWrite"`
	ResourceOwner  []MDSResourcePattern `json:"ResourceOwner"`
}

// Matches the response/request objects of MDS on resource patterns
type MDSResourcePattern struct {
	ResourceType string `json:"resourceType"`
	Name         string `json:"name"`
	PatternType  string `json:"patternType"`
}

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
func (admin *MDSAdmin) SetRoleBinding(context int, res_type string, res_name string, principal string, roles []string, prefixed bool, dry_run bool) error {
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
	resPattern.PatternType = admin.getPrefixStr(prefixed)
	reqData.ResourcePatterns = append(reqData.ResourcePatterns, resPattern)
	// Now should be read to do the POST
	for _, role := range roles {
		// note we queryescape principals as they contain a ':' and fuck-up the endpoint and we get a 404
		url := fmt.Sprintf("%s/security/1.0/principals/%s/roles/%s/bindings", admin.Url, url.QueryEscape(principal), role)
		payload, err := json.Marshal(reqData)
		if err != nil {
			return err
		}
		_, err = admin.doRest("POST", url, bytes.NewBuffer(payload))
		if err != nil {
			return err
		}

	}
	return nil

}

func (admin *MDSAdmin) doRest(method string, url string, payload io.Reader) ([]byte, error) {
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

// For a given principal name, get the rolebindings assigned
func (admin *MDSAdmin) getRoleBindingsForPrincipalContext(principal string, context int) MDSRolebindings {

	type MDSRolebindingResponseInner map[string]MDSRolebindings
	ctx := admin.getContext(context)
	url := fmt.Sprintf("%s/security/1.0/lookup/principal/%s/resources", admin.Url, principal)

	payload, err := json.Marshal(ctx)
	if err != nil {
		log.Fatal(err)
	}
	resp, err := admin.doRest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		log.Fatal(err)
	}
	var respObj MDSRolebindingResponseInner
	err = json.Unmarshal(resp, &respObj)
	if err != nil {
		log.Fatal(err)
	}
	return respObj[principal]

}

func (admin *MDSAdmin) getPrefixStr(prefix bool) string {
	var rval string
	if prefix {
		rval = "PREFIXED"
	} else {
		rval = "LITERAL"
	}
	return rval

}
func (admin *MDSAdmin) getContextValByID(context int) string {
	var rval string
	switch context {
	case CTX_KAFKA:
		rval = admin.KafkaClusterID
	case CTX_SR:
		rval = admin.SchemaRegistryClusterID
	case CTX_KSQL:
		rval = admin.KSQLClusterID
	case CTX_CONNECT:
		rval = admin.ConnectClusterId
	}
	return rval
}
func (admin *MDSAdmin) getRoleBindingsForPrincipal(principal string) MDSRolebindings {
	var allRoles MDSRolebindings
	var respObj MDSRolebindings
	// Get rolebindings for each context and slowly construct the allRoles obj
	contexts := []int{CTX_KAFKA, CTX_SR, CTX_CONNECT, CTX_KSQL}
	for _, ctx := range contexts {
		// If context is empty, skip this
		if admin.getContextValByID(ctx) == "" {
			continue
		}
		respObj = admin.getRoleBindingsForPrincipalContext(principal, ctx)

		allRoles.DeveloperRead = append(allRoles.DeveloperRead, respObj.DeveloperRead...)
		allRoles.DeveloperWrite = append(allRoles.DeveloperWrite, respObj.DeveloperWrite...)
		allRoles.ResourceOwner = append(allRoles.ResourceOwner, respObj.ResourceOwner...)
	}

	return allRoles
}

func (admin *MDSAdmin) doConsumerFor(topic string, principal string, prefixed bool, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	// TODO construct ClientResult
	var subjects []string
	if !prefixed {
		subjects = []string{fmt.Sprintf("%s-value", topic), fmt.Sprintf("%s-key", topic)}

	} else {
		subjects = []string{topic}
	}
	res = append(res, ClientResult{Principal: principal, ResourceType: "Topic", ResourceName: topic, Role: "DeveloperRead", PatternType: admin.getPrefixStr(prefixed)})
	err = admin.SetRoleBinding(CTX_KAFKA, "Topic", topic, principal, []string{"DeveloperRead"}, prefixed, dryRun)
	if err != nil {
		return res, err
	}
	// Set DeveloperRead on subject value
	for _, subject := range subjects {
		res = append(res, ClientResult{Principal: principal, ResourceType: "Subject", ResourceName: subject, Role: "DeveloperRead", PatternType: admin.getPrefixStr(prefixed)})
		err = admin.SetRoleBinding(CTX_SR, "Subject", subject, principal, []string{"DeveloperRead"}, prefixed, dryRun)
		if err != nil {
			return res, err
		}
	}
	return res, nil
}
func (admin *MDSAdmin) doProducerFor(topic string, principal string, prefixed bool, strict bool, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	// TODO construct ClientResult
	// Default role for SR is Read but if strict=false then add Write
	srRoles := []string{"DeveloperRead"}
	var subjects []string
	if !prefixed {
		subjects = []string{fmt.Sprintf("%s-value", topic), fmt.Sprintf("%s-key", topic)}
	} else {
		subjects = []string{topic}

	}
	if !strict {
		srRoles = append(srRoles, "DeveloperWrite")
	}
	res = append(res, ClientResult{Principal: principal, ResourceType: "Topic", ResourceName: topic, Role: "DeveloperWrite", PatternType: admin.getPrefixStr(prefixed)})
	err = admin.SetRoleBinding(CTX_KAFKA, "Topic", topic, principal, []string{"DeveloperWrite"}, prefixed, dryRun)
	if err != nil {
		return res, err
	}
	for _, subject := range subjects {
		// Set DeveloperRead on subject value
		for _, sRole := range srRoles {
			res = append(res, ClientResult{Principal: principal, ResourceType: "Subject", ResourceName: subject, Role: sRole, PatternType: admin.getPrefixStr(prefixed)})
		}
		err = admin.SetRoleBinding(CTX_SR, "Subject", subject, principal, srRoles, prefixed, dryRun)
		if err != nil {
			return res, err
		}
	}

	return res, nil

}

func (admin *MDSAdmin) doResourceOwnerFor(topic string, principal string, prefixed bool, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	roles := []string{"ResourceOwner"}
	var subjects []string
	// TODO construct ClientResult
	if !prefixed {
		subjects = []string{fmt.Sprintf("%s-value", topic), fmt.Sprintf("%s-key", topic)}
	} else {
		subjects = []string{topic}

	}

	res = append(res, ClientResult{Principal: principal, ResourceType: "Topic", ResourceName: topic, Role: "ResourceOwner", PatternType: admin.getPrefixStr(prefixed)})
	err = admin.SetRoleBinding(CTX_KAFKA, "Topic", topic, principal, roles, prefixed, dryRun)
	if err != nil {
		return res, err
	}
	for _, subject := range subjects {
		res = append(res, ClientResult{Principal: principal, ResourceType: "Subject", ResourceName: subject, Role: "ResourceOwner", PatternType: admin.getPrefixStr(prefixed)})
		err = admin.SetRoleBinding(CTX_SR, "Subject", subject, principal, roles, prefixed, dryRun)
		if err != nil {
			return res, err
		}
	}

	return res, nil

}
func (admin *MDSAdmin) Reconcile(clients []Client, dryRun bool) []ClientResult {
	var clientResults []ClientResult
	for _, client := range clients {
		for _, consumerRole := range client.ConsumerFor {
			clientRes, err := admin.doConsumerFor(consumerRole.Topic, client.Principal, consumerRole.Prefixed, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
		for _, producerRole := range client.ProducerFor {
			clientRes, err := admin.doProducerFor(producerRole.Topic, client.Principal, producerRole.Prefixed, producerRole.Strict, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
		for _, resourceOwnerRole := range client.ResourceownerFor {
			clientRes, err := admin.doProducerFor(resourceOwnerRole.Topic, client.Principal, resourceOwnerRole.Prefixed, resourceOwnerRole.Strict, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
	}
	return clientResults
}
