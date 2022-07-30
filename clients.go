package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type ClientTopicRole struct {
	Topic      string `yaml:"topic"`
	IsLiteral  bool   `yaml:"isLiteral"`
	Strict     bool   `yaml:"strict"`
	Idempotent bool   `yaml:"idempotent"` //Only used for roles allowing producer
}

type ClientGroupRole struct {
	Name      string   `yaml:"name"`
	Roles     []string `yaml:"roles"`
	IsLiteral bool     `yaml:"isLiteral" default:"true"`
}

type ClientTransactionalIdRole struct {
	Name      string   `yaml:"name"`
	Roles     []string `yaml:"roles"`
	IsLiteral bool     `yaml:"isLiteral" default:"true"`
}
type Client struct {
	Principal        string                      `yaml:"principal"`
	ConsumerFor      []ClientTopicRole           `yaml:"consumer_for"`
	ProducerFor      []ClientTopicRole           `yaml:"producer_for"`
	ResourceownerFor []ClientTopicRole           `yaml:"resourceowner_for"`
	Groups           []ClientGroupRole           `yaml:"groups"`
	TransactionalIds []ClientTransactionalIdRole `yaml:"transactional_ids"`
}

func mergeClients(target Client, source Client) Client {
	target.ConsumerFor = append(target.ConsumerFor, source.ConsumerFor...)
	target.ProducerFor = append(target.ProducerFor, source.ProducerFor...)
	target.ResourceownerFor = append(target.ResourceownerFor, source.ResourceownerFor...)
	target.Groups = append(target.Groups, source.Groups...)
	return target

}

type MDSAdmin struct {
	User                    string
	Password                string
	Url                     string
	SchemaRegistryClusterID string
	ConnectClusterId        string
	KafkaClusterID          string
	KSQLClusterID           string
	RolebindingsCache       map[string]MDSRolebindings // [principal]rolbindings
	TlsConfig               *tls.Config
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
	admin.RolebindingsCache = make(map[string]MDSRolebindings)
	if config.CAPath != "" {
		admin.TlsConfig = createTlsConfig(config.CAPath, config.SkipVerify)
	}
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

// Check if a role is present in Cache. Use the ClientResult object to do so
func (admin *MDSAdmin) roleExists(role ClientResult, existingRoles MDSRolebindings) bool {

	var found bool = false
	switch role.Role {
	case "DeveloperRead":
		found = compareResultWithResourcePatterns(role, existingRoles.DeveloperRead)
	case "DeveloperWrite":
		found = compareResultWithResourcePatterns(role, existingRoles.DeveloperWrite)
	case "ResourceOwner":
		found = compareResultWithResourcePatterns(role, existingRoles.ResourceOwner)

	}
	return found
}
func compareResultWithResourcePatterns(res ClientResult, patterns []MDSResourcePattern) bool {
	for _, pattern := range patterns {
		if (res.ResourceName == pattern.Name) && (res.ResourceType == pattern.ResourceType) && (res.PatternType == pattern.PatternType) {
			return true
		}
	}
	return false
}

// low level function that sets a role binding.
func (admin *MDSAdmin) SetRoleBinding(context int, res_type string, res_name string, principal string, roles []string, isLiteral bool, dry_run bool) error {
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
	resPattern.PatternType = getPrefixStr(isLiteral)
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

	transport := &http.Transport{TLSClientConfig: admin.TlsConfig}
	hClient := http.Client{Transport: transport}
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

func getPrefixStr(isLiteral bool) string {
	var rval string
	if !isLiteral {
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

	// Check if it is in the Cache and return it, otherwise it can become a very expensive operation
	if rolebindings, exists := admin.RolebindingsCache[principal]; exists {
		return rolebindings
	}

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
	// Set the cache
	admin.RolebindingsCache[principal] = allRoles
	return allRoles
}

func (admin *MDSAdmin) doConsumerFor(topic string, principal string, isLiteral bool, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	var subjects []string
	existingRoles := admin.getRoleBindingsForPrincipal(principal)
	if isLiteral {
		subjects = []string{fmt.Sprintf("%s-value", topic), fmt.Sprintf("%s-key", topic)}

	} else {
		subjects = []string{topic}
	}
	newRole := ClientResult{Principal: principal, ResourceType: "Topic", ResourceName: topic, Role: "DeveloperRead", PatternType: getPrefixStr(isLiteral)}
	if !dryRun && !admin.roleExists(newRole, existingRoles) {
		err = admin.SetRoleBinding(CTX_KAFKA, "Topic", topic, principal, []string{"DeveloperRead"}, isLiteral, dryRun)
		if err != nil {
			return res, err
		}
	}
	if !admin.roleExists(newRole, existingRoles) {
		res = append(res, newRole)
	}
	// Set DeveloperRead on subject value
	for _, subject := range subjects {
		newRole = ClientResult{Principal: principal, ResourceType: "Subject", ResourceName: subject, Role: "DeveloperRead", PatternType: getPrefixStr(isLiteral)}
		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_SR, "Subject", subject, principal, []string{"DeveloperRead"}, isLiteral, dryRun)
			if err != nil {
				return res, err
			}
		}
		if !admin.roleExists(newRole, existingRoles) {
			res = append(res, newRole)
		}
	}
	return res, nil
}
func (admin *MDSAdmin) doProducerFor(topic string, principal string, isLiteral, strict, idempotent, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	// Default role for SR is Read but if strict=false then add Write
	srRoles := []string{"DeveloperRead"}
	var subjects []string
	existingRoles := admin.getRoleBindingsForPrincipal(principal)
	if isLiteral {
		subjects = []string{fmt.Sprintf("%s-value", topic), fmt.Sprintf("%s-key", topic)}
	} else {
		subjects = []string{topic}

	}
	if !strict {
		srRoles = append(srRoles, "DeveloperWrite")
	}

	// ADD developerWrite to Topic resource
	newRole := ClientResult{Principal: principal, ResourceType: "Topic", ResourceName: topic, Role: "DeveloperWrite", PatternType: getPrefixStr(isLiteral)}
	if !dryRun && !admin.roleExists(newRole, existingRoles) {
		err = admin.SetRoleBinding(CTX_KAFKA, "Topic", topic, principal, []string{"DeveloperWrite"}, isLiteral, dryRun)
		if err != nil {
			return res, err
		}
	}
	if !admin.roleExists(newRole, existingRoles) {
		res = append(res, newRole)
	}
	// Add idempotent cluster role if requested
	if idempotent {
		newRole := ClientResult{Principal: principal, ResourceType: "Cluster", ResourceName: "kafka-cluster", Role: "DeveloperWrite", PatternType: "LITERAL"}
		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_KAFKA, "Cluster", "kafka-cluster", principal, []string{"DeveloperWrite"}, true, dryRun)
			if err != nil {
				return res, err
			}
		}
		if !admin.roleExists(newRole, existingRoles) {
			res = append(res, newRole)
		}
		// Add idempotent cluster role if requested
	}
	// Add idempotent role if to result
	if !admin.roleExists(newRole, existingRoles) {
		res = append(res, newRole)
	}
	for _, subject := range subjects {
		// Prepare plan/result
		for _, sRole := range srRoles {
			newRole := ClientResult{Principal: principal, ResourceType: "Subject", ResourceName: subject, Role: sRole, PatternType: getPrefixStr(isLiteral)}
			if !admin.roleExists(newRole, existingRoles) {
				res = append(res, newRole)
			}
		}
		// Actually change it, if needed
		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_SR, "Subject", subject, principal, srRoles, isLiteral, dryRun)
			if err != nil {
				return res, err
			}
		}
	}

	return res, nil

}

func (admin *MDSAdmin) doResourceOwnerFor(topic string, principal string, isLiteral, idempotent, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	roles := []string{"ResourceOwner"}
	var subjects []string
	existingRoles := admin.getRoleBindingsForPrincipal(principal)
	if isLiteral {
		subjects = []string{fmt.Sprintf("%s-value", topic), fmt.Sprintf("%s-key", topic)}
	} else {
		subjects = []string{topic}

	}

	// Add ResourceOwner on the topic
	newRole := ClientResult{Principal: principal, ResourceType: "Topic", ResourceName: topic, Role: "ResourceOwner", PatternType: getPrefixStr(isLiteral)}
	if !dryRun && !admin.roleExists(newRole, existingRoles) {
		err = admin.SetRoleBinding(CTX_KAFKA, "Topic", topic, principal, roles, isLiteral, dryRun)
	}
	if !admin.roleExists(newRole, existingRoles) {
		res = append(res, newRole)
	}

	if err != nil {
		return res, err
	}
	// Add IdempotentWrite
	if idempotent {
		newRole := ClientResult{Principal: principal, ResourceType: "Cluster", ResourceName: "kafka-cluster", Role: "ResourceOwner", PatternType: "LITERAL"}
		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_KAFKA, "Cluster", "kafka-cluster", principal, []string{"ResourceOwner"}, true, dryRun)
			if err != nil {
				return res, err
			}
		}
		if !admin.roleExists(newRole, existingRoles) {
			res = append(res, newRole)
		}
		// Add idempotent cluster role if requested
	}
	// Add Schemaregistry rolebindings
	for _, subject := range subjects {
		newRole = ClientResult{Principal: principal, ResourceType: "Subject", ResourceName: subject, Role: "ResourceOwner", PatternType: getPrefixStr(isLiteral)}

		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_SR, "Subject", subject, principal, roles, isLiteral, dryRun)
		}
		if !admin.roleExists(newRole, existingRoles) {
			res = append(res, newRole)
		}
		if err != nil {
			return res, err
		}
	}

	return res, nil

}

/*
Create any Consumer Group permission
If dry_run, only return a ClientResult so that a Plan can be created.
*/
func (admin *MDSAdmin) doGroupRoleFor(groupName, principal string, roles []string, isLiteral bool, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	var actualRoles []string
	existingRoles := admin.getRoleBindingsForPrincipal(principal)
	if len(roles) > 0 {
		actualRoles = roles
	} else {
		actualRoles = []string{"DeveloperRead"}
	}
	for _, roleName := range actualRoles {
		newRole := ClientResult{Principal: principal, ResourceType: "Group", ResourceName: groupName, Role: roleName, PatternType: getPrefixStr(isLiteral)}
		if !admin.roleExists(newRole, existingRoles) {
			res = append(res, newRole)
		}
		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_KAFKA, "Group", groupName, principal, []string{roleName}, isLiteral, dryRun)
			if err != nil {
				return res, err
			}
		}
	}

	return res, err
}

/*
Create any needed TransactionalId rolebindings
*/
func (admin *MDSAdmin) doTransactionalIdRole(transactionalIdName, principal string, roles []string, isLiteral, dryRun bool) ([]ClientResult, error) {
	var res []ClientResult
	var err error
	var actualRoles []string
	existingRoles := admin.getRoleBindingsForPrincipal(principal)
	if len(roles) > 0 {
		actualRoles = roles
	} else {
		actualRoles = []string{"DeveloperWrite"}
	}
	for _, roleName := range actualRoles {
		log.Trace("doTransactionalIdRole for role: %s", roleName)
		newRole := ClientResult{Principal: principal, ResourceType: "TransactionalId", ResourceName: transactionalIdName, Role: roleName, PatternType: getPrefixStr(isLiteral)}
		if !admin.roleExists(newRole, existingRoles) {
			res = append(res, newRole)
		}
		if !dryRun && !admin.roleExists(newRole, existingRoles) {
			err = admin.SetRoleBinding(CTX_KAFKA, "TransactionalId", transactionalIdName, principal, []string{roleName}, isLiteral, dryRun)
			if err != nil {
				return res, err
			}
		}
	}

	return res, err

}

func (admin *MDSAdmin) Reconcile(clients map[string]Client, dryRun bool) []ClientResult {
	var clientResults []ClientResult
	for _, client := range clients {
		for _, consumerRole := range client.ConsumerFor {
			clientRes, err := admin.doConsumerFor(consumerRole.Topic, client.Principal, consumerRole.IsLiteral, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
		for _, producerRole := range client.ProducerFor {
			clientRes, err := admin.doProducerFor(producerRole.Topic, client.Principal, producerRole.IsLiteral, producerRole.Strict, producerRole.Idempotent, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
		for _, resourceOwnerRole := range client.ResourceownerFor {
			clientRes, err := admin.doResourceOwnerFor(resourceOwnerRole.Topic, client.Principal, resourceOwnerRole.IsLiteral, resourceOwnerRole.Idempotent, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
		// Add any Consumer Group permissions defined
		for _, groupRole := range client.Groups {
			clientRes, err := admin.doGroupRoleFor(groupRole.Name, client.Principal, groupRole.Roles, groupRole.IsLiteral, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
		// Add Transactional Ids
		for _, transactionalIdRole := range client.TransactionalIds {
			clientRes, err := admin.doTransactionalIdRole(transactionalIdRole.Name, client.Principal, transactionalIdRole.Roles, transactionalIdRole.IsLiteral, dryRun)
			if err != nil {
				log.Fatal(err)
			}
			clientResults = append(clientResults, clientRes...)
		}
	}
	return clientResults
}
