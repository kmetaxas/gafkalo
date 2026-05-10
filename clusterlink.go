package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/nsf/jsondiff"
	log "github.com/sirupsen/logrus"
)

// ClusterLink represents a cluster link configuration from input YAML
type ClusterLink struct {
	Name      string            `yaml:"name" json:"-"`
	ClusterID string            `yaml:"cluster_id" json:"remote_cluster_id"` // Remote cluster ID
	Configs   map[string]string `yaml:"configs" json:"configs"`
	// List of matching topics. This is used when listing/describing a cluster link.
	// Not when creating one.
	MatchedTopics []string `json:"-"`
}

type ClusterLinkRequest struct {
	ClusterID string              `yaml:"cluster_id" json:"remote_cluster_id"` // Remote cluster ID
	Configs   []ClusterLinkConfig `yaml:"configs" json:"configs"`
}
type ClusterLinkConfig struct {
	Name  string `yaml:"name" json:"name"`
	Value string `yaml:"value" json:"value"`
}

// When checking a new cluster link config against an running link,
// we want to get all the configs that differ.
type ClusterLinkConfigDiff struct {
	Name           string                              `json:"name" yaml:"name"`
	ChangedConfigs map[string]ClusterLinkChangedConfig `json:"changed_configs"`
}
type ClusterLinkChangedConfig struct {
	OldValue *string `json:"old" yaml:"old"`
	NewValue *string `json:"new" yaml:"new"`
}

// Emit a ClusterLink to JSON for passign to API
func (link *ClusterLink) AsJSON() ([]byte, error) {
	newLink := ClusterLinkRequest{
		ClusterID: link.ClusterID,
	}
	for name, value := range link.Configs {
		newConfig := ClusterLinkConfig{
			Name:  name,
			Value: value,
		}
		newLink.Configs = append(newLink.Configs, newConfig)
	}
	resp, err := json.Marshal(&newLink)
	return resp, err
}

// Populate the configs map from KafkaLinkConfigDataList object
func (link *ClusterLink) ConfigsFromKafkaLinkConfigDataList(configList *KafkaLinkConfigDataList) {
	for _, config := range configList.Data {
		// throw away defaults.
		if !config.IsDefault {
			log.Infof("Copying config %s - %s", config.Name, config.Value)
			link.Configs[config.Name] = config.Value
		}
	}
}

type ClusterLinkAdmin struct {
	Config               RestProxyConfig
	ClusterID            string // ID of the cluster we are managing (destination)
	ClusterLinkListCache map[string]ClusterLink
	TlsConfig            *tls.Config
}

// Confluent REST API types
type RestClusterLinkListResponse struct {
	Kind     string                `json:"kind"`
	Metadata RestMetadata          `json:"metadata"`
	Data     []RestClusterLinkItem `json:"data"`
}
type RestClusterLinkItem struct {
	Kind             string       `json:"kind"`
	Metadata         RestMetadata `json:"metadata"`
	RemoteClusterId  string       `json:"remote_cluster_id"`
	Name             string       `json:"link_name"`
	LinkId           string       `json:"link_id"`
	ClusterLinkId    string       `json:"cluster_link_id"`
	TopicNames       []string     `json:"topic_names"`
	LinkState        string       `json:"link_state"`
	RemoteLinkState  string       `json:"remote_link_state"`
	LinkError        string       `json:"link_error"`
	LinkErrorMessage string       `json:"link_error_message"`
}

/* Get a ClusterLink from a RestClusterLinkItem object*/
func NewClusterLinkFromRestuClusterLinkItem(item *RestClusterLinkItem) ClusterLink {
	link := ClusterLink{
		Name:          item.Name,
		ClusterID:     item.RemoteClusterId,
		Configs:       make(map[string]string),
		MatchedTopics: item.TopicNames,
	}
	return link
}

// Rest api response for cluster link config.
type KafkaLinkConfigDataList struct {
	Kind     string                `json:"kind"`
	Metadata RestMetadata          `json:"metadata"`
	Data     []KafkaLinkConfigData `json:"data"`
}

// Rest Api response for each individual config item inside a KafkaLinkConfigDataList
type KafkaLinkConfigData struct {
	Kind        string       `json:"kind"`
	Metadata    RestMetadata `json:"metadata"`
	ClusterID   string       `json:"cluster_id"`
	Name        string       `json:"name"`
	Value       string       `json:"value"`
	IsDefault   bool         `json:"is_default"`
	IsReadOnly  bool         `json:"is_read_only"`
	IsSensitive bool         `json:"is_sensitive"`
	Source      string       `json:"source"`
	Synonyms    []string     `json:"synonyms"`
	LinkName    string       `json:"link_name"`
}

type RestMetadata struct {
	Self string  `json:"self"`
	Next *string `json:"next"`
}

/* This is returned when status code is not 2xx in responses of Rest api */
type RestErrorResponse struct {
	ErrorCode    int    `json:"error_code,omitempty"`
	ErrorMessage string `json:"message,omitempty"`
}

func (e *RestErrorResponse) String() string {
	return fmt.Sprintf("Error code: %d, Error message: %s", e.ErrorCode, e.ErrorMessage)
}

/* The response payload of REST API for a Cluster Link */
type RestClusterResponse struct {
	Data []struct {
		Kind      string `json:"kind"`
		ClusterID string `json:"cluster_id"`
	} `json:"data"`
}

func NewClusterLinkAdmin(config RestProxyConfig) *ClusterLinkAdmin {
	// If no specific base path is defined, default to /kafka/v3 which is the default on
	// embedded REST proxy from Confluent.
	if config.BasePath == "" {
		config.BasePath = "/kafka/v3"
	}
	// Make sure a URL is defined.
	if config.Url == "" {
		log.Fatalf("When rest Proxy is defined, url can't be empty")
	}
	if config.ClusterID == "" {
		log.Fatalf("ClusterID not set for RestProxy config")
	}
	admin := &ClusterLinkAdmin{
		Config: config,
		// WE copy this because we intend so we need less refactoring when we retrieve it via REST in the future.
		ClusterID:            config.ClusterID,
		ClusterLinkListCache: make(map[string]ClusterLink),
	}

	if config.CAPath != "" {
		admin.TlsConfig = createTlsConfig(config.CAPath, config.SkipVerify)
	}

	return admin
}

func (admin *ClusterLinkAdmin) doREST(method, api string, payload io.Reader) ([]byte, int, error) {
	var httpStatus int = 0
	transport := &http.Transport{TLSClientConfig: admin.TlsConfig}
	hClient := http.Client{Transport: transport}
	uri := fmt.Sprintf("%s%s", admin.Config.Url, api)
	req, err := http.NewRequest(method, uri, payload)
	if err != nil {
		return nil, httpStatus, err
	}
	if admin.Config.Username != "" && admin.Config.Password != "" {
		req.SetBasicAuth(admin.Config.Username, admin.Config.Password)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	resp, err := hClient.Do(req)
	if err != nil {
		return nil, httpStatus, err
	}
	httpStatus = resp.StatusCode
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, httpStatus, err
	}
	return respBody, httpStatus, nil
}

func (admin *ClusterLinkAdmin) ListClusterLinks() (map[string]ClusterLink, error) {
	resp := make(map[string]ClusterLink)
	var clinkItems *RestClusterLinkListResponse
	var err error = nil
	url := fmt.Sprintf("%s/clusters/%s/links", admin.Config.BasePath, admin.Config.ClusterID)
	respBody, _, err := admin.doREST("GET", url, nil)
	if err != nil {
		return resp, err
	}
	err = json.Unmarshal(respBody, &clinkItems)
	for _, item := range clinkItems.Data {
		link := NewClusterLinkFromRestuClusterLinkItem(&item)
		// Now describe the configs for each link. Sadly that is a separate REST call...
		configList, err := admin.DescribeLinkConfig(item.Name)
		if err != nil {
			log.Errorf("Failed to get configs for link: %s", item.Name)
			return resp, err
		}
		log.Infof("Retrived configs for %s: %+v\n", link.Name, configList)
		link.ConfigsFromKafkaLinkConfigDataList(configList)
		resp[item.Name] = link
	}
	log.Debugf("ListClusterLinks got links =%+v\n", clinkItems)
	return resp, err
}

func (admin *ClusterLinkAdmin) DescribeLinkConfig(linkName string) (*KafkaLinkConfigDataList, error) {
	var resp *KafkaLinkConfigDataList
	url := fmt.Sprintf("%s/clusters/%s/links/%s/configs", admin.Config.BasePath, admin.Config.ClusterID, linkName)
	respBody, statusCode, err := admin.doREST("GET", url, nil)
	if err != nil {
		return resp, err
	}
	if statusCode != 200 {
		return resp, fmt.Errorf("Got http %d describing cluster link %s", statusCode, linkName)
	}
	err = json.Unmarshal(respBody, &resp)
	return resp, nil
}

func (admin *ClusterLinkAdmin) CreateClusterLink(name string, config *ClusterLink, dryRun bool) error {
	url := fmt.Sprintf("%s/clusters/%s/links?link_name=%s", admin.Config.BasePath, admin.Config.ClusterID, name)
	if dryRun {
		url = fmt.Sprintf("%s&validate_only=true&validate_link=true", url)
	}
	// Marshal the ClusterLink to Json
	data, err := config.AsJSON()
	if err != nil {
		log.Errorf("Failed to get ClusterLink payload as json for link %s with error %s", name, err)
	}
	log.Debugf("JSON payload to push to ClusterLInk Create: %s\n", data)
	respBody, statusCode, err := admin.doREST("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if statusCode != 201 {
		errorResp := RestErrorResponse{}
		err = json.Unmarshal(respBody, &errorResp)
		log.Debugf("Failed to create link: %+v", errorResp)
		return fmt.Errorf("Failed to create cluster link: %s", errorResp.String())
	}
	return nil
}

func (admin *ClusterLinkAdmin) DeleteClusterLink(name string, force bool, dryRun bool) error {
	var validateOnly string = "false"
	var forceParam string = "false"
	if dryRun {
		validateOnly = "true"
	}
	if force {
		forceParam = "true"
	}
	url := fmt.Sprintf("%s/clusters/%s/links/%s?validate_only=%s&force=%s", admin.Config.BasePath, admin.Config.ClusterID, name, validateOnly, forceParam)
	respBody, statusCode, err := admin.doREST("DELETE", url, nil)
	if err != nil {
		return err
	}
	if statusCode != 204 {
		errorResp := RestErrorResponse{}
		err = json.Unmarshal(respBody, &errorResp)
		log.Debugf("Failed to delete link: %+v", errorResp)
		return fmt.Errorf("Failed to delete cluster link: %s", errorResp.String())
	}
	return nil
}

// Alter a specific config for a cluster link. Maps to API:
// PUT /clusters/{cluster_id}/links/{link_name}/configs/{config_name}
// if ConfigValue is nil it will perform a DELETE,
func (admin *ClusterLinkAdmin) AlterClusterLinkConfig(linkName, configName string, configValue *string) error {
	var respBody []byte
	var statusCode int
	var err error

	url := fmt.Sprintf("%s/clusters/%s/links/%s/configs/%s", admin.Config.BasePath, admin.Config.ClusterID, linkName, configName)

	if configValue == nil {
		respBody, statusCode, err = admin.doREST("DELETE", url, nil)
	} else {
		// Create the request body with the config value
		requestBody := map[string]string{
			"value": *configValue,
		}
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		respBody, statusCode, err = admin.doREST("PUT", url, bytes.NewBuffer(payload))
	}
	if err != nil {
		return err
	}
	if statusCode != 204 {
		errorResp := RestErrorResponse{}
		err = json.Unmarshal(respBody, &errorResp)
		log.Debugf("Failed to update config %s for link: %+v", configName, errorResp)
		return fmt.Errorf("Failed to update config for cluster link: %s with error: %s", linkName, errorResp.String())
	}

	return nil
}

func (admin *ClusterLinkAdmin) NeedsUpdateByLinkName(name string, newConfig *ClusterLink) (bool, *ClusterLinkConfigDiff, error) {
	// Fetch the config first
	var hasChanged bool = false
	diff := new(ClusterLinkConfigDiff)
	diff.ChangedConfigs = make(map[string]ClusterLinkChangedConfig)
	var err error = nil
	var oldConfig ClusterLink
	oldConfig.Configs = make(map[string]string)

	oldConfigList, err := admin.DescribeLinkConfig(name)
	if err != nil {
		log.Debugf("NeedsUpdateByLinkName: Failed ot describe link\n")
		return false, nil, err
	}
	oldConfig.ConfigsFromKafkaLinkConfigDataList(oldConfigList)
	hasChanged, diff, err = admin.NeedsUpdate(&oldConfig, newConfig)
	for key, value := range diff.ChangedConfigs {
		oldValue := value.OldValue
		newValue := value.NewValue
		if newValue == nil {
		}
		log.Debugf("Key %s changed from %+v to %+v\n", key, *oldValue, SafeNullStr(newValue))
	}
	return hasChanged, diff, err
}

// Some configs are not default, not read only, not documented , not specified by us and still pop up in the describe configs.
// ( Example: remote.link.connection.mode )
// We document any config that should be removed from the ChangedConfigs in the diff here.
// compareConfigValues compares two config values, using JSON normalization if they appear to be JSON.
// This prevents false positives when comparing JSON configs with different formatting but same content.
func compareConfigValues(key, value1, value2 string) bool {
	// First try to parse both as JSON to check if they're valid JSON
	var json1, json2 interface{}
	err1 := json.Unmarshal([]byte(value1), &json1)
	err2 := json.Unmarshal([]byte(value2), &json2)

	// If both are valid JSON, use jsondiff for semantic comparison
	if err1 == nil && err2 == nil {
		opts := jsondiff.DefaultConsoleOptions()
		diff, _ := jsondiff.Compare([]byte(value1), []byte(value2), &opts)
		isEqual := (diff == jsondiff.FullMatch)

		if !isEqual {
			log.Debugf("Config '%s' JSON values differ semantically", key)
		} else {
			log.Debugf("Config '%s' JSON values are semantically equal despite formatting differences", key)
		}
		return isEqual
	}

	// If only one is JSON or neither is JSON, do string comparison
	isEqual := value1 == value2
	if !isEqual {
		log.Debugf("Config '%s' values differ (string comparison)", key)
	}
	return isEqual
}

func GetClusterLinkExcludeConfigsFromDiff() map[string]bool {
	resp := make(map[string]bool)
	resp["remote.link.connection.mode"] = true
	return resp
}

func (admin *ClusterLinkAdmin) NeedsUpdate(current *ClusterLink, new *ClusterLink) (bool, *ClusterLinkConfigDiff, error) {
	var hasChanged bool = false
	var diff ClusterLinkConfigDiff
	ignoreFields := GetClusterLinkExcludeConfigsFromDiff()

	diff.ChangedConfigs = make(map[string]ClusterLinkChangedConfig)
	var err error = nil
	diff.Name = current.Name
	for key, value := range current.Configs {
		// Ignore any field in the ignorefields list
		if _, exists := ignoreFields[key]; exists {
			continue
		}
		newValue, exists := new.Configs[key]
		// Is config key deleted in new config set?
		if !exists {
			change := ClusterLinkChangedConfig{
				OldValue: &value,
				NewValue: nil,
			}
			diff.ChangedConfigs[key] = change
			hasChanged = true
		} else {
			// Use compareConfigValues to handle JSON comparison intelligently
			if !compareConfigValues(key, value, newValue) {
				change := ClusterLinkChangedConfig{
					OldValue: &value,
					NewValue: &newValue,
				}
				diff.ChangedConfigs[key] = change
				hasChanged = true
			}
		}
	}
	// Check for new configs that don't exist in current
	for key, value := range new.Configs {
		_, exists := current.Configs[key]
		if !exists {
			change := ClusterLinkChangedConfig{
				OldValue: nil,
				NewValue: &value,
			}
			diff.ChangedConfigs[key] = change
			hasChanged = true
		}
	}
	return hasChanged, &diff, err
}

func (admin *ClusterLinkAdmin) UpdateClusterLink(name string, config *ClusterLink) (bool, *ClusterLinkConfigDiff, error) {
	// Steps:
	// retrieve link data.
	// compare with new link. (call a function as it needs to be reusable)
	// Update changed configs.
	needsUpdate, diff, err := admin.NeedsUpdateByLinkName(name, config)
	if err != nil {
		return needsUpdate, diff, err
	}
	if needsUpdate {
		// For each key that needs updating ,perform a REST API CAll
		for key, changes := range diff.ChangedConfigs {
			newValue := changes.NewValue
			err = admin.AlterClusterLinkConfig(name, key, newValue)
			if err != nil {
				return needsUpdate, diff, err
			}
		}
	}
	return needsUpdate, diff, nil
}

func (admin *ClusterLinkAdmin) Reconcile(links map[string]ClusterLink, dryRun bool) []ClusterLinkResult {
	var results []ClusterLinkResult
	return results
}
