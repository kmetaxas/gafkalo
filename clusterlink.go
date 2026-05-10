package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

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
	fmt.Printf("Links=%+v\n", clinkItems)
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

func (admin *ClusterLinkAdmin) Reconcile(links map[string]ClusterLink, dryRun bool) []ClusterLinkResult {
	var results []ClusterLinkResult
	return results
}

func (admin *ClusterLinkAdmin) needsUpdate(existing, desired map[string]string) bool {
	for k, v := range desired {
		if existingVal, ok := existing[k]; !ok || existingVal != v {
			// Be careful with secrets (jaas config, passwords).
			// Existing might be redacted or hashed?
			// For now, simple comparison.
			// TODO: Handle sensitive redaction comparison if needed.
			// Usually API returns redacted secrets as [hidden] or similar.
			// If existing is [hidden] and we have a value, we might assume update needed?
			// Or we assume "trust user intent".

			if strings.Contains(existingVal, "[hidden]") {
				continue // Can't compare, assume no change? Or always update?
				// Safest is to Always Update if we can't be sure.
				// But that causes churn.
				// Let's assume always update if redacted.
				return true
			}
			return true
		}
	}
	return false
}
