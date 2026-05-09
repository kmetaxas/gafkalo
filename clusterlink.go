package main

import (
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
	Name      string            `yaml:"name"`
	ClusterID string            `yaml:"cluster_id"` // Remote cluster ID
	Configs   map[string]string `yaml:"configs"`
}

type ClusterLinkAdmin struct {
	Config               RestProxyConfig
	ClusterID            string // ID of the cluster we are managing (destination)
	ClusterLinkListCache map[string]ClusterLink
	TlsConfig            *tls.Config
}

// Confluent REST API types
type RestClusterLinkListResponse struct {
	Kind     string               `json:"kind"`
	Metadata RestMetadata         `json:"metadata"`
	Data     *RestClusterLinkItem `json:"data"`
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

type RestLinkResponse struct {
	Kind         string            `json:"kind"`
	Metadata     RestMetadata      `json:"metadata"`
	LinkName     string            `json:"link_name"`
	LinkID       string            `json:"link_id"`
	ClusterID    string            `json:"cluster_id"`
	Configs      map[string]string `json:"configs"`
	ErrorCode    int               `json:"error_code,omitempty"`
	ErrorMessage string            `json:"message,omitempty"`
}

type RestMetadata struct {
	Self string  `json:"self"`
	Next *string `json:"next"`
}

/* The request payload of REST API for a Cluster Link */
type RestLinkPayload struct {
	LinkName  string            `json:"link_name,omitempty"`
	ClusterID string            `json:"cluster_id,omitempty"` // Remote cluster ID
	Configs   map[string]string `json:"configs,omitempty"`
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
	fmt.Printf("Links=%v+\n")
	return resp, err
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
