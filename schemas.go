package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/kmetaxas/srclient"
	"github.com/nsf/jsondiff"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

// Schema object to keep track of desired/requested state
type Schema struct {
	SubjectName   string
	SchemaPath    string `yaml:"schema"`
	Compatibility string `yaml:"compatibility"`
	SchemaData    string
	SchemaType    srclient.SchemaType `yaml:"schema_type"`
}

// Compare the schema text of the two objects and return a tuple.
// with a boolean if they are equal and a string with changes
// Assumes JSON.
func (s *Schema) SchemaDiff(other string) (bool, string) {
	var isEqual bool
	var diffString string
	var cmpRes jsondiff.Difference
	opts := jsondiff.DefaultConsoleOptions()
	cmpRes, diffString = jsondiff.Compare([]byte(s.SchemaData), []byte(other), &opts)
	isEqual = (cmpRes == jsondiff.FullMatch)

	return isEqual, diffString
}

// Create a new Schema instance. Handles defaults etc
// Does not register it
func CreateSchema(SubjectName string, SchemaPath string, Compatibility string, SchemaType srclient.SchemaType) (Schema, error) {
	var newSchema Schema
	newSchema.SubjectName = SubjectName
	newSchema.SchemaPath = SchemaPath
	newSchema.Compatibility = Compatibility

	// Load the schema data
	pathToSchemaFile := normalizeSchemaPath(SchemaPath)
	if pathToSchemaFile != "" && SchemaPath != "" {
		data, err := ioutil.ReadFile(pathToSchemaFile)
		if err != nil {
			log.Fatalf("Unable to create schema with Error: %s\n", err)
		}
		newSchema.SchemaData = string(data)
		if SchemaType == "" {
			newSchema.SchemaType = "AVRO"
		} else {
			newSchema.SchemaType = SchemaType
		}
	}
	return newSchema, nil
}

func (s *Schema) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawSchema Schema
	raw := rawSchema{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	var err error
	*s, err = CreateSchema(raw.SubjectName, raw.SchemaPath, raw.Compatibility, raw.SchemaType)
	if err != nil {
		return err
	}
	return nil
}

// SRAdmin 'class'
type SRAdmin struct {
	Client       srclient.SchemaRegistryClient
	SubjectCache []string
	GlobalCompat string
	url          string
	user         string
	pass         string
	TlsConfig    *tls.Config
	UseSRCache   bool //Use schema registy cache for requests
	SRCache      *SchemaRegistryCache
}

// Create a new SRAdmin
func NewSRAdmin(config *Configuration) SRAdmin {
	var timeout time.Duration = 5
	// If TLS config is provided, construct HttpClient and use CreateSchemaRegistryClientWithOptions to construct Schema registry client.
	var srClient *srclient.SchemaRegistryClient
	var tlsConfig *tls.Config

	if config.Connections.Schemaregistry.CAPath != "" {
		log.Debug("Setting custom TLS config for Schema registry client")
		tlsConfig = createTlsConfig(config.Connections.Schemaregistry.CAPath, config.Connections.Schemaregistry.SkipVerify)
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		httpClient := &http.Client{Transport: transport}
		srClient = srclient.CreateSchemaRegistryClientWithOptions(config.Connections.Schemaregistry.Url, httpClient, 1)

	} else {
		srClient = srclient.CreateSchemaRegistryClient(config.Connections.Schemaregistry.Url)
	}
	// Set a default for Timeouts or use config provided one
	if config.Connections.Schemaregistry.Timeout != 0 {
		timeout = config.Connections.Schemaregistry.Timeout
	}
	srClient.SetTimeout(timeout * time.Second)
	if config.Connections.Schemaregistry.Username != "" && config.Connections.Schemaregistry.Password != "" {
		srClient.SetCredentials(config.Connections.Schemaregistry.Username, config.Connections.Schemaregistry.Password)
	}

	sradmin := SRAdmin{Client: *srClient, user: config.Connections.Schemaregistry.Username, pass: config.Connections.Schemaregistry.Password}
	sradmin.url = config.Connections.Schemaregistry.Url
	if config.Connections.Schemaregistry.SkipRestForReads {
		sradmin.UseSRCache = true
	}
	if sradmin.UseSRCache {
		srCache, err := NewSchemaRegistryCache(config)
		if err != nil {
			log.Fatal(err)
		}
		sradmin.SRCache = srCache
		sradmin.SRCache.ReadSchemaTopic("_schemas")
		sradmin.SubjectCache = sradmin.SRCache.GetSubjects()
	} else {
		subjects, err := sradmin.Client.GetSubjects()
		if err != nil {
			log.Fatalf("Unable to get SR subjects: %s\n", err)
		}
		sradmin.SubjectCache = subjects
	}
	return sradmin
}

func (admin *SRAdmin) RegisterSubject(schema Schema) (int, error) {

	// Create a value subject (isKey = false)
	newSchema, err := admin.Client.CreateSchema(schema.SubjectName, schema.SchemaData, schema.SchemaType)
	if err != nil {
		return 0, err
	}
	return newSchema.Version(), nil
}

func (admin *SRAdmin) IsRegistered(schema Schema) error {
	return nil
}

// Facilitate Rest calls to Schema registry as some calls are not offered by srclient library yet.
func (admin *SRAdmin) makeRestCall(method string, uri string, payload io.Reader) ([]byte, error) {
	hClient := http.Client{}
	req, err := http.NewRequest(method, uri, payload)
	if err != nil {
		log.Fatal(err)
	}
	req.SetBasicAuth(admin.user, admin.pass)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	resp, err := hClient.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Println(err)
	}
	return respBody, nil

}

// Lookup a Schema object in the Registry.
// If it exists it will return the ID and the version for that subject
func (admin *SRAdmin) LookupSchema(schema Schema) (int, int, error) {
	var existingID, existingVersion int
	var err error
	if admin.UseSRCache {
		existingID, existingVersion, err = admin.SRCache.LookupSchemaForSubject(schema.SubjectName, schema.SchemaData)
	} else {
		type Response struct {
			Subject string `json:"subject"`
			Id      int    `json:"id"`
			Version int    `json:"version"`
			Schema  string `json:"schema"`
		}
		type Request struct {
			Schema string `json:"schema"`
		}
		type RequestNonAvro struct {
			Request
			SchemaType string `json:"schemaType"`
		}
		var request []byte
		var err error
		// field schemaType was introduced in confluent 5.5 along with protobuf/jsonschema support. Even though its in the docs, it raises an HTTP 422. So only pass it when schema type is not AVRO
		if schema.SchemaType != "AVRO" {
			request, err = json.Marshal(RequestNonAvro{Request: Request{Schema: string(schema.SchemaData)}, SchemaType: string(schema.SchemaType)})
		} else {
			request, err = json.Marshal(Request{Schema: string(schema.SchemaData)})
		}
		if err != nil {
			log.Fatalf("Failed to construct request for LookupSchema call: %s\n", err)
		}
		respBody, err := admin.makeRestCall("POST", fmt.Sprintf("%s/subjects/%s", admin.url, schema.SubjectName), bytes.NewBuffer(request))
		if err != nil {
			log.Fatal(err)
		}

		var respObj Response
		err = json.Unmarshal(respBody, &respObj)
		if err != nil {
			log.Fatalf("Failed unmarshaling response from POST: %s\n", err)
		}
		// Now check if we have the *latest* for this subject

		existingID = respObj.Id
		existingVersion = respObj.Version
		err = nil
	}
	return existingID, existingVersion, err
}

// set schema Compatibility
func (admin *SRAdmin) SetCompatibility(schema Schema, compatibility string) error {
	type RequestResponse struct {
		Compatibility string `json:"compatibility"`
	}
	reqObj := RequestResponse{Compatibility: compatibility}
	request, err := json.MarshalIndent(&reqObj, "", "\t")
	if err != nil {
		log.Fatal(err)
	}
	respBody, err := admin.makeRestCall("PUT", fmt.Sprintf("%s/config/%s", admin.url, schema.SubjectName), bytes.NewBuffer(request))

	if err != nil {
		log.Fatalf("Failed alter compatibility for schema %s with error: %s", schema.SubjectName, err)
	}
	var respObj RequestResponse
	err = json.Unmarshal(respBody, &respObj)
	if err != nil {
		log.Fatalf("Failed unmarshaling response from POST: %s\n", err)
	}

	return nil
}

// Get the compatibility setting
func (admin *SRAdmin) GetCompatibility(schema Schema) (string, error) {
	// If we can use the SR CAche then its quite simple. Otherwise the rest of this function deals with REST API details
	if admin.UseSRCache {
		return admin.SRCache.GetCompatibilityForSubject(schema.SubjectName), nil
	}

	type RequestResponse struct {
		// Confluent docs say the return field is `compatibility` but the example (and reality) is `compatibilityLevel`
		Compatibility string `json:"compatibilityLevel"`
	}
	respBody, err := admin.makeRestCall("GET", fmt.Sprintf("%s/config/%s", admin.url, schema.SubjectName), bytes.NewBuffer(nil))
	if err != nil {
		log.Printf("Failed to get compat:%s\n", err)
		return "", err
	}

	var respObj RequestResponse
	err = json.Unmarshal(respBody, &respObj)
	if err != nil {
		log.Printf("Failed to unmarshal response: %s\n", err)
		return "", nil
	}
	return respObj.Compatibility, nil
}

// Get the GLOBAL compatibility setting
func (admin *SRAdmin) GetCompatibilityGlobal() (string, error) {
	if admin.UseSRCache {
		return admin.SRCache.GetGlobalCompatibility(), nil
	}
	type RequestResponse struct {
		// Confluent docs say the return field is `compatibility` but the example (and reality) is `compatibilityLevel`
		Compatibility string `json:"compatibilityLevel"`
	}
	respBody, err := admin.makeRestCall("GET", fmt.Sprintf("%s/config", admin.url), bytes.NewBuffer(nil))
	if err != nil {
		log.Printf("Failed to get compat:%s\n", err)
		return "", err
	}

	var respObj RequestResponse
	err = json.Unmarshal(respBody, &respObj)
	if err != nil {
		log.Printf("Failed to unmarshal response: %s\n", err)
		return "", nil
	}
	return respObj.Compatibility, nil
}

// Reconcile actual with desired schema for a single schema
func (admin *SRAdmin) ReconcileSchema(schema Schema, dryRun bool) *SchemaResult {
	result := SchemaResult{
		SubjectName: schema.SubjectName,
	}
	globalCompat, err := admin.GetCompatibilityGlobal()
	if err != nil {
		log.Fatal(err)
	}
	// Only go through the whole schema check/update thing if SchemaData is not empty
	var mustRegister bool = false
	if schema.SchemaData != "" {
		existingID, _, err := admin.LookupSchema(schema)
		if err != nil {
			log.Printf("Reconcile Failed to lookup %s with %s\n", schema.SubjectName, err)
		}
		// No schemaID, so we must register
		if existingID == 0 {
			mustRegister = true
		}
		if mustRegister {
			log.Debugf("Must register schema %v , (existingID:%d) dryRun:%v", schema, existingID, dryRun)
			if !dryRun {
				newVersion, err := admin.RegisterSubject(schema)
				if err != nil {
					log.Fatal(err)
				}
				result.NewVersion = newVersion
			}
		}
	}
	// ---- Compatibility settings
	/*
		- If current compatibility is NOT SET then:
		  - If requested compatibility matches global compat, do nothing
		  - Otherwise, set per subject compat
		- If current compatibility is SET then:
		  - If If requested compat matches set compat do nothing
		  - Otherwise, set compat
	*/
	var newCompat string = ""
	curCompat, _ := admin.GetCompatibility(schema)
	if schema.Compatibility != "" {
		if (curCompat == "") && !strings.EqualFold(schema.Compatibility, globalCompat) {
			log.Tracef("curCompat=%s, schema.Compatibility (%s) != globalCompat (%s)", curCompat, schema.Compatibility, globalCompat)
			newCompat = schema.Compatibility
		}
		if (curCompat != "") && !strings.EqualFold(schema.Compatibility, curCompat) {
			log.Tracef("curCompat=%s, schema.Compatibility (%s) != curCompat (%s)", curCompat, schema.Compatibility, curCompat)
			newCompat = schema.Compatibility
		}
		if !dryRun && newCompat != "" {
			log.Debugf("Setting compatibility for subject %s to %s", schema.SubjectName, schema.Compatibility)
			admin.SetCompatibility(schema, schema.Compatibility)

		}
	}
	result.NewCompat = newCompat
	result.Changed = mustRegister
	return &result

}

// Get the list of topics and reconcile all subjects
func (admin *SRAdmin) Reconcile(topics map[string]Topic, dryRun bool) []SchemaResult {
	var schemaResults []SchemaResult
	for _, topic := range topics {
		if (Schema{} != topic.Value) {
			res := admin.ReconcileSchema(topic.Value, dryRun)
			schemaResults = append(schemaResults, *res)
		}
		if (Schema{} != topic.Key) {
			res := admin.ReconcileSchema(topic.Key, dryRun)
			schemaResults = append(schemaResults, *res)
		}

	}

	return schemaResults

}

func getSubjectForTopic(topic string, isKey bool) string {
	var subject_suffix string
	if isKey {
		subject_suffix = "-key"
	} else {
		subject_suffix = "-value"
	}
	return fmt.Sprintf("%s-%s", topic, subject_suffix)
}
