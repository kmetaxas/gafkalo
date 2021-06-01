package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/riferrei/srclient"
	"io"
	"io/ioutil"
	"log"
	"net/http"
)

// Schema object to keep track of desired/requested state
type Schema struct {
	SubjectName   string
	SchemaPath    string `yaml:"schema"`
	Compatibility string `yaml:"compatibility"`
	SchemaData    string
	SchemaType    srclient.SchemaType `yaml:"schema_type"`
}

// Create a new Schema instance. Handles defaults etc
func CreateSchema(SubjectName string, SchemaPath string, Compatibility string, SchemaType srclient.SchemaType) (Schema, error) {
	var newSchema Schema
	newSchema.SubjectName = SubjectName
	newSchema.SchemaPath = SchemaPath
	newSchema.Compatibility = Compatibility

	// Load the schema data
	data, err := ioutil.ReadFile(SchemaPath)
	if err != nil {
		log.Fatalf("Unable to create schema with Error: %s\n", err)
	}
	newSchema.SchemaData = string(data)
	if SchemaType == "" {
		SchemaType = "AVRO"
	}
	newSchema.SchemaType = SchemaType
	return newSchema, nil

}

func (s *Schema) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type rawSchema Schema
	raw := rawSchema{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	//fmt.Printf("------- Unmarshaling %s\n", raw)
	//fmt.Printf("-------SchemaType=%s Compatib:%s \n", raw.SchemaType, raw.Compatibility)
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
}

// Create a new SRAdmin
func NewSRAdmin(url string, username string, password string) SRAdmin {
	srclient := srclient.CreateSchemaRegistryClient(url)
	srclient.SetCredentials(username, password)

	sradmin := SRAdmin{Client: *srclient, user: username, pass: password}
	sradmin.url = url
	subjects, err := sradmin.Client.GetSubjects()
	sradmin.SubjectCache = subjects
	if err != nil {
		log.Fatalf("Unable to get SR subjects: %s\n", err)
	}
	return sradmin
}

func (admin *SRAdmin) RegisterSubject(schema Schema) error {

	// Create a value subject (isKey = false)
	fmt.Printf("++Trying to create Schema: %s...", schema.SubjectName)
	_, err := admin.Client.CreateSchemaWithArbitrarySubject(schema.SubjectName, schema.SchemaData, schema.SchemaType)
	if err != nil {
		log.Fatalf("FAILED:: %s\n", err)
	}
	fmt.Printf("DONE!\n")
	return nil
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
	type Response struct {
		Subject string `json:"subject"`
		Id      int    `json:"id"`
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}
	type Request struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}

	request, err := json.Marshal(Request{Schema: string(schema.SchemaData), SchemaType: string(schema.SchemaType)})
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

	return respObj.Id, respObj.Version, nil
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
	globalCompat, err := admin.GetCompatibilityGlobal()
	existingID, existingVersion, err := admin.LookupSchema(schema)
	if err != nil {
		log.Printf("Reconcile Failed to lookup %s with %s\n", schema.SubjectName, err)
	}
	if err != nil {
		log.Fatal(err)
	}
	var mustRegister bool = false
	if existingID != 0 {
		// Schema already registered, but is this the latest version?
		versions, err := admin.Client.GetSchemaVersionsWithArbitrarySubject(schema.SubjectName)
		if err != nil {
			log.Fatalf("Failed to fetch versions for %s: %s\n", schema.SubjectName, err)
		}
		if existingVersion != versions[len(versions)-1] {
			mustRegister = true
		}

	} else {
		mustRegister = true // Must register new schema
	}
	if mustRegister {
		admin.RegisterSubject(schema)
	}
	// --- compat
	var newCompat string = ""
	curCompat, _ := admin.GetCompatibility(schema)
	if (schema.Compatibility != "") && ((schema.Compatibility != curCompat) && (schema.Compatibility != globalCompat)) {
		admin.SetCompatibility(schema, schema.Compatibility)
		newCompat = schema.Compatibility
	}
	result := SchemaResult{
		SubjectName: schema.SubjectName,
		Changed:     mustRegister,
		NewCompat:   newCompat,
	}
	return &result

}

// Get the list of topics and reconcile all subjects
func (admin *SRAdmin) Reconcile(topics map[string]Topic, dryRun bool) []SchemaResult {
	var schemaResults []SchemaResult
	for _, topic := range topics {
		if (Schema{} != topic.Value) {
			res := admin.ReconcileSchema(topic.Value, false)
			schemaResults = append(schemaResults, *res)
		}
		if (Schema{} != topic.Key) {
			res := admin.ReconcileSchema(topic.Key, false)
			schemaResults = append(schemaResults, *res)
		}

	}

	return schemaResults

}
