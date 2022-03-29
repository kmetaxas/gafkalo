package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

/*
The Confluent Schema registry does not offer a way to check subjects efficiently.
Gafkalo produces a huge amount of REST API calls and can take a very long time to do a `plan` or an `apply`

For this reason, we can bypass the schema registry REST API completely and read from the `_schemas` topic, creating an internal representation of all subjects.
All lookups, and checks will then be completely performed on in-memory structures of Gafkalo
This *can* require a lot of RAM in the case of huge amount of subjects and big schemas but in most cases it should be completely manageable for modern machines
*/

type SchemaRegistryCache struct {
	// Schema IDs
	schemas      map[int]string
	subjects     map[string]map[int]int
	globalCompat string
	// Compatibility level per subject. Since this can be set using a CONFIG and does not exist at Subject create time, we set a separate field here for fast lookups/updates
	compatPerSubject   map[string]string
	consumer           *Consumer
	lastKnownOffsetEnd int64 // Last end of topic offset that we know of
}

// keytype SCHEMA in _schemas topic Key
type SRKey struct {
	Keytype string `json:"keytype"` // SCHEMA , CONFIG, NOOP
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Magic   int    `json:"magic"`
}

// Value for CONFIG type
type SRValueConfig struct {
	CompatibilityLevel string `json:"compatibilityLevel"`
}

// Value for SCHEMA type
type SRValueSchema struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Id      int    `json:"id"`
	Schema  string `json:"schema"`
	Deleted bool   `json:"deleted"`
}

func NewSchemaRegistryCache(config *Configuration) (*SchemaRegistryCache, error) {
	var err error
	topics := []string{"_schemas"}
	var srCache SchemaRegistryCache
	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, topics, fmt.Sprintf("gafkalo-sr-%s", RandomString(4)), nil, false, false, false, true, "", &srCache)
	srCache.consumer = consumer
	srCache.schemas = make(map[int]string)
	srCache.subjects = make(map[string]map[int]int)
	srCache.compatPerSubject = make(map[string]string)
	srCache.globalCompat = "BACKWARD" //default is BACKWARD.
	return &srCache, err
}

func (c *SchemaRegistryCache) ReadSchemaTopic(topic string) {
	// Find current end offset and out it in lastKnownOffsetEnd so that
	// ConsumeClaim will stop consuming at that point
	last_offset, err := c.consumer.Client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	c.lastKnownOffsetEnd = last_offset - 1
	if err != nil {
		log.Fatalf("Failed to fetch latest offset for _schemas with: %s", err)
	}

	err = c.consumer.Consume(-1)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Reading _schemas topic complete")
}

// Update the schema cache ith the provided schema object
// If it does not exist it is added, otherwise updated.
func (c *SchemaRegistryCache) updateOrCreateSchema(schema Schema) error {
	var err error
	return err
}

// Get the schema for the provided subject
func (c *SchemaRegistryCache) GetSchemaForSubjectVersion(name string, version int) (*Schema, error) {
	var schema Schema
	var err error
	schemaID := c.subjects[name][version]
	schemaText := c.schemas[schemaID]
	schema = Schema{SubjectName: name, SchemaData: schemaText}
	return &schema, err
}

// Fetch the compatibility setting for given Subject.
func (c *SchemaRegistryCache) GetCompatibilityForSubject(subject string) string {
	return c.compatPerSubject[subject]
}

// Retrieve the global compatibility setting
func (c *SchemaRegistryCache) GetGlobalCompatibility() string {
	return c.globalCompat
}

// Lookup schema text under a subject. Return version and ID if found. Zeros if not.
func (c *SchemaRegistryCache) LookupSchemaForSubject(subject, schema string) (int, int, error) {
	// Compare all schema versions registered
	for version, schema_id := range c.subjects[subject] {
		log.Debugf("Comparing version %d of subject %s", version, subject)
		existing_schema := c.schemas[schema_id]
		existingschemaObj := Schema{SubjectName: subject, SchemaData: existing_schema}
		equals, diff := existingschemaObj.SchemaDiff(schema)
		log.Debugf("Subject %s equals=%v ", subject, equals)
		if equals {
			return schema_id, version, nil
		} else {
			log.Debugf("Not it. Schemas differ.:: %s", diff)
		}

	}
	return 0, 0, nil
}

// Implement sarama consumer ConsumerGroupHandler interface
func (r *SchemaRegistryCache) Setup(session sarama.ConsumerGroupSession) error {
	close(r.consumer.ready)
	return nil
}

func (r *SchemaRegistryCache) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (r *SchemaRegistryCache) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var recordKey SRKey
		err := json.Unmarshal(message.Key, &recordKey)
		if err != nil {
			return err
		}
		switch recordKey.Keytype {
		case "CONFIG":
			r.processConfigValue(&recordKey, message.Value)
		case "SCHEMA":
			r.processSchemaValue(&recordKey, message.Value)
		}
		session.MarkMessage(message, "")
		// If that was the last message we should read, stop consuming
		if message.Offset == r.lastKnownOffsetEnd {
			log.Debugf("Stopping at end of topic %v", message.Offset)
			r.consumer.cancel()
			break
		}
	}
	return nil
}

func (r *SchemaRegistryCache) processConfigValue(key *SRKey, data []byte) error {
	var value SRValueConfig
	subject := key.Subject
	// If subject == nil this must be a global compatibility config
	if data == nil {
		// Tombstone. Deleting compatibility only supported in CP 7.0 schemaregistry
		delete(r.compatPerSubject, subject)
		return nil
	}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	if subject == "" {
		log.Debugf("Global compatibility update seen (%s)", value.CompatibilityLevel)
		r.globalCompat = value.CompatibilityLevel
	} else {

		r.compatPerSubject[subject] = value.CompatibilityLevel
	}

	return nil
}

// Add a subject to known subjects. Handled nested maps properly
func (r *SchemaRegistryCache) addSubject(name string, version, id int) {
	_, exists := r.subjects[name]
	if !exists {
		r.subjects[name] = make(map[int]int)
	}
	r.subjects[name][version] = id
}

// Process the Kafka record with a keytype=SCHEMA. ACcepts the binary value and needs to be unmarshalled and processed
func (r *SchemaRegistryCache) processSchemaValue(key *SRKey, data []byte) error {
	var value SRValueSchema
	subject := key.Subject
	if data == nil {
		// Tombstone. Deleting compatibility only supported in CP 7.0 schemaregistry
		if data == nil {
			log.Debugf("Tombstone. Dropping version %d of subject %s", key.Version, subject)
		}

		delete(r.subjects[subject], key.Version)
		return nil
	}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	// Also check for soft deleted subject versions (delete flag has been set)
	if value.Deleted {
		if value.Deleted {
			log.Debugf("Delete flag seen. Dropping version %d of subject %s", key.Version, subject)
		}
		delete(r.subjects[subject], key.Version)
		return nil
	}
	// We don't check if it exists already as any event replaces the previous one.
	// Since this is a compacted topic, if a key exists then we create a map entry.
	r.schemas[value.Id] = value.Schema
	r.addSubject(value.Subject, value.Version, value.Id)
	return nil
}

// Get a list of registered subjects
func (c *SchemaRegistryCache) GetSubjects() []string {
	var subjectResponse []string
	for subject, _ := range c.subjects {
		subjectResponse = append(subjectResponse, subject)
	}
	return subjectResponse
}

// This is a debugging method. TODO remove when not needed
func (c *SchemaRegistryCache) ListSubjects() {

	for key, value := range c.subjects {
		log.Printf("Subject %s:", key)
		custom_compat := c.compatPerSubject[key]
		for version, schemaID := range value {
			log.Printf("Version %d -> ID: %d (custom compatibility: %s", version, schemaID, custom_compat)
		}
	}

}
