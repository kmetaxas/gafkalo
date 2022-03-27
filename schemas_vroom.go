package main

import (
	//	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	//"os/signal"
	//"sync"
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
	compatPerSubject map[string]string
	consumer         *Consumer
	//ready        chan bool
	//ctx                context.Context // tell the consumer to stop
	//cancel             context.CancelFunc
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
	return &srCache, err
}

func (c *SchemaRegistryCache) readSchemaTopic(topic string) {
	// Find current end offset and out it in lastKnownOffsetEnd so that
	// ConsumeClaim will stop consuming at that point
	last_offset, err := c.consumer.Client.GetOffset("_schemas", 0, sarama.OffsetNewest)
	c.lastKnownOffsetEnd = last_offset - 1
	if err != nil {
		log.Fatalf("Failed to fetch latest offset for _schemas with: %s", err)
	}

	//err = c.consumer.Consume(int(last_offset - 2))
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
		//log.Printf("SR_Value=%+vs\n", string(message.Value))
		session.MarkMessage(message, "")
		// If that was the last message we should read, stop consuming
		if message.Offset == r.lastKnownOffsetEnd {
			log.Printf("Stopping at end of topic %v", message.Offset)
			r.consumer.cancel()
			break
		}
	}
	return nil
}

func (r *SchemaRegistryCache) processConfigValue(key *SRKey, data []byte) error {
	var value SRValueConfig
	subject := key.Subject
	if data == nil {
		// Tombstone. Deleting compatibility only supported in CP 7.0 schemaregistry
		delete(r.compatPerSubject, subject)
		return nil
	}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	r.compatPerSubject[subject] = value.CompatibilityLevel

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
			log.Printf("Tombstone. Dropping version %d of subject %s", key.Version, subject)
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
			log.Printf("Delete flag seen. Dropping version %d of subject %s", key.Version, subject)
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

func (c *SchemaRegistryCache) ListSubjects() {

	for key, value := range c.subjects {
		log.Printf("Subject %s:", key)
		custom_compat := c.compatPerSubject[key]
		for version, schemaID := range value {
			log.Printf("Version %d -> ID: %d (custom compatibility: %s", version, schemaID, custom_compat)
		}
	}

}
