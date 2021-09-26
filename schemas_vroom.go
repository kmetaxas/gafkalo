package main

import (
	"context"
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
	consumer     *Consumer
	//ready        chan bool
	ctx    context.Context // tell the consumer to stop
	cancel context.CancelFunc
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
	return &srCache, err
}

func (c *SchemaRegistryCache) readSchemaTopic(topic string) {
	// TODO read until the end and stop.
	err := c.consumer.Consume(10)
	if err != nil {
		log.Fatal(err)
	}
}

// Update the schema cache ith the provided schema object
// If it does not exist it is added, otherwise updated.
func (c *SchemaRegistryCache) updateOrCreateSchema(schema Schema) error {
	var err error
	return err
}

// Get the schema for the provided subject
func (c *SchemaRegistryCache) GetSchemaForSubject(name string) (*Schema, error) {
	var schema Schema
	var err error
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
		log.Printf("SR_KEY=%+v\n", string(message.Key))
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
	}
	return nil
}

func (r *SchemaRegistryCache) processConfigValue(key *SRKey, data []byte) error {
	var value SRValueConfig
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	log.Printf("VALUE_CONFIG=%+v\n", value)
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
	err := json.Unmarshal(data, &value)
	if err != nil {
		return err
	}
	// We don't check if it exists already as any event replaces the previous one.
	// Since this is a compacted topic, if a key exists then we create a map entry.
	// TODO handle tombstones!
	r.schemas[value.Id] = value.Schema
	r.addSubject(value.Subject, value.Version, value.Id)
	return nil
}