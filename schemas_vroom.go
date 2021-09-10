package main

import (
	"context"
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
	schemas      map[string]Schema
	globalCompat string
	consumer     *Consumer
	ready        chan bool
	ctx          context.Context // tell the consumer to stop
	cancel       context.CancelFunc
}

func NewSchemaRegistryCache(config *Configuration) (*SchemaRegistryCache, error) {
	var err error
	topics := []string{"_schemas"}
	var srCache SchemaRegistryCache
	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, topics, fmt.Sprintf("gafkalo-sr-%s", RandomString(4)), nil, false, false, false, true, "", srCache)
	srCache.consumer = consumer
	return &srCache, err
}

func (c *SchemaRegistryCache) readSchemaTopic(topic string) {
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
func (r SchemaRegistryCache) Setup(session sarama.ConsumerGroupSession) error {
	close(r.ready)
	return nil
}

func (r SchemaRegistryCache) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (r SchemaRegistryCache) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("CONSUME_CLAIN!!\n")
	for message := range claim.Messages() {
		log.Printf("SR_KEY=%+vs\n", message.Key)
		log.Printf("SR_Value=%+vs\n", message.Value)
		session.MarkMessage(message, "")
	}
	return nil
}
