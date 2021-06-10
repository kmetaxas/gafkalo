package main

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	//"github.com/fatih/color"
	"github.com/riferrei/srclient"
	"io/ioutil"
	"log"
)

type Producer struct {
	Client   sarama.SyncProducer
	SRClient *srclient.SchemaRegistryClient
}

func NewProducer(kConf KafkaConfig, srConf *SRConfig, acks sarama.RequiredAcks, idempotent bool) *Producer {
	var newProducer Producer
	kafkaConf := SaramaConfigFromKafkaConfig(kConf)
	kafkaConf.Producer.RequiredAcks = acks
	kafkaConf.Producer.Partitioner = sarama.NewReferenceHashPartitioner // Use the reference partitioner that is compatible with Java partitioner implementation
	kafkaConf.Producer.Return.Successes = true
	kafkaConf.Producer.Idempotent = idempotent
	if idempotent {
		kafkaConf.Net.MaxOpenRequests = 1
	}
	if srConf != nil {
		newProducer.SRClient = srclient.CreateSchemaRegistryClient(srConf.Url)
		if srConf.Username != "" && srConf.Password != "" {
			newProducer.SRClient.SetCredentials(srConf.Username, srConf.Password)
		}
	}
	producer, err := sarama.NewSyncProducer(kConf.Brokers, kafkaConf)
	if err != nil {
		log.Fatal(err)
	}
	newProducer.Client = producer
	return &newProducer
}

// From a given srclient Schema object, return a binary record to push to kafka.
func makeBinaryRecordUsingSchema(data string, schema *srclient.Schema) []byte {
	var resp []byte

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))
	valueAvroNative, _, err := schema.Codec().NativeFromTextual([]byte(data))
	if err != nil {
		log.Fatalf("NativeFromTextual failed: %s\n", err)
	}
	// Get a byte array from the Avro native textual representation
	valueBytes, err := schema.Codec().BinaryFromNative(nil, valueAvroNative)
	if err != nil {
		log.Fatal(err)
	}

	resp = append(resp, byte(0))
	resp = append(resp, schemaIDBytes...)
	resp = append(resp, valueBytes...)
	return resp
}

func (c *Producer) GetOrCreateSchema(topic, schemaData string, format srclient.SchemaType, isKey bool) (*srclient.Schema, error) {
	// TODO first check if it exists
	var schema *srclient.Schema
	if format == "" {
		format = "AVRO"
	}
	schema, err := c.SRClient.CreateSchema(topic, schemaData, format, isKey)
	if err != nil {
		return schema, err
	}
	return schema, nil

}

// Takes care of generating a ProduceMessage.
func (c *Producer) makeProduceMsg(topic string, key string, value string, serialize bool, valSchemaPath string, keySchemaPath string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	var schema *srclient.Schema
	var err error
	if serialize {
		// Value
		if valSchemaPath != "" {
			data, err := ioutil.ReadFile(valSchemaPath)
			if err != nil {
				log.Fatal(err)
			}
			schema, err := c.GetOrCreateSchema(topic, string(data), "AVRO", false)
			if err != nil {
				log.Fatal(err)
			}
			record := makeBinaryRecordUsingSchema(value, schema)
			msg.Value = sarama.ByteEncoder(record)
		} else {
			schema, err = c.SRClient.GetLatestSchema(topic, false)
			if err != nil {
				log.Fatal(err)
			}
			if schema == nil {
				log.Fatalf("No schema registered for topic %s and no schema file provided\n", topic)
			}
			record := makeBinaryRecordUsingSchema(value, schema)
			msg.Value = sarama.ByteEncoder(record)
		}
		if keySchemaPath != "" {
			data, err := ioutil.ReadFile(keySchemaPath)
			if err != nil {
				log.Fatal(err)
			}
			schema, err := c.GetOrCreateSchema(topic, string(data), "AVRO", true)
			if err != nil {
				log.Fatal(err)
			}
			record := makeBinaryRecordUsingSchema(value, schema)
			msg.Key = sarama.ByteEncoder(record)
		} else {
			schema, err = c.SRClient.GetLatestSchema(topic, true)
			if err != nil {
				log.Fatal(err)
			}
			if schema == nil {
				log.Fatalf("No schema registered for topic %s and no schema file provided\n", topic)
			}
			record := makeBinaryRecordUsingSchema(value, schema)
			msg.Key = sarama.ByteEncoder(record)
		}
	} else {
		msg.Value = sarama.StringEncoder(value)
		msg.Key = sarama.StringEncoder(key)
	}
	return msg
}
func (c *Producer) Produce(topic, key, value string, serialize bool, valSchemaPath, keySchemaPath string) error {
	msg := c.makeProduceMsg(topic, key, value, serialize, valSchemaPath, keySchemaPath)
	_, offset, err := c.Client.SendMessage(msg)
	if err != nil {
		log.Fatalf("Produce() error: %s\n", err)
		return err
	}
	log.Printf("Produced record with offset[%v]\n", offset)
	return nil

}
