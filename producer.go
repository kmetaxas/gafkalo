package main

import (
	"encoding/binary"

	"github.com/Shopify/sarama"

	//"github.com/fatih/color"
	"io/ioutil"

	"github.com/kmetaxas/srclient"
	log "github.com/sirupsen/logrus"
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
	var schema *srclient.Schema
	// TODO, it would be better to do a schema Lookup because this schema may not be the Latest but one of the older versions
	// However srclient does not offer this api yet
	subject := getSubjectForTopic(topic, isKey)
	schema, err := c.SRClient.GetLatestSchema(subject)
	if err != nil || schema == nil {
		return schema, err
	}
	if format == "" {
		format = "AVRO"
	}
	schema, err = c.SRClient.CreateSchema(subject, schemaData, format)
	if err != nil {
		return schema, err
	}
	return schema, nil

}

func (c *Producer) GetSerializedPayload(topic, data, schemaPath string, format srclient.SchemaType, isKey bool) ([]byte, error) {
	var resp []byte
	subject := getSubjectForTopic(topic, isKey)
	if schemaPath != "" {
		schemaData, err := ioutil.ReadFile(schemaPath)
		if err != nil {
			return resp, err
		}
		schema, err := c.GetOrCreateSchema(topic, string(schemaData), "AVRO", false)
		if err != nil {
			return resp, err
		}
		resp = makeBinaryRecordUsingSchema(data, schema)
	} else {
		schema, err := c.SRClient.GetLatestSchema(subject)
		if err != nil {
			return resp, err
		}
		if schema == nil {
			log.Fatalf("No schema registered for topic %s and no schema file provided\n", topic)
			return resp, err
		}
		resp = makeBinaryRecordUsingSchema(data, schema)
	}
	return resp, nil

}

// Takes care of generating a ProduceMessage.
func (c *Producer) makeProduceMsg(topic string, key, value *string, serialize bool, valSchemaPath string, keySchemaPath string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	var err error
	var valuePayload []byte
	var keyPayload []byte
	if serialize {
		// Value
		if value != nil {
			valuePayload, err = c.GetSerializedPayload(topic, string(*value), valSchemaPath, "AVRO", false)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			valuePayload = sarama.ByteEncoder(nil)
		}
		msg.Value = sarama.ByteEncoder(valuePayload)
		// Key
		if key != nil {
			keyPayload, err = c.GetSerializedPayload(topic, string(*key), valSchemaPath, "AVRO", true)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			keyPayload = sarama.ByteEncoder(nil)
		}
		msg.Key = sarama.ByteEncoder(keyPayload)
	} else {
		if value == nil {
			valuePayload = sarama.ByteEncoder(nil)
		} else {
			msg.Value = sarama.StringEncoder(*value)
		}
		msg.Key = sarama.StringEncoder(*key)
	}
	return msg
}
func (c *Producer) Produce(topic string, key, value *string, serialize bool, valSchemaPath, keySchemaPath string) error {
	msg := c.makeProduceMsg(topic, key, value, serialize, valSchemaPath, keySchemaPath)
	_, offset, err := c.Client.SendMessage(msg)
	if err != nil {
		log.Fatalf("Produce() error: %s\n", err)
		return err
	}
	log.Printf("Produced record with offset[%v]\n", offset)
	return nil

}

// Send a kafka message as bytes. No serializes or schema registry actions are performed. USer is responsible for the payload
func (c *Producer) SendByteMsg(topic string, key, value []byte) error {
	msg := &sarama.ProducerMessage{}
	msg.Key = sarama.ByteEncoder(key)
	msg.Value = sarama.ByteEncoder(value)
	msg.Topic = topic
	_, offset, err := c.Client.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("Sent record with offset %d\n", offset)
	return nil
}
