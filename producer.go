package main

import (
	"github.com/Shopify/sarama"
	//"github.com/fatih/color"
	"github.com/riferrei/srclient"
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

// Takes care of generating a ProduceMessage.
func makeProduceMsg(topic string, key string, value string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	// TODO encode with Avro or whatever is needed here..
	msg.Value = sarama.StringEncoder(value)
	msg.Key = sarama.StringEncoder(key)
	return msg
}
func (c *Producer) Produce(topic string, key string, value string) error {
	msg := makeProduceMsg(topic, key, value)
	_, offset, err := c.Client.SendMessage(msg)
	if err != nil {
		log.Fatalf("Produce() error: %s\n", err)
		return err
	}
	log.Printf("Produced record with offset[%v]\n", offset)
	return nil

}
