package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"github.com/riferrei/srclient"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"text/template"
	"time"
)

type Consumer struct {
	Client               sarama.ConsumerGroup
	SRClient             *srclient.SchemaRegistryClient
	ConsumerGroup        sarama.ConsumerGroup
	Topics               []string
	PartitionOffsets     map[int32]int64 // map of partion number, offset to start fro
	UsePartitionOffsets  bool
	ready                chan bool
	msgCount             int             // consumed messages count
	maxRecords           int             //  max records to read
	ctx                  context.Context // tell the consumer to stop
	cancel               context.CancelFunc
	deserializeKey       bool
	deserializeValue     bool
	customRecordTemplate *template.Template
}

type CustomRecordTemplateContext struct {
	Topic       string
	Key         string
	Value       string
	Timestamp   time.Time
	Partition   int32
	Offset      int64
	KeySchemaID int // The schema registry ID of the Key schema
	ValSchemaID int // The Schema registry ID of the Value schema
}

// Naive random string implementation ( https://golangdocs.com/generate-random-string-in-golang )
func RandomString(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func loadTemplate(path string) *template.Template {
	tplData, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	tmpl := template.Must(template.New("kafkarecord").Parse(string(tplData)))
	return tmpl

}

func NewConsumer(kConf KafkaConfig, srConf *SRConfig, topics []string, groupID string, partitionOffsets map[int32]int64, useOffsets bool, deserializeKey, deserializeValue bool, fromBeginning bool, customTemplateFile string) *Consumer {
	var consumer Consumer
	kafkaConf := SaramaConfigFromKafkaConfig(kConf)

	if srConf != nil {
		consumer.SRClient = srclient.CreateSchemaRegistryClient(srConf.Url)
		if srConf.Username != "" && srConf.Password != "" {
			consumer.SRClient.SetCredentials(srConf.Username, srConf.Password)
		}
	}
	if groupID == "" {
		randGroupPart := RandomString(5)
		groupID = fmt.Sprintf("gafkalo-consumer-%s", randGroupPart)
	}
	// Note, even though we set that, it only works if there are no offsets recorded for the consumer group
	if fromBeginning {
		kafkaConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	client, err := sarama.NewConsumerGroup(kConf.Brokers, groupID, kafkaConf)
	if err != nil {
		log.Fatal(err)
	}

	consumer.Client = client
	consumer.Topics = topics
	consumer.PartitionOffsets = partitionOffsets
	consumer.UsePartitionOffsets = useOffsets
	consumer.deserializeKey = deserializeKey
	consumer.deserializeValue = deserializeValue
	if customTemplateFile != "" {
		consumer.customRecordTemplate = loadTemplate(customTemplateFile)
	}
	return &consumer
}

func (c *Consumer) Consume(maxRecords int) error {
	// wait for ready
	c.ready = make(chan bool)
	c.maxRecords = maxRecords // I hate this ...
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.ctx = ctx
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := c.Client.Consume(ctx, c.Topics, c); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			c.ready = make(chan bool)
		}
	}()

	<-c.ready // Await till the consumer has been set up
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating..")
	case <-sigterm:
		log.Println("terminating (received signal)")
	}
	cancel()
	wg.Wait()
	if err := c.Client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
	return nil
}

func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	close(c.ready)
	if c.UsePartitionOffsets {
		for partition, offset := range c.PartitionOffsets {
			// TODO support multiple topics. For now only onet topic is supported by offset reset
			session.ResetOffset(c.Topics[0], partition, offset, "Gafkalo CLI offset rest")
		}
	}
	return nil
}

func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Deserialize a binary payload (wire format)
// return the deserialized string, the schema id , error
func (c *Consumer) DeserializePayload(payload []byte) (string, int, error) {
	var resp string
	if len(payload) < 5 {
		return resp, 0, errors.New("payload <5 bytes. Not schema registry wire format")
	}
	schemaID := binary.BigEndian.Uint32(payload[1:5])
	schema, err := c.SRClient.GetSchema(int(schemaID))
	if err != nil {
		return resp, 0, fmt.Errorf("Failed to deserialize schema ID [%d] with error: %s", schemaID, err)
	}
	native, _, _ := schema.Codec().NativeFromBinary(payload[5:])
	deserialized, _ := schema.Codec().TextualFromNative(nil, native)
	resp = string(deserialized)
	return resp, int(schemaID), nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		var key, val string
		var err error
		var keySchemaID, valSchemaID int
		if c.deserializeKey {
			key, keySchemaID, err = c.DeserializePayload(message.Key)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			key = string(message.Key)
		}
		if c.deserializeValue {
			val, valSchemaID, err = c.DeserializePayload(message.Value)
			if err != nil {
				log.Fatal(err)
			}

		} else {
			val = string(message.Value)
		}
		// Print the record. Either with a user provided template or our own "prettyprint" function
		if c.customRecordTemplate != nil {
			c.printRecordWithCustomTemplate(message.Topic, key, val, message.Timestamp, message.Partition, message.Offset, keySchemaID, valSchemaID)
		} else {
			prettyPrintRecord(message.Topic, key, val, message.Timestamp, message.Partition, message.Offset, keySchemaID, valSchemaID)
		}
		session.MarkMessage(message, "")
		// Do we need to call Commit()?
		c.msgCount += 1
		if c.maxRecords == c.msgCount {
			fmt.Printf("Reached user defined message limit of %d. Stoppping.\n", c.maxRecords)
			c.cancel()
			break
		}
	}
	return nil
}
func (c *Consumer) printRecordWithCustomTemplate(topic, key, value string, timestamp time.Time, partition int32, offset int64, keySchemaID, valSchemaID int) {
	context := CustomRecordTemplateContext{Topic: topic, Key: key, Value: value, Timestamp: timestamp, Partition: partition, Offset: offset, KeySchemaID: keySchemaID, ValSchemaID: valSchemaID}
	err := c.customRecordTemplate.Execute(os.Stdout, context)
	if err != nil {
		log.Fatal(err)
	}
}
func prettyPrintRecord(topic, key, value string, timestamp time.Time, partition int32, offset int64, keySchemaID, valSchemaID int) {
	fmtOffset := color.New(color.FgCyan).SprintFunc()
	fmtValue := color.New(color.FgGreen).SprintFunc()
	fmtKey := color.New(color.FgBlue).SprintFunc()
	var msg string
	if keySchemaID > 0 {
		msg = fmt.Sprintf("SchemaID(key)[%d]", keySchemaID)
	}
	if valSchemaID > 0 {
		msg = fmt.Sprintf("%s SchemaID(value)[%d]", msg, valSchemaID)
	}
	msg = fmt.Sprintf("%s Topic[%s] Offset[%s] Timestamp[%s]: Key:=%s, Value:%s", msg, topic, fmtOffset(fmt.Sprint(offset)), timestamp, fmtKey(key), fmtValue(value))
	fmt.Println(msg)
}
