package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	"github.com/riferrei/srclient"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Consumer struct {
	Client        sarama.ConsumerGroup
	SRClient      *srclient.SchemaRegistryClient
	ConsumerGroup sarama.ConsumerGroup
	ready         chan bool
	msgCount      int             // consumed messages count
	maxRecords    int             //  max records to read
	ctx           context.Context // tell the consumer to stop
	cancel        context.CancelFunc
}

// Naive random string implementation ( https://golangdocs.com/generate-random-string-in-golang )
func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func NewConsumer(kConf KafkaConfig, srConf *SRConfig, groupID string, deserialize bool) *Consumer {
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
	client, err := sarama.NewConsumerGroup(kConf.Brokers, groupID, kafkaConf)
	if err != nil {
		log.Fatal(err)
	}

	consumer.Client = client
	return &consumer
}

func (c *Consumer) Consume(topic string, fromOffet int, maxRecords int) error {
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
			if err := c.Client.Consume(ctx, []string{topic}, c); err != nil {
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

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		prettyPrintRecord(message)
		session.MarkMessage(message, "")
		c.msgCount += 1
		if c.maxRecords == c.msgCount {
			c.cancel()
		}
	}

	return nil
}

func prettyPrintRecord(message *sarama.ConsumerMessage) {
	fmtOffset := color.New(color.FgCyan).SprintFunc()
	fmtValue := color.New(color.FgGreen).SprintFunc()
	fmtKey := color.New(color.FgBlue).SprintFunc()

	log.Printf("Topic[%s] Offset[%s] Timestamp[%s]: Key:=%s, Value:%s", message.Topic, fmtOffset(fmt.Sprint(message.Offset)), message.Timestamp, fmtKey(string(message.Key)), fmtValue(string(message.Value)))
}
