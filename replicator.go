package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
)

/*
Simple replicator functionality
Useful to easily copy topic A to topic B (since Kafka does not support renaming)
*/

// Replicator object also implemented the sarama ConsumerGroupHandler interface
type Replicator struct {
	consumer  *Consumer
	producer  *Producer
	fromTopic string
	toTopic   string
	ready     chan bool
	ctx       context.Context // tell the consumer to stop
	cancel    context.CancelFunc
}

func (r *Replicator) Setup(session sarama.ConsumerGroupSession) error {
	close(r.ready)
	return nil
}

func (r *Replicator) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (r *Replicator) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			err := r.producer.SendByteMsg(r.toTopic, message.Key, message.Value)
			if err != nil {
				return err
			}
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func NewReplicator(consumer *Consumer, producer *Producer, fromTopic, toTopic string) (*Replicator, error) {
	var newReplicator Replicator
	newReplicator.consumer = consumer
	newReplicator.producer = producer
	newReplicator.fromTopic = fromTopic
	newReplicator.toTopic = toTopic

	return &newReplicator, nil
}

// Copy from source to destination.
func (r *Replicator) Copy() {
	r.ready = make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.ctx = ctx
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := r.consumer.ConsumerGroup.Consume(ctx, []string{r.fromTopic}, r); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			r.ready = make(chan bool)
		}
	}()

	<-r.ready // Await till the consumer has been set up
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
	if err := r.consumer.ConsumerGroup.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}
