package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"testing"
	"time"
)

// Setup the CP Env in Docker testcontainers and return the references to it.
// the calling application is responsible for setup up a defer call to close close the context
type TestCPEnv struct {
	kafkaBroker  testcontainers.Container
	zookeeprNode testcontainers.Container
	kafkaPort    nat.Port
	kafkaIP      string
	zkIP         string
	zkPort       nat.Port
	ctx          context.Context
}

// Log consumer (as per documentation example)
type TestLogConsumer struct {
	Msgs []string
}

func (g *TestLogConsumer) Accept(l testcontainers.Log) {
	log.Printf("Accepted(): %s\n", l.Content)
	g.Msgs = append(g.Msgs, string(l.Content))
}

func setupCPEnv() *TestCPEnv {
	var cpEnv TestCPEnv
	var err error
	var zkPort string = "2181"
	kafkaEnv := make(map[string]string)
	zkEnv := make(map[string]string)
	zkEnv["ZOOKEEPER_CLIENT_PORT"] = zkPort
	// Create log consumers
	logConsumer := TestLogConsumer{Msgs: []string{}}

	cpEnv.ctx = context.Background()
	// Create ZK container
	reqZK := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-zookeeper",
		ExposedPorts: []string{"9092/tcp", fmt.Sprintf("%s/tcp", zkPort)},
		Env:          zkEnv,
		WaitingFor:   wait.ForListeningPort("2181").WithStartupTimeout(10 * time.Second),
	}
	cpEnv.zookeeprNode, err = testcontainers.GenericContainer(cpEnv.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqZK,
		Started:          true,
	})
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.zookeeprNode.StartLogProducer(cpEnv.ctx)
	cpEnv.zookeeprNode.FollowOutput(&logConsumer)
	cpEnv.zkIP, err = cpEnv.zookeeprNode.Host(cpEnv.ctx)
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.zkPort, err = cpEnv.zookeeprNode.MappedPort(cpEnv.ctx, "9092")
	if err != nil {
		log.Fatal(err)
	}
	// Create Kafka broker after Zookeeper starts
	kafkaEnv["KAFKA_ZOOKEEPER_CONNECT"] = fmt.Sprintf("%s:%s", cpEnv.zkIP, cpEnv.zkPort)
	kafkaEnv["KAFKA_ADVERTISED_LISTENERS"] = "PLAINTEXT://0.0.0.0:9092"
	kafkaEnv["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1"
	reqKafka := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-server",
		ExposedPorts: []string{"9092/tcp", "8091/tcp"},
		WaitingFor:   wait.ForListeningPort("9092").WithStartupTimeout(40 * time.Second),
		Env:          kafkaEnv,
	}
	cpEnv.kafkaBroker, err = testcontainers.GenericContainer(cpEnv.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqKafka,
		Started:          true,
	})
	cpEnv.kafkaBroker.StartLogProducer(cpEnv.ctx)
	cpEnv.kafkaBroker.FollowOutput(&logConsumer)
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.kafkaIP, err = cpEnv.kafkaBroker.Host(cpEnv.ctx)
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.kafkaPort, err = cpEnv.kafkaBroker.MappedPort(cpEnv.ctx, "9092")
	if err != nil {
		log.Fatal(err)
	}
	return &cpEnv
}

func (e *TestCPEnv) Terminate() {
	log.Print("TERMINATE!\n")
	e.kafkaBroker.StartLogProducer(e.ctx)
	e.kafkaBroker.Terminate(e.ctx)
	e.zookeeprNode.Terminate(e.ctx)
}

func TestProducer(t *testing.T) {
	cpEnv := setupCPEnv()
	buf := new(bytes.Buffer)
	logs, err := cpEnv.kafkaBroker.Logs(cpEnv.ctx)
	if err != nil {
		log.Fatal(err)
	}
	buf.ReadFrom(logs)
	log.Printf(buf.String())
	log.Printf("Setup env %+vs\n", cpEnv)
	time.Sleep(20 * time.Second)
	cpEnv.Terminate()
}
