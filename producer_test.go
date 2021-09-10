package main

import (
	"bytes"
	"context"
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

func setupCPEnv() *TestCPEnv {
	var cpEnv TestCPEnv
	var err error
	cpEnv.ctx = context.Background()

	reqKafka := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-server",
		ExposedPorts: []string{"9091/tcp", "8091/tcp"},
		WaitingFor:   wait.ForListeningPort("9091").WithStartupTimeout(10 * time.Second),
	}
	cpEnv.kafkaBroker, err = testcontainers.GenericContainer(cpEnv.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqKafka,
		Started:          true,
	})
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.kafkaIP, err = cpEnv.kafkaBroker.Host(cpEnv.ctx)
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.kafkaPort, err = cpEnv.kafkaBroker.MappedPort(cpEnv.ctx, "9091")
	if err != nil {
		log.Fatal(err)
	}
	// Create ZK container
	reqZK := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-zookeeper",
		ExposedPorts: []string{"9091/tcp", "2181/tcp"},
	}
	cpEnv.zookeeprNode, err = testcontainers.GenericContainer(cpEnv.ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: reqZK,
		Started:          true,
	})
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.zkIP, err = cpEnv.zookeeprNode.Host(cpEnv.ctx)
	if err != nil {
		log.Fatal(err)
	}
	cpEnv.zkPort, err = cpEnv.zookeeprNode.MappedPort(cpEnv.ctx, "9091")
	if err != nil {
		log.Fatal(err)
	}
	return &cpEnv
}

func (e *TestCPEnv) Terminate() {
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
	time.Sleep(10 * time.Second)
	buf.ReadFrom(logs)
	log.Printf(buf.String())
	log.Printf("Setup env %+vs\n", cpEnv)
	cpEnv.Terminate()
}
