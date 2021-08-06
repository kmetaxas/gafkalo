package main

import (
	"log"
)

type ReplicatorCmd struct {
	SourceTopic string `required help:"Source topic"`
	DestTopic   string `required help:"Destination topic"`
	DestConfig  string `help:"Config YAML for destination cluster. If not provided --config YAML will be used"`
}

func (cmd *ReplicatorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	var destConfig Configuration
	if cmd.DestConfig != "" {
		destConfig = LoadConfig(cmd.DestConfig)
	} else {
		destConfig = config
	}
	// Create consumer
	// TODO we should use a stable consumer group to make this replicator resumable instead of randomly generated ones
	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, []string{cmd.SourceTopic}, "", nil, false, false, false, true, "", nil)

	// Create Producer make it idempotent)
	producer := NewProducer(destConfig.Connections.Kafka, &config.Connections.Schemaregistry, -1, true)
	// Create replicator
	replicator, err := NewReplicator(consumer, producer, cmd.SourceTopic, cmd.DestTopic)
	if err != nil {
		log.Fatal(err)
	}
	// Start copying
	replicator.Copy()
	return nil
}
