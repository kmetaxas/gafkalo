package main

import (
	"log"
)

type ReplicatorCmd struct {
	SourceTopic string `required help:"Source topic"`
	DestTopic   string `required help:"Destination topic"`
	DestConfig  string `help:"Config YAML for destination cluster. If not provided --config YAML will be used"`
	GroupID     string `help:"Consumer group ID to use. Useful to be able to continue replication where it left off. Note that contrary to a regular consumer this will be a stable group id (gafkalo-replicator) if left undefined"`
}

func (cmd *ReplicatorCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	var destConfig Configuration
	if cmd.DestConfig != "" {
		destConfig = LoadConfig(cmd.DestConfig)
	} else {
		destConfig = config
	}
	var groupID = "gafkalo-replicator"
	if len(cmd.GroupID) > 0 {
		groupID = cmd.GroupID
	}

	// Create consumer
	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, []string{cmd.SourceTopic}, groupID, nil, false, false, false, true, "", nil)

	// Create Producer make it idempotent)
	producer := NewProducer(destConfig.Connections.Kafka, &config.Connections.Schemaregistry, -1, true)
	// Create replicator
	replicator, err := NewReplicator(consumer, producer, cmd.SourceTopic, cmd.DestTopic)
	if err != nil {
		log.Fatal(err)
	}
	// Start copying. This does not exist unless stopped
	replicator.Copy()
	return nil
}
