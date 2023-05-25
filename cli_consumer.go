package main

import ()

type ConsumerCmd struct {
	Topics           []string `required arg help:"Topic to read from"`
	MaxRecords       int      `default:"0" help:"Max reacords to read. default to no limit"` // 0 means no limit
	DeserializeKey   bool     `default:"false" help:"Deserialize message key"`
	DeserializeValue bool     `default:"false" help:"Deserialize message value"`
	GroupID          string   `help:"Consumer group ID to use"`
	FromBeginning    bool     `default:"false" help:"Start reading from the beginning of the topic"`
	SetOffsets       string   `help:"Set offsets for partition on topic. Syntax is: TOPICNAME=partition:offset,partition:offset,.."`
	RecordTemplate   string   `help:"Path to a golan template to format records"`
}

func (cmd *ConsumerCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	var offsets map[int32]int64
	var err error
	var useOffsets bool = false
	if cmd.SetOffsets != "" {
		useOffsets = true
		offsets, err = parseOffsetsArg(&cmd.SetOffsets)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		offsets = make(map[int32]int64)
	}

	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, cmd.Topics, cmd.GroupID, offsets, useOffsets, cmd.DeserializeKey, cmd.DeserializeValue, cmd.FromBeginning, cmd.RecordTemplate, nil)
	err = consumer.Consume(cmd.MaxRecords)
	if err != nil {
		log.Fatal(err)
	}
	return nil

}
