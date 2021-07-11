package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
)

type ProduceCmd struct {
	Topic           string              `required arg help:"Topic to read from"`
	Idempotent      bool                `help:"Make producer idempotent"`
	Acks            sarama.RequiredAcks `help:"Required acks (0,1,-1) defaults to WaitForAll" default:"-1"`
	Separator       string              `help:"character to separate Key from Value. If set, alows sending keys from user input"`
	Tombstone       bool                `help:"Produce a tombstone. Input will be used as key and value will be null"`
	Serialize       bool                `help:"Serialize the record"`
	ValueSchemaFile string              `help:"Path to schema file for Value. If empty, the latest version will be pulled from the SchemaRegistry"`
	KeySchemaFile   string              `help:"Path to schema file for Key. If empty, the latest version will be pulled from the SchemaRegistry"`
}

func (cmd *ProduceCmd) Run(ctx *CLIContext) error {
	// Tombstone and separator don't mix
	if cmd.Tombstone && (cmd.Separator != "") {
		log.Fatal("tombstone can't be used with separator because value will always be null")
	}
	config := LoadConfig(ctx.Config)
	producer := NewProducer(config.Connections.Kafka, &config.Connections.Schemaregistry, cmd.Acks, cmd.Idempotent)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf(">")
	for scanner.Scan() {
		line := scanner.Text()
		var key, value *string
		if cmd.Separator != "" {
			splitStrings := strings.Split(line, cmd.Separator)
			if len(splitStrings) != 2 {
				log.Fatal("Can't split input string in key/value pair")
			}
			key, value = new(string), new(string)
			*key, *value = splitStrings[0], splitStrings[1]
		} else {
			value = &line
			key = new(string) // TODO maybe generate a sequence here?
		}
		if cmd.Tombstone {
			// Well if the user specified that they want a tombtstone message, take what they types and make it a key, and set the value to nil
			key = value
			value = nil
		}
		err := producer.Produce(cmd.Topic, key, value, cmd.Serialize, cmd.ValueSchemaFile, cmd.KeySchemaFile)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(">")
	}
	return nil

}
