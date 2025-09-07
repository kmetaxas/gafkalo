package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
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
	WholeFile       string              `help:"Read whole file as value payload. (for example to test max size). No schema will be used"`
}

func (cmd *ProduceCmd) Run(ctx *CLIContext) error {
	// Tombstone and separator don't mix
	if cmd.Tombstone && (cmd.Separator != "") {
		log.Fatal("tombstone can't be used with separator because value will always be null")
	}
	config := LoadConfig(ctx.Config)
	producer := NewProducer(config.Connections.Kafka, &config.Connections.Schemaregistry, cmd.Acks, cmd.Idempotent)
	// If WholeFile is provided, read the file and produce it.
	if cmd.WholeFile != "" {
		data, err := ioutil.ReadFile(cmd.WholeFile)
		if err != nil {
			log.Printf("unable to read %s with error %s\n", cmd.WholeFile, err)
		}

		err = producer.SendByteMsg(cmd.Topic, nil, data)
		if err != nil {
			log.Fatal(err)
		}
		return nil
	}
	// Read messages from console input
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
			key = nil
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
