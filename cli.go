package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
)

// Make it global so that it can be accessed in callbacks, though its not a great solution
var gafkaloConfig Configuration

type CLIContext struct {
	Config string `required arg help:"configuration file"`
}
type PlanCmd struct {
	Dryrun bool `default:"true" hidden`
}

type ApplyCmd struct {
	Dryrun bool `default:"false" hidden`
}

type ProduceCmd struct {
	Topic           string              `required arg help:"Topic to read from"`
	Idempotent      bool                `help:"Make producer idempotent"`
	Acks            sarama.RequiredAcks `help:"Required acks (0,1,-1) defaults to WaitForAll" default:"-1"`
	Separator       string              `help:"character to separate Key from Value. If set, alows sending keys from user input"`
	Serialize       bool                `help:"Serialize the record"`
	ValueSchemaFile string              `help:"Path to schema file for Value. If empty, the latest version will be pulled from the SchemaRegistry"`
	KeySchemaFile   string              `help:"Path to schema file for Key. If empty, the latest version will be pulled from the SchemaRegistry"`
}

type ConsumerCmd struct {
	Topic            string `required arg help:"Topic to read from"`
	Offset           int    `default:"-1" help:"Offset to read from"` // -1 means latest
	Partition        int16  `default:"0" help:"Partition to read from (used with --offset)"`
	MaxRecords       int    `default:"0" help:"Max reacords to read. default to no limit"` // 0 means no limit
	DeserializeKey   bool   `default:"false" help:"Deserialize message key"`
	DeserializeValue bool   `default:"false" help:"Deserialize message value"`
	GroupID          string `help:"Consumer group ID to use"`
}

type CheckExistsCmd struct {
	SchemaFile string `required help:"Schema file to checj"`
	Subject    string `required help:"Subject to check against schema"`
}

type SchemaCmd struct {
	CheckExists CheckExistsCmd `cmd help:"Check if provided schema is registered"`
}

var CLI struct {
	Config   string      `required help:"configuration file"`
	Apply    ApplyCmd    `cmd help:"Apply the changes"`
	Plan     PlanCmd     `cmd help:"Produce a plan of changes"`
	Consumer ConsumerCmd `cmd help:"Consume from topics"`
	Produce  ProduceCmd  `cmd help:"Produce to a topic"`
	Schema   SchemaCmd   `cmd help:"Manage schemas"`
}

func (cmd *ApplyCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	kafkadmin, sradmin, mdsadmin := GetAdminClients(config)
	DoSync(&kafkadmin, &sradmin, &mdsadmin, &inputData, false)
	return nil
}
func (cmd *PlanCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	kafkadmin, sradmin, mdsadmin := GetAdminClients(config)
	DoSync(&kafkadmin, &sradmin, &mdsadmin, &inputData, true)
	return nil
}

func (cmd *ConsumerCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, cmd.GroupID, cmd.DeserializeKey, cmd.DeserializeValue)
	err := consumer.Consume(cmd.Topic, cmd.Offset, cmd.MaxRecords)
	if err != nil {
		log.Fatal(err)
	}
	return nil

}

// Check if a schema is registered in specified subject. Shows version and Id if it is
func (cmd CheckExistsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	_, sradmin, _ := GetAdminClients(config)
	schema, err := CreateSchema(cmd.Subject, cmd.SchemaFile, "BACKWARD", "AVRO")
	if err != nil {
		log.Fatal(err)
	}
	schemaID, schemaVersion, err := sradmin.LookupSchema(schema)
	if err != nil {
		return fmt.Errorf("Failed to lookup schema [%+vs]\n", schema)
	}
	if schemaID == 0 {
		fmt.Printf("Schema not found in subject %s\n", schema.SubjectName)
		fmt.Printf("Did not find schema: %+vs\n", schema)
	} else {

		fmt.Printf("Schema is registered under %s with version %d and ID %d\n", schema.SubjectName, schemaVersion, schemaID)
	}
	return nil

}
func (cmd *ProduceCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	producer := NewProducer(config.Connections.Kafka, &config.Connections.Schemaregistry, cmd.Acks, cmd.Idempotent)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf(">")
	for scanner.Scan() {
		line := scanner.Text()
		var key, value string
		if cmd.Separator != "" {
			splitStrings := strings.Split(line, cmd.Separator)
			if len(splitStrings) != 2 {
				log.Fatal("Can't split input string in key/value pair")
			}
			key, value = splitStrings[0], splitStrings[1]
		} else {
			value = line
			key = "" // TODO maybe generate a sequence here?
		}
		err := producer.Produce(cmd.Topic, key, value, cmd.Serialize, cmd.ValueSchemaFile, cmd.KeySchemaFile)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(">")
	}
	return nil

}
func LoadConfig(config string) Configuration {
	configuration := parseConfig(config)
	// set global variable
	gafkaloConfig = Configuration(configuration)
	return configuration
}

func GetInputData(config Configuration) DesiredState {
	files, err := config.ResolveFilesFromPatterns(config.GetInputPatterns())
	if err != nil {
		log.Fatalf("Failed to get input files: %s\n", err)
	}
	inputData := Parse(files)
	return inputData
}

func GetAdminClients(config Configuration) (KafkaAdmin, SRAdmin, MDSAdmin) {
	kafkadmin := NewKafkaAdmin(config.Connections.Kafka)
	sradmin := NewSRAdmin(&config.Connections.Schemaregistry)
	mdsadmin := NewMDSAdmin(config.Connections.Mds)
	return kafkadmin, sradmin, *mdsadmin
}
func DoSync(kafkadmin *KafkaAdmin, sradmin *SRAdmin, mdsadmin *MDSAdmin, inputData *DesiredState, dryRun bool) {
	topicResults := kafkadmin.ReconcileTopics(inputData.Topics, dryRun)
	schemaResults := sradmin.Reconcile(inputData.Topics, dryRun)
	// Do MDS
	roleResults := mdsadmin.Reconcile(inputData.Clients, dryRun)
	NewReport(topicResults, schemaResults, roleResults, dryRun)
}
