package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strconv"
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
type LintCmd struct {
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
	Topics           []string `required arg help:"Topic to read from"`
	MaxRecords       int      `default:"0" help:"Max reacords to read. default to no limit"` // 0 means no limit
	DeserializeKey   bool     `default:"false" help:"Deserialize message key"`
	DeserializeValue bool     `default:"false" help:"Deserialize message value"`
	GroupID          string   `help:"Consumer group ID to use"`
	FromBeginning    bool     `default:"false" help:"Start reading from the beginning of the topic"`
	SetOffsets       string   `help:"Set offsets for partition on topic. Syntax is: TOPICNAME=partition:offset,partition:offset,.."`
}

type CheckExistsCmd struct {
	SchemaFile string `required help:"Schema file to checj"`
	Subject    string `required help:"Subject to check against schema"`
}
type SchemaDiffCmd struct {
	SchemaFile string `required help:"Schema file to checj"`
	Subject    string `required help:"Subject to check against schema"`
	Version    int    `required help:"Version to check against"`
}

type SchemaCmd struct {
	CheckExists CheckExistsCmd `cmd help:"Check if provided schema is registered"`
	SchemaDiff  SchemaDiffCmd  `cmd help:"Get the diff between a schema file and a registered schema"`
}

var CLI struct {
	Config   string      `required help:"configuration file"`
	Apply    ApplyCmd    `cmd help:"Apply the changes"`
	Plan     PlanCmd     `cmd help:"Produce a plan of changes"`
	Consumer ConsumerCmd `cmd help:"Consume from topics"`
	Produce  ProduceCmd  `cmd help:"Produce to a topic"`
	Schema   SchemaCmd   `cmd help:"Manage schemas"`
	Lint     LintCmd     `cmd help:"Run a linter against topic definitions"`
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
func parseOffsetsArg(arg *string) (map[int32]int64, error) {
	offsets := make(map[int32]int64)
	splitStrings := strings.Split(*arg, "=")
	if len(splitStrings) != 2 {
		return offsets, fmt.Errorf("= not found in SetOffsets param. Expected TOPIC=partition:offset")
	}
	// topic not used so assign to blank var
	_, offsetStr := splitStrings[0], splitStrings[1]
	partOffsetPairs := strings.Split(offsetStr, ",")
	for _, partOffsetPair := range partOffsetPairs {
		// Split into partition and offset and merge into offsets map
		splitStrings = strings.Split(partOffsetPair, ":")
		if len(splitStrings) != 2 {
			return offsets, fmt.Errorf("expected format partition:offset. Found: %s", partOffsetPair)
		}
		partition, err := strconv.ParseInt(splitStrings[0], 10, 32)
		if err != nil {
			return offsets, err
		}
		offset, err := strconv.ParseInt(splitStrings[1], 10, 64)
		if err != nil {
			return offsets, err
		}
		offsets[int32(partition)] = offset
	}
	return offsets, nil

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

	consumer := NewConsumer(config.Connections.Kafka, &config.Connections.Schemaregistry, cmd.Topics, cmd.GroupID, offsets, useOffsets, cmd.DeserializeKey, cmd.DeserializeValue, cmd.FromBeginning)
	err = consumer.Consume(cmd.MaxRecords)
	if err != nil {
		log.Fatal(err)
	}
	return nil

}

// Check if a schema is registered in specified subject. Shows version and Id if it is
func (cmd *CheckExistsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	_, sradmin, _ := GetAdminClients(config)
	schema, err := CreateSchema(cmd.Subject, cmd.SchemaFile, "BACKWARD", "AVRO")
	if err != nil {
		log.Fatal(err)
	}
	schemaID, schemaVersion, err := sradmin.LookupSchema(schema)
	if err != nil {
		return fmt.Errorf("failed to lookup schema [%+vs]", schema)
	}
	if schemaID == 0 {
		fmt.Printf("Schema not found in subject %s\n", schema.SubjectName)
		fmt.Printf("Did not find schema: %+vs\n", schema)
	} else {

		fmt.Printf("Schema is registered under %s with version %d and ID %d\n", schema.SubjectName, schemaVersion, schemaID)
	}
	return nil

}

// compared specified schema file against specified subject/version and produce a diff
// usefull to identify schemas that differ only in newlines or comments and schemaregistry considers it a new schema
func (cmd *SchemaDiffCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	_, sradmin, _ := GetAdminClients(config)
	schema, err := CreateSchema(cmd.Subject, cmd.SchemaFile, "BACKWARD", "AVRO")
	if err != nil {
		return err
	}
	existingSchema, err := sradmin.Client.GetSchemaByVersionWithArbitrarySubject(cmd.Subject, cmd.Version)
	if err != nil {
		return err
	}
	fmt.Printf("File schema %+vs \nExisting schema:  %+vs\n", schema, existingSchema)
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
func (cmd *LintCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	inputData := GetInputData(config)
	var results []LintResult
	for _, topic := range inputData.Topics {
		res := LintTopic(topic)
		results = append(results, res...)
	}
	PrettyPrintLintResults(results)
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
