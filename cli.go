package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
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
type ExportCmd struct {
	OutputFile      bool   `default:""`
	IncludeInternal bool   `help:"Include internal topics (starting with _)"`
	IncludeDefaults bool   `help:"Include kafka topic options that match broker defaults"`
	OutputDir       string `required help:"Output directory. Should exist and be empty"`
}
type LintCmd struct {
}

var CLI struct {
	Config        string           `required help:"configuration file"`
	Apply         ApplyCmd         `cmd help:"Apply the changes"`
	Export        ExportCmd        `cmd help:"Export a clusters state into YAML suitable for gafkalo"`
	Plan          PlanCmd          `cmd help:"Produce a plan of changes"`
	Consumer      ConsumerCmd      `cmd help:"Consume from topics"`
	Produce       ProduceCmd       `cmd help:"Produce to a topic"`
	Schema        SchemaCmd        `cmd help:"Manage schemas"`
	Lint          LintCmd          `cmd help:"Run a linter against topic definitions"`
	Connect       ConnectCmd       `cmd help:"manage connectors"`
	Consumergroup ConsumerGroupCmd `cmd help:"manage and view consumer groups"`
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
func (cmd *ExportCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	kafkadmin, sradmin, _ := GetAdminClients(config)
	topics := kafkadmin.Export(&sradmin, cmd.IncludeInternal, cmd.IncludeDefaults)
	var state InputYaml
	state.Topics = topics
	output, _ := yaml.Marshal(state)
	// TODO save YAML to OutputDir
	//fmt.Printf("Result: %s\n", string(output))
	ioutil.WriteFile(fmt.Sprintf("%s/topics.yaml", cmd.OutputDir), output, 0644)
	// Save schemas to requested location
	for _, topic := range topics {
		if topic.Key.SchemaPath != "" && topic.Key.schemaData != "" {
			outPath := fmt.Sprintf("%s/%s", cmd.OutputDir, topic.Key.SchemaPath)
			ioutil.WriteFile(outPath, []byte(topic.Key.schemaData), 0644)
		}
		if topic.Value.SchemaPath != "" && topic.Value.schemaData != "" {
			outPath := fmt.Sprintf("%s/%s", cmd.OutputDir, topic.Value.SchemaPath)
			ioutil.WriteFile(outPath, []byte(topic.Value.schemaData), 0644)
		}
	}

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
